import wx
import matplotlib
matplotlib.use('WXAgg')
from matplotlib.figure import Figure
from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg
import logging
import datetime
import data_retrieval
import data_loader
import timeseries

################################################################################

class ApplicationMenu(wx.MenuBar):
    '''
    This is the menu bar for file, edit, help, ... menu items
    '''

    def __init__(self, parent):

        logging.info('Creating menu bar')

        # Persist parent state for interaction with the listeners
        self.parent = parent

        # Add each menu heading in turn
        # (just a File menu for now)
        wx.MenuBar.__init__(self)
        
        self.Append(self.create_filemenu(self.parent),"&File") 
        
        # Add the MenuBar to the Frame content.
        parent.Show(True)

    def create_filemenu(self, parent):
        # Create a file menu
        filemenu = wx.Menu()

        # wx.ID_ABOUT and wx.ID_EXIT are standard IDs provided by wxWidgets.
        menu_about = filemenu.Append(wx.ID_ABOUT, "&About", " Demo")
        filemenu.AppendSeparator()
        parent.Bind(wx.EVT_MENU, self.on_menu_about, menu_about)
        
        menu_exit = filemenu.Append(wx.ID_EXIT,"E&xit"," Exit the program")
        parent.Bind(wx.EVT_MENU, self.on_menu_exit, menu_exit)
        
        return filemenu

    #
    # Listeners
    #

    def on_menu_about(self, event):
        # A message dialog box with an OK button. 
        # wx.OK is a standard ID in wxWidgets.
        popup(self.parent, 
                'A simple charting test programme',
                self.parent.config.title, 
                wx.OK)

    def on_menu_exit(self, event):
        # Close the application
        self.parent.Close(True)
        
################################################################################

class InfoPanel():

    def __init__(self, parent, container, config):
        logging.info('Creating the info panel')

        self.parent = parent

        self.header = wx.StaticBox(container, -1, 'Series statistics:')
        
        self.current_label = wx.StaticText(container, -1, 'Current:')
        self.min_val_label = wx.StaticText(container, -1, 'Min:')
        self.max_val_label = wx.StaticText(container, -1, 'Max:')
        self.ave_label = wx.StaticText(container, -1, 'Average:')
        self.sd_label = wx.StaticText(container, -1, 'Std dev:')
        self.zscore_label = wx.StaticText(container, -1, 'Z-Score:')

        self.current = wx.StaticText(container)
        self.min_val = wx.StaticText(container)
        self.max_val = wx.StaticText(container)
        self.ave = wx.StaticText(container)
        self.sd = wx.StaticText(container)
        self.zscore = wx.StaticText(container)

    def layout(self):
        sizer = wx.StaticBoxSizer(self.header, wx.VERTICAL)

        flags = wx.ALIGN_LEFT
        
        sizer.AddSpacer(self.parent.GetFont().GetPointSize())
        stats_grid = wx.FlexGridSizer(cols=2, hgap=5)
        stats_grid.Add(self.current_label, flag=flags)
        stats_grid.Add(self.current, flag=wx.EXPAND)
        stats_grid.Add(self.min_val_label, flag=flags)
        stats_grid.Add(self.min_val, flag=wx.EXPAND)
        stats_grid.Add(self.max_val_label, flag=flags)
        stats_grid.Add(self.max_val, flag=wx.EXPAND)
        stats_grid.Add(self.ave_label, flag=flags)
        stats_grid.Add(self.ave, flag=wx.EXPAND)
        stats_grid.Add(self.sd_label, flag=flags)
        stats_grid.Add(self.sd, flag=wx.EXPAND)
        stats_grid.Add(self.zscore_label, flag=flags)
        stats_grid.Add(self.zscore, flag=wx.EXPAND)

        stats_grid.AddGrowableCol(1, 1)
        sizer.Add(stats_grid, flag=flags)

        return sizer

    def update(self, data, config):
        ts = sorted(zip(data['dates'], data['values']))
        self.current.SetLabel('{0}'.format(ts[-1][1]))

        min = timeseries.min(ts)[0]
        self.min_val.SetLabel('{0} ({1})'.
                format(min[1], min[0].strftime('%Y-%m-%d')))
        max = timeseries.max(ts)[0]
        self.max_val.SetLabel('{0} ({1})'.
                format(max[1], max[0].strftime('%Y-%m-%d')))

        self.ave.SetLabel('{0:.2f}'.format(timeseries.mean(ts)[0][1]))
        self.sd.SetLabel('{0:.2f}'.format(timeseries.sd(ts)[0][1]))
        self.zscore.SetLabel('{0:.2f}'.
                format(timeseries.zscore(ts)[0][1]))

################################################################################

class ControlPanel():

    def __init__(self, parent, container, config):
        logging.info('Creating the control panel')

        # wx.Panel.__init__(self, parent)
        self.parent = parent

        # Text box that allows the user to select the series to view
        self.symbol_label= wx.StaticText(container, -1, 'Symbol')
        
        self.symbol_textbox = wx.TextCtrl(
                container, style=wx.TE_PROCESS_ENTER)

        self.parent.Bind(
                wx.EVT_TEXT_ENTER, 
                self.on_select_symbol, 
                self.symbol_textbox)
        self.symbol_textbox.SetValue(str(config.symbol))

        # Create a button that allows the user to create a new
        # data set
        self.redraw_button = wx.Button(container, -1, 'Redraw')
        self.parent.Bind(
                wx.EVT_BUTTON, 
                self.on_redraw_button,
                self.redraw_button)

    def layout(self):
        '''
        Return a sizer that lays out the control panel elements
        '''
        sizer = wx.BoxSizer(wx.HORIZONTAL)
        flags = wx.ALIGN_LEFT | wx.ALL | wx.ALIGN_CENTER_VERTICAL
        
        sizer.Add(self.symbol_label, flag=flags)
        sizer.Add(self.symbol_textbox , 0, border=3, flag=flags)
        sizer.Add(self.redraw_button, 0, border=3, flag=wx.ALIGN_RIGHT)

        return sizer

    #
    # Listeners
    #

    def on_select_symbol(self, event):
        '''
        Called when the listener fires on the set sample size text box 
        '''
        self.parent.config.symbol = self.symbol_textbox.GetValue()
        logging.debug('Redrawing chart for symbol ' + 
                self.parent.config.symbol)

        data = get_data(self.parent.config.date, self.parent.config.symbol)
        self.parent.chart.draw(data, self.parent.config)
        self.parent.chart_info.update(data, self.parent.config)

    def on_redraw_button(self, event):
        '''
        Called when the listener fires on the redraw button
        '''
        logging.debug('User clicked the redraw button')
        self.on_select_symbol(event)

################################################################################

def popup(parent, message, caption, style):
    '''
    Wrap the dialogue box popup into a single function call
    '''
    dlg = wx.MessageDialog(parent, message, caption, style)
    dlg.ShowModal()
    dlg.Destroy()
    
def get_data(date, symbol):
    args = {
        'symbol': symbol, 
        'start': datetime.datetime(2003, 11, 11), 
        'end': datetime.datetime(2013, 11, 11)
        }

    loader = 'download_yahoo_timeseries'
    id = '{symbol}_YAHOO_{start}_{end}'.format(**args)

    ts = data_retrieval.get_time_series(id, loader, args)
    dates, values = zip(*ts[args['symbol']][data_loader.TIMESERIES])
    return { 'dates': dates, 'values': [float(v) for v in values] } 

################################################################################

class ChartCanvas():
    '''
    Container for the matplotlib (or any other) chart object
    '''
    def __init__(self, container, config):
        
        # Create the matplotlib figure and attach it to a canvas
        self.figure = Figure(
                (config.chart_width, config.chart_height), 
                dpi=config.chart_dpi)
        self.canvas = FigureCanvasWxAgg(container, -1, self.figure)
        self.chart = self.figure.add_subplot(111)

    def layout(self):
        return self.canvas

    def draw(self, data, config):
        '''
        Redraw figure
        '''
        logging.debug('Redrawing time series')

        self.chart.clear()        
        # self.axes.grid(self.cb_grid.IsChecked())
        
        self.chart.plot(data['dates'], data['values']) 
        self.figure.autofmt_xdate()
        self.canvas.draw()

################################################################################

class ApplicationConfig():
    '''
    Constant config that's shared between the application elements
    '''

    title = 'python-tools'
    
    chart_dpi = 100
    chart_height = 4.0
    chart_width = 5.0

    symbol = 'IBM'
    date = datetime.datetime(2011, 11, 11)

################################################################################

class ApplicationFrame(wx.Frame):
    '''
    This is the primary application interface.
    It contains panels the user can interact with (menu bar, control
    panel, charting window, ...)
    '''

    def __init__(self):
        self.config = ApplicationConfig()
        
        wx.Frame.__init__(self, None, -1, self.config.title)

        # Add the menu bar
        menu = ApplicationMenu(self)
        self.SetMenuBar(menu)  
        
        # Create the panel that contains the chart
        self.panel = wx.Panel(self)
        # self.chart_panel = wx.Panel(self)
        self.chart = ChartCanvas(self.panel, self.config)
        self.chart_info = InfoPanel(self, self.panel, self.config)
        chart_sizer = wx.BoxSizer(wx.HORIZONTAL)
        chart_sizer.Add(self.chart.layout(), 3, wx.EXPAND | wx.ALL)
        chart_sizer.Add(self.chart_info.layout(), 1, wx.EXPAND | wx.ALL, border=5)
        # self.chart_panel.SetSizerAndFit(chart_sizer)

        # Create a panel for the user input
        self.control_panel = ControlPanel(self, self.panel, self.config)
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self.control_panel.layout(), 0, wx.ALIGN_CENTER | wx.TOP)
        sizer.Add(chart_sizer, 1, wx.EXPAND | wx.ALL)
        self.panel.SetSizerAndFit(sizer)

        # self.CreateStatusBar()

        # Draw an initial plot on startup
        data = get_data(self.config.date, self.config.symbol)
        self.chart.draw(data, self.config)
        self.chart_info.update(data, self.config)

################################################################################

def python_tools():
    logging.info('Starting python-tools')

    app = wx.PySimpleApp()
    app.frame = ApplicationFrame()
    app.frame.Fit()
    app.frame.Center()
    app.frame.Show()
    app.MainLoop()

################################################################################

if __name__ == '__main__':
    logging.basicConfig(level='DEBUG')
    python_tools() 

################################################################################

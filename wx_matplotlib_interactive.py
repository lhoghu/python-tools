'''
A test programme to try matplotlib with wxpython
'''
import random
import wx
import matplotlib
matplotlib.use('WXAgg')
from matplotlib.figure import Figure
from matplotlib.backends.backend_wxagg import FigureCanvasWxAgg
import logging

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
                'About this programme', 
                wx.OK)

    def on_menu_exit(self, event):
        # Close the application
        self.parent.Close(True)
        
################################################################################

class ControlPanel():

    def __init__(self, parent, container, config):
        logging.info('Creating the control panel')

        # wx.Panel.__init__(self, parent)
        self.parent = parent

        # Create a text box that allows the user to set the number 
        # of bars in the bar chart
        self.sample_size_label = wx.StaticText(
                container, -1, 'Set sample size')
        
        self.sample_size_textbox = wx.TextCtrl(
                container, 
                # size=(200,-1),
                style=wx.TE_PROCESS_ENTER)
        self.parent.Bind(
                wx.EVT_TEXT_ENTER, 
                self.on_set_sample_size, 
                self.sample_size_textbox)
        self.sample_size_textbox.SetValue(str(config.sample_size))

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
        
        sizer.Add(self.sample_size_label, flag=flags)
        sizer.Add(self.sample_size_textbox, 0, border=3, flag=flags)
        sizer.Add(self.redraw_button, 0, border=3, flag=wx.ALIGN_RIGHT)

        return sizer

    #
    # Listeners
    #

    def on_set_sample_size(self, event):
        '''
        Called when the listener fires on the set sample size text box 
        '''
        size_as_str = self.sample_size_textbox.GetValue()
        logging.debug('Redrawing chart with sample size ' + size_as_str)

        # Surround with try/catch to check the text box can be coerced to int
        try:
            sample_size = int(size_as_str)

            # Impose condition on the largest numbers of bars we'll plot
            if sample_size > 20:
                popup(self.parent, 
                        "You're silly. Try a smaller sample size " + 
                        "(preferably less than 20)",
                        'Input error',
                        wx.ICON_EXCLAMATION)
                return
            
            if sample_size < 1:
                popup(self.parent, 
                        'Please specify a sample size greater than 0',
                        'Input error',
                        wx.ICON_ERROR)
                return
            
            self.parent.config.sample_size = sample_size 

        except Exception, e:
            # Handle the input error and continue without updating the plot
            logging.warning('Could not convert sample size text ' + 
                    size_as_str + ' to integer: ' + str(e))
            popup(self.parent, 
                    'Could not convert the sample size to an int',
                    'Input error', 
                    wx.OK)
            return

        data = generate_data(
                self.parent.config.chart_max,
                self.parent.config.sample_size)
        self.parent.chart.draw(data, self.parent.config)

    def on_redraw_button(self, event):
        '''
        Called when the listener fires on the redraw button
        '''
        logging.debug('User clicked the redraw button')
        self.on_set_sample_size(event)

################################################################################

def popup(parent, message, caption, style):
    '''
    Wrap the dialogue box popup into a single function call
    '''
    dlg = wx.MessageDialog(parent, message, caption, style)
    dlg.ShowModal()
    dlg.Destroy()
    
def generate_data(limit, sample_size):
    '''
    Randomly generate sample data
    '''
    return [random.randrange(limit) for s in xrange(sample_size)]

################################################################################

class ChartCanvas():
    '''
    Container for the matplotlib (or any other) chart object
    '''
    def __init__(self, container, config):
        
        # self.parent = parent

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
        logging.debug('Redrawing bar chart')

        x = range(len(data))

        self.chart.clear()        
        # self.axes.grid(self.cb_grid.IsChecked())
        
        self.chart.bar(
            left=x, 
            height=data, 
            width=config.bar_width, 
            align='center', 
            alpha=0.44,
            picker=5)
        
        self.canvas.draw()

################################################################################

class ApplicationConfig():
    '''
    Constant config that's shared between the application elements
    '''

    title = 'Interactive Manhattan chart'
    bar_width = 0.2
    chart_dpi = 100
    chart_height = 4.0
    chart_width = 5.0

    chart_max = 10
    sample_size = 5

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
        
        # Create the panel that contains the chart. For now the chart
        # and control panel both attach to this panel, but would like
        # to factor those into separate panel objects eventually
        self.panel = wx.Panel(self)

        self.chart = ChartCanvas(self.panel, self.config)
        self.control_panel = ControlPanel(self, self.panel, self.config)

        # Use sizers to arrange the frame elements
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self.chart.layout(), 1, wx.EXPAND | wx.ALL)
        sizer.Add(self.control_panel.layout(), 0, wx.ALIGN_LEFT | wx.TOP)
        self.panel.SetSizerAndFit(sizer)

        # self.CreateStatusBar()

        # Draw an initial plot on startup
        data = generate_data(self.config.chart_max, self.config.sample_size)
        self.chart.draw(data, self.config)

################################################################################

def interactive_manhattan():
    logging.info('Starting interactive manhattan chart')

    app = wx.PySimpleApp()
    app.frame = ApplicationFrame()
    app.frame.Fit()
    app.frame.Center()
    app.frame.Show()
    app.MainLoop()

################################################################################

if __name__ == '__main__':
    logging.basicConfig(level='DEBUG')
    interactive_manhattan()

import logging
import data_loader
import matplotlib.pyplot as pyplot
import utils
import datetime
import data_retrieval
import algorithms
import timeseries
import charts

################################################################################

def get_treasuries_series(series):
    data = data_loader.download_treasuries()
    ts = data[series][data_loader.TIMESERIES]
    cleaned = [(date, value) for date, value in ts if value != 'ND']
    return zip(*cleaned)

################################################################################

def line_plot(dates, values):

    figure, axes = pyplot.subplots()
    axes.plot_date(dates, values, linestyle='-', marker='')
    figure.autofmt_xdate()
    pyplot.show()

################################################################################

def linear_reg():
    ibm_id = 'IBM_YAHOO_20131111'
    ibm_data = data_retrieval.get_time_series(ibm_id, 
            'download_yahoo_timeseries', 
            {
                'symbol': 'IBM', 
                'start': datetime.datetime(2003, 11, 11), 
                'end': datetime.datetime(2013, 11, 11)
            })

    spx_id = 'SPX_YAHOO_20131111'
    spx_data = data_retrieval.get_time_series(spx_id, 
            'download_yahoo_timeseries', 
            {
                'symbol': '^GSPC', 
                'start': datetime.datetime(2003, 11, 11), 
                'end': datetime.datetime(2013, 11, 11)
            })

    series = zip(*timeseries.common_dates(
            ibm_data['IBM'][data_loader.TIMESERIES], 
            spx_data['^GSPC'][data_loader.TIMESERIES]))

    ibm = [float(val) for val in series[1]]
    spx = [float(val) for val in series[2]]
    slope, intercept, r_value, _, _ = algorithms.linreg(ibm, spx) 
    print('Beta: {0}'.format(intercept))
    print('Slope: {0}'.format(slope))
    print('RSq: {0}'.format(r_value**2))

    series_for_charting = [('IBM', ibm), ('SPX', spx)]
    charts.line(series[0], series_for_charting)
    charts.scatter(series_for_charting)

################################################################################

def get_series():
    args = {
            'symbol': 'IBM', 
            'start': datetime.datetime(2003, 11, 11), 
            'end': datetime.datetime(2013, 11, 11)
            }

    loader = 'download_yahoo_timeseries'
    id = 'IBM_YAHOO_20131111'

    ts = data_retrieval.get_time_series(id, loader, args)
    dates, values = zip(*ts[args['symbol']][data_loader.TIMESERIES])
    line_plot(dates, values)
    
################################################################################

def plot_treasuries():
    dates, values = get_treasuries_series('H15/H15/RIFLGFCY10_N.B')
    line_plot(dates, values)

################################################################################

def get_yahoo_stock(symbol, start, end):
    data = data_loader.download_yahoo_timeseries(symbol, start, end)
    return zip(*data[symbol][data_loader.TIMESERIES])

################################################################################

def get_google_stock(symbol, start, end):
    data = data_loader.download_google_timeseries(symbol, start, end)
    return zip(*data[symbol][data_loader.TIMESERIES])

################################################################################

def plot_stock(symbol, start, end):
    dates, values = get_yahoo_stock(symbol, start, end)
    line_plot(dates, values)

################################################################################

def generate_test_transform_treasuries_data():
    data = data_loader.download_csv(data_loader.treasuries_config['TREASURIES_URL'])
    nb_rows = len(data)
    test_data = data[:10] + data[nb_rows-10:]
    utils.serialise_obj(test_data, 'testdata/test_transform_treasuries_data.data.py')
    utils.serialise_obj(data_loader.transform_treasuries_data(test_data), 'testdata/test_transform_treasuries_data.result.py')

def generate_test_transform_yahoo_timeseries():
    symbol = 'IBM'
    start = datetime.datetime(2013, 10, 11)
    end = datetime.datetime(2013, 11, 11)
    data = data_loader.download_yahoo_timeseries_raw(symbol, start, end)
    utils.serialise_obj(data, 'testdata/test_transform_yahoo_timeseries.data.py')
    utils.serialise_obj(data_loader.transform_yahoo_timeseries(data, symbol), 'testdata/test_transform_yahoo_timeseries.result.py')

def generate_test_transform_google_timeseries():
    symbol = 'GOOG'
    start = datetime.datetime(2013, 10, 11)
    end = datetime.datetime(2013, 11, 11)
    data = data_loader.download_google_timeseries_raw(symbol, start, end)
    utils.serialise_obj(data, 'testdata/test_transform_google_timeseries.data.py')
    utils.serialise_obj(data_loader.transform_google_timeseries(data, symbol), 'testdata/test_transform_google_timeseries.result.py')

################################################################################

if __name__ == '__main__':
    logging.basicConfig(level='DEBUG')
    # plot_treasuries()
    # plot_stock(
    #         'IBM', 
    #         datetime.datetime(2003, 11, 11), 
    #         datetime.datetime(2013, 11, 11))
    # generate_test_transform_google_timeseries()
    # generate_test_transform_yahoo_timeseries()
    # get_series()
    linear_reg()

################################################################################

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

def get_bbg_id():
    t = datetime.datetime.today()
    today_date = datetime.datetime(t.year, t.month, t.day)
    start_date =  today_date - datetime.timedelta(10)
    end_date = today_date  - datetime.timedelta(1)
    index_list = ('SPX Index', 'DJI Index', 'RTY Index')

    for index in index_list:
        index_id = data_retrieval.get_id(
            'download_bbg_timeseries', 
            {
                'symbol': index, 
                'start': start_date, 
                'end': end_date,
                'field' : 'INDX_MWEIGHT_HIST'
            })
        print index_id;

################################################################################

def get_bbg_data():
    t = datetime.datetime.today()
    today_date = datetime.datetime(t.year, t.month, t.day)
    start_date =  datetime.datetime(1990, 1, 1)
    end_date = today_date  - datetime.timedelta(1)
    index_list = ('SPX Index', 'DJI Index', 'RTY Index')
    equity_set = set()
    for index in index_list:
        field = ''
        if index == 'DJI Index':
            field = 'INDX_MWEIGHT'
        else:
            field = 'INDX_MWEIGHT_HIST'

        idx_data = data_retrieval.get_time_series(
            'download_bbg_timeseries', 
            {
                'symbol': index, 
                'start': start_date, 
                'end': end_date,
                'field' : field
            })
        if idx_data is not None:
            utils.serialise_csv(idx_data, data_retrieval.get_csv_filename('_'.join((index, 'COMPOSITION'))))
            for date,equity_list in idx_data:
                equity_set.update(equity_list)

    fields = (
        'PX_LAST', 
        'PX_OFFICIAL_CLOSE', 
        'PX_OPEN', 
        'PX_LAST',
        'PX_OPEN',
        'PX_LOW',
        'TOT_RETURN_INDEX_GROSS_DVDS',
        'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS',
        'EQY_DPS',
        'SECURITY_DES',
        'TRAIL_12M_GROSS_MARGIN',
        'ASSET_TURNOVER',
        'BS_SH_OUT',
        'TRAIL_12M_CASH_FROM_OPER',
        'CF_CASH_FROM_OPER',
        'BS_LT_BORROW',
        'LT_DEBT_TO_TOT_ASSET',
        'LIVE_LT_DEBT_TO_TOTAL_ASSETS',
        'CUR_RATIO',
        'CASH_RATIO',
        'QUICK_RATIO',
        'NET_INCOME',
        'CF_CASH_FROM_OPER',
        'RETURN_ON_ASSET',
        'EBIT',
        'EBITDA',
        'TOT_DEBT_TO_TOT_ASSET',
        'TOT_DEBT_TO_COM_EQY',
        'BS_TOT_ASSET',
        'HISTORICAL_MARKET_CAP')


    for equity in equity_set:
        equity = equity + " Equity"
        for field in fields:
            equ_data = data_retrieval.get_time_series(
                'download_bbg_timeseries', 
                {
                    'symbol': equity, 
                    'start': start_date, 
                    'end': end_date,
                    'field' : field
                })
            if equ_data is not None:
                utils.serialise_csv(equ_data, data_retrieval.get_csv_filename('_'.join((equity, field))))

# read indices and query all equities
#    for infile in glob.glob( os.path.join(os.getcwd(), index_dir,'*.data') ):
#        print infile
#        idx_equity_list = utils.deserialise_obj(infile)

################################################################################

def linear_reg():
    ibm_data = data_retrieval.get_time_series(
            'download_yahoo_timeseries', 
            {
                'symbol': 'IBM', 
                'start': datetime.datetime(2003, 11, 11), 
                'end': datetime.datetime(2013, 11, 11)
            })

    spx_data = data_retrieval.get_time_series(
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

def time_loops():
    import random

    @utils.log_time
    def gen_tuple_series(n):
        for i in range(n):
            yield random.randrange(0, 100), random.uniform(0, 100)

    @utils.log_time
    def gen_series(n):
        for i in range(n):
            yield random.uniform(0, 100)

    # Test 1 - note this version actually returns the tuple that contains
    # the min
    ts = gen_tuple_series(1000000)
    with utils.Timer() as t1: min(ts, key=lambda (_, v): v)
    print('Using key: {0} ms'.format(t1.interval*1000.0))

    # Test 2 - this version returns only the min value
    ts = gen_tuple_series(1000000)
    with utils.Timer() as t2: min(v for _, v in ts)
    print('Using genexp: {0} ms'.format(t2.interval*1000.0))

    # Test 3 - repeat the test where both return the same object
    ts = gen_series(1000000)
    with utils.Timer() as t1: min(ts)
    print('Raw min: {0} ms'.format(t1.interval*1000.0))

    # Test 4
    ts = gen_series(1000000)
    with utils.Timer() as t2: min(v for v in ts)
    print('Using genexp: {0} ms'.format(t2.interval*1000.0))

################################################################################

def get_series():
    args = {
            'symbol': 'IBM', 
            'start': datetime.datetime(2003, 11, 11), 
            'end': datetime.datetime(2013, 11, 11)
            }

    loader = 'download_yahoo_timeseries'

    ts = data_retrieval.get_time_series(loader, args)
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
    #logging.basicConfig(level='DEBUG')
    # plot_treasuries()
    # plot_stock(
    #         'IBM', 
    #         datetime.datetime(2003, 11, 11), 
    #         datetime.datetime(2013, 11, 11))
    # generate_test_transform_google_timeseries()
    # generate_test_transform_yahoo_timeseries()
    # get_series()
    # linear_reg()
    # time_loops()
    get_bbg_data()
    #get_bbg_id()
    print "done"
################################################################################

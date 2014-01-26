import logging
import data_loader
import matplotlib.pyplot as pyplot
import data_structure
import db
import utils
import datetime
import data_retrieval
import algorithms
import timeseries
import charts
import random

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
        ibm_data[data_structure.TIMESERIES],
        spx_data[data_structure.TIMESERIES]))

    ibm = [float(val) for val in series[1]]
    spx = [float(val) for val in series[2]]
    slope, intercept, r_value, _, _ = algorithms.linreg(ibm, spx)
    print('Beta: {0}'.format(intercept))
    print('Slope: {0}'.format(slope))
    print('RSq: {0}'.format(r_value ** 2))

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
    with utils.Timer() as t1:
        min(ts, key=lambda (_, v): v)
    print('Using key: {0} ms'.format(t1.interval * 1000.0))

    # Test 2 - this version returns only the min value
    ts = gen_tuple_series(1000000)
    with utils.Timer() as t2:
        min(v for _, v in ts)
    print('Using genexp: {0} ms'.format(t2.interval * 1000.0))

    # Test 3 - repeat the test where both return the same object
    ts = gen_series(1000000)
    with utils.Timer() as t1:
        min(ts)
    print('Raw min: {0} ms'.format(t1.interval * 1000.0))

    # Test 4
    ts = gen_series(1000000)
    with utils.Timer() as t2:
        min(v for v in ts)
    print('Using genexp: {0} ms'.format(t2.interval * 1000.0))

################################################################################

def get_series():
    args = {
        'symbol': 'IBM',
        'start': datetime.datetime(2003, 11, 11),
        'end': datetime.datetime(2013, 11, 11)
    }

    loader = 'download_yahoo_timeseries'

    ts = data_retrieval.get_time_series(loader, args)
    dates, values = zip(*ts[data_structure.TIMESERIES])
    line_plot(dates, values)

################################################################################

def plot_treasuries():
    dates, values = get_treasuries_series('H15/H15/RIFLGFCY10_N.B')
    line_plot(dates, values)

################################################################################

def get_yahoo_stock(symbol, start, end):
    data = data_loader.download_yahoo_timeseries(symbol, start, end)
    return zip(*data[data_structure.TIMESERIES])

################################################################################

def get_google_stock(symbol, start, end):
    data = data_loader.download_google_timeseries(symbol, start, end)
    return zip(*data[data_structure.TIMESERIES])

################################################################################

def plot_stock(symbol, start, end):
    dates, values = get_yahoo_stock(symbol, start, end)
    line_plot(dates, values)

################################################################################

def insert_to_mongo():
    client = db.MongoClient()
    collection = 'test'
    doc = dict(a=3, b='text', c=True, timestamp=datetime.datetime.now())
    client.insert(collection, doc)
    matches = client.find(collection)
    for match in matches:
        print(match)

################################################################################

def date_sequence(start, end):
    dte = start
    while dte <= end:
        yield dte
        dte = utils.offset(dte, 1)

def generate_symbols(count):
    import hashlib
    for i in range(count):
        yield hashlib.sha1('{0}'.format(i)).hexdigest()

def insert_timepoint(db_client, collection, id, dte, val):
    db_client.insert(collection, {
        'id': id,
        'date': dte,
        'val': val
    })

def load_mongo():
    import config
    config.MONGO_FOLDER = 'mongo_stress'
    config.MONGOD_PORT = 27021
    collection = 'overload'
    datatype = 'close'
    start_date = datetime.datetime(2003, 01, 03)
    end_date = datetime.datetime(2014, 01, 24)
    nb_series = 1000

    with db.MongoService():
        logging.info('Starting mongo service...')
        import time
        time.sleep(1)

        logging.info('Creating mongo client...')
        client = db.MongoClient()

        logging.info('Loading database...')
        with utils.Timer() as t:
            for symbol in generate_symbols(nb_series):
                id = {
                    'symbol': symbol,
                    'datatype': datatype
                }
                for dte in date_sequence(start_date, end_date):
                    insert_timepoint(client, collection, id, dte,
                                     random.uniform(0, 100.0))

        print('Inserted {0} series in {1} ms'.format(nb_series,
                                                     t.interval * 1000.0))

def query_mongo_stress():
    import config
    config.MONGO_FOLDER = 'mongo_stress'
    config.MONGOD_PORT = 27021
    collection = 'overload'
    datatype = 'close'

    with db.MongoService():
        logging.info('Starting mongo service...')
        import time
        time.sleep(1)

        logging.info('Creating mongo client...')
        client = db.MongoClient()

        logging.info('Finding symbols...')
        with utils.Timer() as t:
            matches = client.distinct(collection, 'id.symbol')
        logging.info('Found {0} series in {1} ms'.format(len(matches),
                                                  t.interval * 1000.0))

        import random
        idx = random.randint(0, len(matches))
        logging.info('Retrieving series {0}...'.format(matches[idx]))
        with utils.Timer() as t:
            matches = client.find(collection, {
                'id.symbol': matches[idx],
                'id.datatype': datatype
            })
        logging.info('Found {0} timepoints in {1} ms'.format(len(matches),
                                                             t.interval * 1000.0))

        logging.info('Converting time series...')
        with utils.Timer() as t:
            ts = map(lambda el: (el['date'], el['val']), matches)
            ts.sort()

        logging.info('Converted timeseries of length {0} in {1} ms'.format(
            len(ts),
            t.interval * 1000.0))

################################################################################

def test_time_series_record():
    doc = data_loader.download_mock_series(
        'sym',
        datetime.datetime(2014, 1, 1),
        datetime.datetime(2014, 1, 4))

    print(doc)

################################################################################

def generate_test_transform_treasuries_data():
    data = data_loader.download_csv(data_loader.treasuries_config['TREASURIES_URL'])
    nb_rows = len(data)
    test_data = data[:10] + data[nb_rows - 10:]
    utils.serialise_obj(test_data, 'testdata/test_transform_treasuries_data.data.py')
    utils.serialise_obj(data_loader.transform_treasuries_data(test_data),
                        'testdata/test_transform_treasuries_data.result.py')


def generate_test_transform_yahoo_timeseries():
    symbol = 'IBM'
    start = datetime.datetime(2013, 10, 11)
    end = datetime.datetime(2013, 11, 11)
    data = data_loader.download_yahoo_timeseries_raw(symbol, start, end)
    utils.serialise_obj(data, 'testdata/test_transform_yahoo_timeseries.data.py')
    utils.serialise_obj(data_loader.transform_yahoo_timeseries(data),
                        'testdata/test_transform_yahoo_timeseries.result.py')


def generate_test_transform_google_timeseries():
    symbol = 'GOOG'
    start = datetime.datetime(2013, 10, 11)
    end = datetime.datetime(2013, 11, 11)
    data = data_loader.download_google_timeseries_raw(symbol, start, end)
    utils.serialise_obj(data, 'testdata/test_transform_google_timeseries.data.py')
    utils.serialise_obj(data_loader.transform_google_timeseries(data),
                        'testdata/test_transform_google_timeseries.result.py')

################################################################################

if __name__ == '__main__':
    logging.basicConfig(level='DEBUG')
    # plot_treasuries()
    # plot_stock(
    #         'IBM', 
    #         datetime.datetime(2003, 11, 11), 
    #         datetime.datetime(2013, 11, 11))
    # generate_test_transform_treasuries_data()
    # generate_test_transform_google_timeseries()
    # generate_test_transform_yahoo_timeseries()
    # get_series()
    # linear_reg()
    # time_loops()
    # get_bbg_data()
    # get_bbg_id()
    # test_time_series_record()
    # insert_to_mongo()
    # load_mongo()
    query_mongo_stress()

    print ("done")

################################################################################

import os
import logging
import urllib2
import csv
import tempfile
import datetime
import re

################################################################################

TIMESERIES = 'timeseries'
METADATA = 'metadata'

################################################################################

treasuries_config = {
    'TREASURIES_URL': r'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H15&series=bf17364827e38702b42a58cf8eaa3f78&lastObs=&from=&to=&filetype=csv&label=include&layout=seriescolumn',
    'ID_FIELD': 'Unique Identifier: ',
    'DATE_REGEX': '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
}

################################################################################

def download_treasuries():
    '''
    Download history of US treasuries from fed website
    '''
    logging.info('Retrieving treasuries data from website')
    data = download_csv(treasuries_config['TREASURIES_URL'])
    return transform_treasuries_data(data)

################################################################################

def download_csv(url):
    '''
    Download raw csv from url
    Return the contents in a python array
    '''
    socket = urllib2.urlopen(url)

    # Write to a temporary csv so we can use csv.reader in a simple way
    # TODO remove the need for this temp file
    _, tmpfile = tempfile.mkstemp(suffix='.csv')
    data = []
    try:
        with open(tmpfile, 'w') as f:
            f.write(socket.read())

        with open(tmpfile, 'r') as f:
            reader = csv.reader(f)
            data = [row for row in reader]
    except Exception, e:
        msg = 'Failed to read from url ' + url + ' with error: ' + str(e)
        logging.error(msg)
        raise e
    finally:
        os.remove(tmpfile)

    return data

################################################################################

def transform_treasuries_data(data):
    '''
    Turn the raw data returned from the fed website into a form
    appropriate for downstream processing

    For now returns a python object
    In future might pass in a sink object to load the data to
    '''

    logging.debug('Transforming treasuries data')

    if len(data) == 0:
        return data

    # The first column is metadata headers and date information, the
    # remaining columns are the metadata and data associated with each
    # time series
    nb_time_series = len(data[0]) - 1
    if nb_time_series < 1:
        return data

    # Initialise the return object - this will by an dictionary of 
    # dictionaries, Each dictionary is an individual time series with 2 keys:
    #   timeseries: an array of (date, val) tuples
    #   metadata: a dictionary of metadata associated with each time series
    # Here we create an array of dictionaries. Later we'll turn this into a 
    # dictionary of dictionaries where we key off unique id and this is the
    # object we'll finally return. We just don't know the unique keys at 
    # this stage
    time_series = [{} for i in range(1, nb_time_series)]
    for t in time_series:
        t[METADATA] = {} 
        t[TIMESERIES] = []

    # Parse the csv data
    for row in data:
        if re.match(treasuries_config['DATE_REGEX'], row[0]) is not None:
            # The row is time series data: add it to the array of tuples
            year, month, day = row[0].split('-')
            dte = datetime.datetime(int(year), int(month), int(day))
            for s in range(1, nb_time_series):
                time_series[s-1][TIMESERIES].append((dte, row[s]))
        else:
            # The row is metadata: add it to the metadata entry
            # Use the key we get back from the input data as the
            # dicionary key
            for s in range(1, nb_time_series):
                time_series[s-1][METADATA][row[0]] = row[s]

    # Finally create the return dicionary keyed of unique id
    ret = {}
    for t in time_series:
        unique_id = t[METADATA][treasuries_config['ID_FIELD']]
        ret[unique_id] = t

    logging.info('Retrieved the following treasuries: {0}'.
            format(', '.join(ret.keys())))

    return ret

################################################################################

if __name__ == '__main__':
    logging.basicConfig(level='DEBUG')
    import utils
    data = download_csv(treasuries_config['TREASURIES_URL'])
    utils.serialise_obj(data, 'test_transform_treasuries_data.data.py')
    utils.serialise_obj(transform_treasuries_data(data), 'test_transform_treasuries_data.result.py')

import os
import logging
from urllib import urlencode as urlencode
import urllib2
import csv
import tempfile
import datetime
import re
import warnings

################################################################################
# required by BBG COM interface
bbg_imports = False
try:
    from pythoncom import PumpWaitingMessages
    from pythoncom import Empty
    from pythoncom import Missing
    from pythoncom import com_error
    import win32api
    from win32com.client import DispatchWithEvents
    from win32com.client import constants
    from win32com.client import CastTo
    bbg_imports = True
except ImportError:
    warnings.warn("unable to load PythonCOM + win32API + win32COM", DeprecationWarning)

################################################################################
# BBG global async variables
pending_requests = 0
max_pending_requests = 50
session = None
rfd = None
results = list()

################################################################################

TIMESERIES = 'timeseries'
METADATA = 'metadata'

################################################################################

treasuries_config = {
        'TREASURIES_URL': r'http://www.federalreserve.gov/datadownload/Output.aspx?rel=H15&series=bf17364827e38702b42a58cf8eaa3f78&lastObs=&from=&to=&filetype=csv&label=include&layout=seriescolumn',
        'ID_FIELD': 'Unique Identifier: ',
        'DATE_REGEX': '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        }

yahoo_config = {
        'HISTO_URL': 'http://ichart.yahoo.com/table.csv?',
        'DATE_COL': 'Date',
        'CLOSE_COL': 'Close',
        'DATEFMT': '%Y-%m-%d'
        }

google_config = {
        'HISTO_URL': 'http://www.google.com/finance/historical?',
        'DATE_COL': 'Date',
        'CLOSE_COL': 'Close',
        'DATEFMT': '%d-%b-%y'
        }

################################################################################

class SessionEvents(object):
    def OnProcessEvent(self, event_object):
        global pending_requests

        # obtain Event object interface
        event = CastTo(event_object, 'Event')
        
        if event.EventType == constants.RESPONSE: 
            pending_requests = pending_requests - 1
            process_event(event)
            self.continueLoop = False
            
        elif event.EventType == constants.PARTIAL_RESPONSE:
            process_event(event)
        
        else:
             process_other_event(event)

################################################################################

def download_bbg_timeseries(symbol, start, end, field):
    if field == 'INDX_MWEIGHT_HIST':
        return download_bbg_refdatarequest(symbol, start, end, field)
    else:
        return download_bbg_historicaldatarequest(symbol, start, end, field)

################################################################################

def init_bbg_session():
    global session
    global rfd
    if not bbg_imports:
        logging.info('Call to BBG but could not load the imports')
        return False;
    try:
        if session is None:
            session = DispatchWithEvents('blpapicom.ProviderSession.1', SessionEvents)
        
            # Start a Session
            session.Start()

            if not session.OpenService('//blp/refdata'):
                print 'Failed to opent service'
                raise Exception
            
            # event loop
            session.continueLoop = True
            rfd = session.GetService('//blp/refdata')
            return True
    except com_error as error:
        logging.info('Failed to init BBG: {0}'.format(error[1]))
        return False

################################################################################

def download_bbg_historicaldatarequest(symbol, start, end, field):
    global pending_requests
    global max_pending_requests
    global results
    global session
    global rfd

    results = list()

    start_date = datetime.datetime(1960,1,1)
    end_date = datetime.datetime.today() - datetime.timedelta(1)

    bbgdtformat = '%Y%m%d'

    if init_bbg_session():
        # for each index file
        request = rfd.CreateRequest("HistoricalDataRequest")
        request.GetElement("fields").AppendValue(field)
        request.Set("periodicityAdjustment", "ACTUAL")
        request.Set("periodicitySelection", "DAILY")
        request.Set("startDate", start_date.strftime(bbgdtformat))
        request.Set("endDate", end_date.strftime(bbgdtformat))

        request.GetElement("securities").AppendValue(symbol)

            # send request
        cid = session.SendRequest(request)
        pending_requests = pending_requests + 1

        while pending_requests > 0:
            PumpWaitingMessages()

        return results
    else:
        return None


################################################################################

def download_bbg_refdatarequest(symbol, start, end, field): 
    global pending_requests
    global max_pending_requests
    global results
    global session
    global rfd

    if init_bbg_session():
        results = list();
        bbgdtformat = '%Y%m%d'
        day_count = (end - start).days + 1
            
        # event loop
        session.continueLoop = True

        for single_date in (start + datetime.timedelta(n) for n in range(day_count)):
            
            request = rfd.CreateRequest('ReferenceDataRequest')

            # configure historical request
            request.GetElement('securities').AppendValue(symbol)
            
            request.GetElement('fields').AppendValue(field)
            request.GetElement('fields').AppendValue('END_DATE_OVERRIDE')
            override = request.GetElement('overrides').AppendElment()
            override.SetElement('fieldId', 'END_DATE_OVERRIDE')
            override.SetElement('value', single_date.strftime(bbgdtformat))

            # send request
            cid = session.SendRequest(request)
            print "Sent " + single_date.strftime(bbgdtformat) + symbol

            pending_requests = pending_requests + 1
            while pending_requests >= max_pending_requests:
                PumpWaitingMessages()
          
        
        while pending_requests > 0:
            PumpWaitingMessages()
            
        return results;
    else:
        return None

################################################################################

def process_other_event(event):
    
    iter = event.CreateMessageIterator()
    while iter.Next():

        msg = iter.Message
        print 'messageType=%s' % msg.MessageTypeAsString

################################################################################

def process_event(event):
    iter = event.CreateMessageIterator()
    while iter.Next():
        msg = iter.Message
        if msg.MessageTypeAsString == 'HistoricalDataResponse':
            process_historicaldataresponse(msg)
        elif msg.MessageTypeAsString == 'ReferenceDataResponse':
            process_referencedataresponse(msg)
        else:
            raise 'unknown message type <' + msg.MessageTypeAsString + '>'

################################################################################

def process_referencedataresponse(msg):
    global results

    security_data = msg.GetElement('securityData')
#   print("num securities" + str(security_data.NumValues))
    if security_data.NumValues > 1:
        raise 'not supporting multiple symbols at once for bbg'

    for i in range(security_data.NumValues):
        idx_equity_list = list()
#            requesttime = datetime.datetime(1900,1,1)
        security = security_data.GetValue(i)
        security_name = security.GetElement('security')
        field_data = security.GetElement('fieldData')

#            print(str(security_name) + str(field_data.NumElements))
        
        for i in range(field_data.NumElements):
            field = field_data.GetElement(i)
#                print("field" + str(field) + str(field.NumValues))
            if field.Datatype == 15:
                for j in range(field.NumValues): 
                    bulkElement = field.GetValue(j)   
                    for k in range(bulkElement.NumElements):
                        elem = bulkElement.GetElement(k)
                        if elem.Name == "Index Member":
                            equity_name = elem.Value
                            idx_equity_list.append(equity_name)         
#                                print elem.Name, elem.Value
            elif field.Name == "END_DATE_OVERRIDE":
#                    print str(field.Value)
                requesttime = datetime.datetime.strptime(str(field.Value), '%m/%d/%y %H:%M:%S')
                if requesttime > datetime.datetime.today():
                    requesttime = datetime.datetime(requesttime.year-100, requesttime.month, requesttime.day)
        print "found <" + str(len(idx_equity_list)) + "> elts for " + str(requesttime) + " " + str(security_name)

        results.append( (requesttime, idx_equity_list) )

    return results

################################################################################

def process_historicaldataresponse(msg):
    global results

    results = list()
    hist_field = '';
    if msg.MessageTypeAsString == "HistoricalDataResponse":
        security_data = msg.GetElement('securityData')
#   print("num securities:" + str(security_data.NumValues))
        security_name = str(security_data.GetElement('security'))
        field_data = security_data.GetElement('fieldData')            
#        print security_name + ", fields #" + str(field_data.NumValues)
        for i in range(field_data.NumValues):
            fields = field_data.GetValue(i)
            for j in range(fields.NumElements):    
                field = fields.GetElement(j)
                if field.Name == "date":
                    hist_dt = datetime.datetime.strptime(str(field.Value), '%m/%d/%y %H:%M:%S')
                else:
                    hist_field = field.Name
                    hist_val = field.Value
            results.append( (hist_dt, hist_val) )
    return results
 

def download_yahoo_quote():
    '''
    Retrieve the latest quote for a symbol from yahoo finance

    See http://www.gummy-stuff.org/Yahoo-data.htm for detail on what
    yahoo make available
    '''
    # TODO
    raise 'Not implemented'

################################################################################

def download_yahoo_timeseries(symbol, start, end):
    data = download_yahoo_timeseries_raw(symbol, start, end)
    return transform_yahoo_timeseries(data, symbol)

################################################################################

def download_yahoo_timeseries_raw(symbol, start, end):
    '''
    Retrieve time series for a symbol from yahoo finance
    '''
    logging.info('Retrieving time series for symbol {0} from Yahoo finance'.
            format(symbol))

    params = {
            'base': yahoo_config['HISTO_URL'],
            'symbol': symbol,
            'start_month': start.month - 1,
            'start_day': start.day,
            'start_year': start.year,
            'end_month': end.month - 1,
            'end_day': end.day,
            'end_year': end.year
            }
    url = '{base}s={symbol}&a={start_month}&b={start_day}&c={start_year}&d={end_month}&e={end_day}&f={end_year}&g=d&ignore=.csv'.format(**params)

    data = download_csv(url)
    return data

################################################################################

def transform_yahoo_timeseries(data, unique_id):
    '''
    Turn time series data from yahoo into form suitable for downstream 
    processing
    '''
    logging.debug('Transforming yahoo data')
    return transform_timeseries_table(data, unique_id, yahoo_config)

################################################################################

def download_google_timeseries(symbol, start, end):
    data = download_google_timeseries_raw(symbol, start, end)
    return transform_google_timeseries(data, symbol)

################################################################################

def download_google_timeseries_raw(symbol, start, end):
    '''
    Retrieve time series for a symbol from yahoo finance
    '''
    logging.info('Retrieving time series for symbol {0} from Google finance'.
            format(symbol))

    params = {
            'q': symbol,
            'startdate': start.strftime('%b %d, %Y'),
            'enddate': end.strftime('%b %d, %Y'),
            'output': 'csv'
            }
    url = '{0}{1}'.format(google_config['HISTO_URL'], urlencode(params))

    data = download_csv(url)
    return data

################################################################################

def transform_google_timeseries(data, unique_id):
    '''
    Turn time series data from google into form suitable for downstream 
    processing
    '''
    logging.debug('Transforming google data')
    return transform_timeseries_table(data, unique_id, google_config)

################################################################################

def transform_timeseries_table(data, unique_id, config):
    '''
    Turns tables of the form
    Date, Open, High, Low, Close, Volume, ...
    2012-11-11, 125, 634, 234.2, 623.52, 26642, ...
    2012-11-12, 125, 634, 234.2, 623.52, 26642, ...
    ...

    into a dictionary of the form
    { unique_id: 
        timeseries: [(date1, close1), (date2, close2), ...], 
        metadata: {}}
    '''
    if len(data) == 0:
        return {}

    date = data[0].index(config['DATE_COL'])
    close = data[0].index(config['CLOSE_COL'])

    def timepoint(row):
        dte = datetime.datetime.strptime(row[date], config['DATEFMT'])
        return (dte, row[close])

    timeseries = [timepoint(row) for row in data[1:]]

    return { 
            unique_id: 
            {
                TIMESERIES: timeseries,
                METADATA: {}
                } 
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
    logging.debug('Downloading from {url}'.format(url=url))
    socket = urllib2.urlopen(url)

    # Write to a temporary csv so we can use csv.reader in a simple way
    # TODO remove the need for this temp file
    _, tmpfile = tempfile.mkstemp(suffix='.csv')
    data = []
    try:
        with open(tmpfile, 'w') as f:
            # The decode is here to strip any BOM at the beginning of the file
            # Google return this so windows excel can pick up the utf-8 charset
            # We don't need it and it propagates through as a byte string
            # that complicates subsequent parsing
            f.write(socket.read().decode('utf-8-sig'))

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

    For now returns a python dictionary
    In future might pass in a sink object to load the data to
    '''

    logging.debug('Transforming treasuries data')

    if len(data) == 0:
        return {}

    # The first column is metadata headers and date information, the
    # remaining columns are the metadata and data associated with each
    # time series
    nb_time_series = len(data[0]) - 1
    if nb_time_series < 1:
        return {}

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

def download_mock_series(symbol, start, end):
    '''
    Mock query for unit testing - just returns the input args as metadata
    '''
    return { symbol: {
        TIMESERIES: [],
        METADATA: {
            'symbol': symbol,
            'start': start,
            'end': end
            }
        }
        }

################################################################################

if __name__ == '__main__':
    logging.basicConfig(level='DEBUG')
    import utils
    # utils.serialise_obj(
    #         download_yahoo_timeseries_raw(
    #             'IBM', 
    #             datetime.datetime(2012, 11, 11), 
    #             datetime.datetime(2013, 11, 11)), 
    #         'cache/yahoo_data.py')

    utils.serialise_obj(
            download_google_timeseries_raw(
                'GOOG', 
                datetime.datetime(2012, 11, 11), 
                datetime.datetime(2013, 11, 11)), 
            'cache/google_data.py')

    # utils.serialise_obj(download_treasuries(), 'cache/treasuries_data.py')

################################################################################


################################################################################

TIMESERIES = 'timeseries'
ID = 'id'
SOURCE = 'source'

################################################################################

def create_time_series(id, timeseries, source):
    return {
        TIMESERIES: timeseries,
        ID: id,
        SOURCE: [source]
    }

################################################################################

def time_series_record(f):

    def wrap(*args, **kwargs):
        return create_time_series(args, f(*args, **kwargs), {})
    
    return wrap

################################################################################

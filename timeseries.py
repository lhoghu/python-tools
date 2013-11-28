'''
Using a list of tuples as a common time series object
Each tuple has two elements, the first is a datetime.datetime object,
the second is the value
'''
import math

################################################################################

def returns(ts):
    '''
    Compute return as Series(t+1)/Series(t) - 1
    '''
    return [(ts[i][0], (ts[i][1] / ts[i-1][1]) - 1.0) 
            for i in range(1, len(ts))]

################################################################################

def log_returns(ts):
    '''
    Compute log return as log[Series(t+1)/Series(t)]
    '''
    return [(ts[i][0], math.log(ts[i][1] / ts[i-1][1])) 
            for i in range(1, len(ts))]

################################################################################

def mean(ts):
    '''
    The average of the input series
    Returns a time series object, with a date index given by the last time point
    and the value equal to the mean of the value elements
    '''
    return [(ts[-1][0], sum(v for _, v in ts)/len(ts))]

################################################################################

def sd(ts):
    '''
    The standard deviation of the input series
    Returns a time series object, with a date index given by the last time point
    and the value equal to the standard deviation of the value elements
    '''
    _, ave = mean(ts)[0]
    return [(ts[-1][0], (sum((v - ave)**2 for _, v in ts) / len(ts))**0.5)]

################################################################################

def zscore(ts):
    '''
    Return the zscore of the input series as a time series object
    '''
    # Not reusing mean and sd to avoid recomputation of mean
    ave = sum(v for _, v in ts)/len(ts)
    std = (sum((v - ave)**2 for _, v in ts) / len(ts))**0.5
    dte, val = ts[-1]
    return [(dte, (val - ave) / std)]

################################################################################

def min(ts):
    '''
    Return the min and date of min for the input timeseries as a timeseries object
    '''
    import __builtin__
    return [__builtin__.min(ts, key=lambda (_, v): v)]

################################################################################

def max(ts):
    '''
    Return the max and date of max for the input timeseries as a timeseries object
    '''
    import __builtin__
    return [__builtin__.max(ts, key=lambda (_, v): v)]

################################################################################

def common_dates(x, y):
    '''
    Takes two input time series objects and returns a list of tuples, 
    [(date1, x1, y1),...] on a the intersection of the date ranges
    '''
    xd = dict(x)
    yd = dict(y)
    dates = set(xd.keys()) & set(yd.keys())
    return [(d, xd[d], yd[d]) for d in sorted(dates)]

################################################################################

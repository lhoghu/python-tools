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
    mean = reduce(lambda (sx, sy), (x, y): (x, sy + y), ts, (None, 0.0))
    return [(mean[0], mean[1]/len(ts))]

################################################################################

def sd(ts):
    '''
    The standard deviation of the input series
    Returns a time series object, with a date index given by the last time point
    and the value equal to the standard deviation of the value elements
    '''
    ave = mean(ts)[0][1]

    var = reduce(lambda (sx, sy), (x, y): (x, sy + (y - ave)**2), ts, (None, 0.0))
    return [(var[0], (var[1] / len(ts))**0.5)]

################################################################################

def zscore(ts):
    '''
    Return the zscore of the input series as a time series object
    '''

    ave = mean(ts)[0][1]
    std = sd(ts)[0][1]
    return [(ts[-1][0], (ts[-1][1] - ave) / std)]

################################################################################

def min(ts):
    '''
    Return the min and date of min for the input timeseries as a timeseries object
    '''
    def min_and_date((mx, my), (x, y)):
        if y < my: return (x, y)
        return (mx, my)

    return [reduce(min_and_date, ts)]

################################################################################

def max(ts):
    '''
    Return the max and date of max for the input timeseries as a timeseries object
    '''
    def max_and_date((mx, my), (x, y)):
        if y > my: return (x, y)
        return (mx, my)

    return [reduce(max_and_date, ts)]

################################################################################

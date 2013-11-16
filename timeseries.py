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

import scipy.stats

################################################################################

def linreg(x, y):
    '''
    Wraps the scipy linear regression impl
    Returns tuple: slope, intercept, r_value, p_value, std_err
    The r-square is r_value**2
    '''
    
    return scipy.stats.linregress(x, y)

################################################################################

import pickle
import logging
import datetime

################################################################################

def serialise_obj(obj, filename):
    '''
    Use pickle to store obj in binary format in target filename
    '''
    logging.debug('Serialising object to file ' + filename)
    with open(filename, 'wb') as f:
        pickle.dump(obj, f)

def deserialise_obj(filename):
    '''
    Use pickle to deserialise object that's been saved in binary format
    '''
    logging.debug('Deserialising object from file ' + filename)
    with open(filename, 'rb') as f:
        return pickle.load(f)

################################################################################

def log_entry(f): 
    ''' 
    Decorator for functions to log entry 
    ''' 

    def log_entry(*args): 
        logging.debug('.'.join([f.__module__, f.__name__])) 
        return f(*args) 
                    
    log_entry.__name__ = f.__name__ 
    return log_entry

################################################################################

def offset(dte, off, period='BD'):
    '''
    Offset the input date (a datetime.datetime object) by the amount off
    where the period can be
        BD - business days
        M - months
        Y - years
    '''

    def apply_offset():
        period_uc = period.upper()
        if period_uc == 'BD':
            week_offset = off // 5
            days_offset = off % 5
            return dte + datetime.timedelta(days=days_offset, weeks=week_offset)
        elif period_uc == 'M':
            return datetime.datetime(dte.year, dte.month + off, dte.day)
        elif period_uc == 'Y':
            return datetime.datetime(dte.year + off, dte.month, dte.day)
        else:
            raise ValueError('Unrecognised period "{0}" in offset function. Use "BD", "M" or "Y"'.format(period))

    def bridge_weekend(dte, day_of_week):
        if off > 0:
            if day_of_week == 5: return dte + datetime.timedelta(days=2)
            if day_of_week == 6: return dte + datetime.timedelta(days=1)
        if off < 0:
            if day_of_week == 5: return dte + datetime.timedelta(days=-1)
            if day_of_week == 6: return dte + datetime.timedelta(days=-2)
        # If the offset is zero, or the day of the week is not a saturday or
        # a sunday, do not adjust. This is just because the direction of 
        # adjustment isn't obvious
        return dte

    newdate = apply_offset()

    day_of_week = newdate.weekday()
    if day_of_week in [5, 6]: 
        newdate = bridge_weekend(newdate, day_of_week)

    return newdate

################################################################################

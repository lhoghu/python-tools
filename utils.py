import pickle
import spickle
import logging
import datetime
import time
import csv
import os

################################################################################

def isiterable(obj):
    try:
        _ = iter(obj)
        return True
    except TypeError:
        return False

################################################################################

def s_serialise_obj(obj, filename):
    """
    Use pickle to store obj in binary format in target filename
    """
    logging.debug('Serialising object to file ' + filename)
    with open(filename, 'wb') as f:
        spickle.s_dump(obj, f)


def s_deserialise_obj(filename):
    """
    Use pickle to deserialise object that's been saved in binary format
    """
    logging.debug('Deserialising object from file ' + filename)
    results = list()
    with open(filename, 'rb') as f:
        for element in spickle.s_load(f):
            if isiterable(element[1]):
                results.append((element[0], tuple(element[1])))
            else:
                results.append((element[0], element[1]))
    return results

################################################################################

def serialise_obj(obj, filename):
    """
    Use pickle to store obj in binary format in target filename
    """
    logging.debug('Serialising object to file ' + filename)
    with open(filename, 'wb') as f:
        pickle.dump(obj, f)


def deserialise_obj(filename):
    """
    Use pickle to deserialise object that's been saved in binary format
    """
    logging.debug('Deserialising object from file ' + filename)
    with open(filename, 'rb') as f:
        return pickle.load(f)

################################################################################

def flatten(seq):
    for x in seq:
        try:
            if isinstance(x, basestring):
                yield x  # not iterable
            else:
                for y in flatten(x):
                    yield y
        except TypeError:
            yield x  # not iterable

################################################################################

def serialise_csv(obj, filename):
    if not os.path.isfile(filename):
        logging.debug('Serialising object to CSV file ' + filename)
        with open(filename, 'wb') as csvfile:
            csvwr = csv.writer(csvfile, quoting=csv.QUOTE_ALL, dialect='excel')
            if (obj):
                for line in obj:
                    csvwr.writerow(list(flatten(line)))

################################################################################

def deserialise_csv(filename):
    ts = list()
    if os.path.isfile(filename):
        logging.debug('Deserialising object from CSV file ' + filename)
        with open(filename, 'r') as csvfile:
            csvrd = csv.reader(csvfile, quoting=csv.QUOTE_ALL, dialect='excel')
            for csvrow in csvrd:
                csvrow_date = datetime.datetime.strptime(csvrow[0], '%Y-%m-%d %H:%M:%S')
                ts.append ((csvrow_date, tuple(csvrow[1:])))

    return ts

################################################################################

def log_entry(f):
    """ 
    Decorator for functions to log entry 
    """ 

    def wrap(*args):
        logging.debug('.'.join([f.__module__, f.__name__])) 
        return f(*args) 
                    
    wrap.__name__ = f.__name__
    return wrap


def log_time(f):
    """
    Decorator to time the function call
    """
    def wrap(*args):
        with Timer() as t:
            res = f(*args)
        logging.debug('{0}: {1} ms'.format(f.func_name, t.interval * 1000.0))
        return res

    wrap.__name__ = f.__name__
    return wrap
    
################################################################################

class Timer:    
    def __init__(self):
        pass

    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start

################################################################################

def offset(dte, off, period='BD'):
    """
    Offset the input date (a datetime.datetime object) by the amount off
    where the period can be
        BD - business days
        M - months
        Y - years
    """

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

    def bridge_weekend(bridge_dte, day):
        if off > 0:
            if day == 5:
                return bridge_dte + datetime.timedelta(days=2)
            if day == 6:
                return bridge_dte + datetime.timedelta(days=1)
        if off < 0:
            if day == 5:
                return bridge_dte + datetime.timedelta(days=-1)
            if day == 6:
                return bridge_dte + datetime.timedelta(days=-2)
        # If the offset is zero, or the day of the week is not a saturday or
        # a sunday, do not adjust. This is just because the direction of 
        # adjustment isn't obvious
        return bridge_dte

    new_date = apply_offset()

    day_of_week = new_date.weekday()
    if day_of_week in [5, 6]: 
        new_date = bridge_weekend(new_date, day_of_week)

    return new_date

################################################################################

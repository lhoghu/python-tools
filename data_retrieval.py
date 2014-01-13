import os
import data_structure
import utils
import data_loader
import config
import hashlib
import db
import logging

################################################################################

CACHE_EXT = '.py'
CSV_EXT = '.csv'
MONGO_DB = 'mongo'

################################################################################

def get_cache_filename(id):
    """
    Get the filename in the cache associated with id
    """
    return os.path.join(config.CACHE_FOLDER, id) + CACHE_EXT
    

################################################################################

def get_csv_filename(id):
    """
    Get the filename in the cache associated with id
    """
    return os.path.join(config.CSV_FOLDER, id) + CSV_EXT

################################################################################

def get_from_cache(id):
    """
    Check whether the series with the input id is present in the cache
    """
    if id is None:
        return False

    cache_file = get_cache_filename(id)
    if os.path.exists(cache_file):
        logging.debug("cached file found {0}".format(cache_file))
        return utils.deserialise_obj(cache_file)

    return False 
    
################################################################################

def clear_cache(id=None):
    """
    Remove all cache files in the cache folder, or just the specified id
    """
    if id is not None:
        os.remove(get_cache_filename(id))
    else:
        for root, dirs, files in os.walk(config.CACHE_FOLDER):
            for f in files:
                if f.endswith(CACHE_EXT):
                    os.remove(f)

################################################################################

def get_id(loader, loader_args):
    """
    Create the hash of the loader function name and its arguments
    """
    return hashlib.sha1('{0}{1}'.format(loader, loader_args)).hexdigest()

################################################################################

def get_db():
    if config.DB == MONGO_DB:
        return db.MongoClient()

    return None

################################################################################

def db_query_string(timeseries_id):
    query = {}
    for k, v in timeseries_id.iteritems():
        if k not in ('start', 'end'):
            query['{0}.{1}'.format(data_structure.ID, k)] = v

    return query

################################################################################

def get_time_series(loader, loader_args):
    """
    Interrogate the cache for the requested series
    If it doesn't exit, call the loader function from the data_loader module
    with the dictionary args and add it to the cache
    """

    client = get_db()
    if client is not None:
        # Get the time series from the db
        query = db_query_string(loader_args)
        match = client.find(db.TIMESERIES_COLLECTION, query)
        if len(match) > 0:
            return match
        else:
            # If it's not in the db, get it using the loader function
            # and add it to the db
            ts = getattr(data_loader, loader)(**loader_args)
            client.insert(db.TIMESERIES_COLLECTION, ts)
            return ts

    else:
        # Get the time series from the cache
        id = get_id(loader, loader_args)
        ts = get_from_cache(id)

        # If the time series is not in the cache/db, load it using the loader
        # function
        if not ts:
            ts = getattr(data_loader, loader)(**loader_args)
            utils.serialise_obj(ts, get_cache_filename(id))

        return ts

################################################################################

def save_time_series_csv(ts, id):
    utils.serialise_csv(ts, get_csv_filename(id))

################################################################################

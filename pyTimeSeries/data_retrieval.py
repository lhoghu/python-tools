import os
import hashlib
import logging

import data_structure
from pyTimeSeries import utils
import data_loader
import config
import db






################################################################################

CACHE_EXT_PICKLE = '.pickle'
CACHE_EXT_SPICKLE = '.spickle'
CSV_EXT = '.csv'
MONGO_DB = 'mongo'


################################################################################

def get_cache_filename_pickle(id):
    """
    Get the filename in the cache associated with id
    """
    return os.path.join(config.CACHE_FOLDER, id) + CACHE_EXT_PICKLE

################################################################################

def get_cache_filename_spickle(id):
    """
    Get the filename in the cache associated with id
    """
    return os.path.join(config.CACHE_FOLDER, id) + CACHE_EXT_SPICKLE

################################################################################

def get_cache_filename_csv(id):
    """
    Get the filename in the cache associated with id
    """
    return os.path.join(config.CSV_FOLDER, id) + CSV_EXT

################################################################################

def get_from_file_cache(id):
    """
    Check whether the series with the input id is present in the cache
    """
    logger = logging.getLogger('root')
    if id is None:
        return False

    if (config.SERIALISER == 'pickle'):
        cache_file = get_cache_filename_pickle(id)
        if os.path.exists(cache_file):
            logger.info("cached file found {0}".format(cache_file))
            return utils.deserialise_obj(cache_file)
        else:
            logger.info("cached file NOT found {0}".format(cache_file))
    elif (config.SERIALISER == 'spickle'):
        cache_file = get_cache_filename_spickle(id)
        if os.path.exists(cache_file):
            logger.info("cached file found {0}".format(cache_file))
            return utils.s_deserialise_obj(cache_file)
        else:
            logger.info("cached file NOT found {0}".format(cache_file))
    elif (config.SERIALISER == 'csv'):
        cache_file = get_cache_filename_csv(id)
        if os.path.exists(cache_file):
            logger.info("cached file found {0}".format(cache_file))
            return utils.deserialise_csv(cache_file)
        else:
            logger.info("cached file NOT found {0}".format(cache_file))
    else:
        raise Exception('invalid serialiser' + config.SERIALISER)

    return None
    
################################################################################

def clear_cache(id=None):
    """
    Remove all cache files in the cache folder, or just the specified id
    """
    if id is not None:
        try:
            os.remove(get_cache_filename_pickle(id))
        except OSError:
            pass
        try:
            os.remove(get_cache_filename_spickle(id))
        except OSError:
            pass
    else:
        for root, dirs, files in os.walk(config.CACHE_FOLDER):
            for f in files:
                if f.endswith(CACHE_EXT_PICKLE):
                    os.remove(f)
                if f.endswith(CACHE_EXT_SPICKLE):
                    os.remove(f)

################################################################################

def get_id(loader, loader_args):
    """
    Create the hash of the loader function name and its arguments
    """
    if (config.FILEID_TYPE == 'sha1'):
        return hashlib.sha1('{0}{1}'.format(loader, loader_args)).hexdigest()
    elif (config.FILEID_TYPE == 'explicit'):
        return '$$'.join((loader_args['symbol'], loader_args['field']))
    else:
        raise Exception('invalid FILEID_TYPE')

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

def get_from_cache(loader, loader_args):
    """
    Interrogate the cache for the requested series
    If it doesn't exit, call the loader function from the data_loader module
    with the dictionary args and add it to the cache
    """

    logger = logging.getLogger('root')
    client = get_db()
    if client is not None:
        # Get the time series from the db
        query = db_query_string(loader_args)
        match = client.find(db.TIMESERIES_COLLECTION, query)
        if len(match) > 0:
            return match
        else:
            logger.info('could not find in MONGO cache')
            return None

    else:
        # Get the time series from the cache
        id = get_id(loader, loader_args)
        ts = get_from_file_cache(id)

        # If the time series is not in the cache/db, load it using the loader
        # function
        if not ts:
            return None

        return ts

################################################################################

def write_to_cache(loader, loader_args, ts):
    client = get_db()
    if client is not None:
        client.insert(db.TIMESERIES_COLLECTION, ts)
    else:
        id = get_id(loader, loader_args)
        if (config.SERIALISER == 'pickle'):
            utils.serialise_obj(ts, get_cache_filename_pickle(id))
        elif (config.SERIALISER == 'spickle'):
            utils.s_serialise_obj(ts, get_cache_filename_spickle(id))
        elif (config.SERIALISER == 'csv'):
            utils.serialise_csv(ts, get_cache_filename_csv(id))
        else:
            raise Exception('invalid serialiser')

################################################################################

def get_time_series(loader, loader_args):
    """
    Interrogate the cache for the requested series
    If it doesn't exit, call the loader function from the data_loader module
    with the dictionary args and add it to the cache
    """
    logger = logging.getLogger('root')
    ts = get_from_cache(loader, loader_args)

    if not ts:
        ts = getattr(data_loader, loader)(**loader_args)
        if ts:
            write_to_cache(loader, loader_args, ts)
        else:
            return None
    else:
        logger.info('Found in cache')

    return ts

################################################################################

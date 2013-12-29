import os
import utils
import data_loader
import config
import hashlib
import db

################################################################################

CACHE_EXT = '.py'
MONGO_DB = 'mongo'

################################################################################

def get_cache_filename(id):
    '''
    Get the filename in the cache associated with id
    '''
    return os.path.join(config.CACHE_FOLDER, id) + CACHE_EXT
    
################################################################################

def get_from_cache(id):
    '''
    Check whether the series with the input id is present in the cache
    '''
    if id is None:
        return False

    cache_file = get_cache_filename(id)
    if os.path.exists(cache_file):
        return utils.deserialise_obj(cache_file)

    return False 
    
################################################################################

def clear_cache(id=None):
    '''
    Remove all cache files in the cache folder, or just the specified id
    '''
    if id is not None:
        os.remove(get_cache_filename(id))
    else:
        for root, dirs, files in os.walk(config.CACHE_FOLDER):
            for file in files:
                if file.endswith(CACHE_EXT):
                    os.remove(file)                                     

################################################################################

def get_id(loader, loader_args):
    '''
    Create the hash of the loader function name and its arguments
    '''
    db_client = get_db()
    if db_client is not None:
        return db_client.get_id(loader, loader_args)

    return hashlib.sha1('{0}{1}'.format(loader, loader_args)).hexdigest()

################################################################################

def get_db():
    if config.DB == MONGO_DB:
        return db.MongoClient()

    return None

################################################################################

def get_time_series(loader, loader_args):
    '''
    Interrogate the cache for the requested series
    If it doesn't exit, call the loader function from the data_loader module
    with the dictionary args and add it to the cache
    '''

    # Get the time series from the cache/db
    # For now we just get back from the cache based on whether the exact
    # same call has been made before
    # TODO add support to interrogate a db for whether the data can be found
    # in the cache
    id = get_id(loader, loader_args)

    def get_ts():
        if id is not None:
            return get_from_cache(id)
        return None

    ts = get_ts()

    # If the time series is not in the cache/db, load it using the loader 
    # function
    if not ts:
        ts = getattr(data_loader, loader)(**loader_args)

        # Add the time series to the cache/db
        # Ticky logic here while we transition to mongo...
        # If the id is none, we're using the db but the series is not in
        # the db. Therefore create a record for it in the db and pass the
        # newly created id onto the file cache
        # There's definite scope here for synchronisation between the file 
        # cache and db meta data to cause compilications. Perhaps the time
        # series should be put in the db too... 
        if id is None:
            db_client = get_db()
            if db_client is None:
                raise Exception('id not set in db mode {0}'.format(config.DB))
            id = db_client.insert(loader, loader_args)
            # If the db as added an object id, remove it, for now
            if loader_args.has_key(db.OBJECT_ID):
                del loader_args[db.OBJECT_ID]
        utils.serialise_obj(ts, get_cache_filename(id))

    return ts

################################################################################

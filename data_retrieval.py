import os
import utils
import data_loader
import config
import hashlib

################################################################################

CACHE_EXT = '.py'

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
    return hashlib.sha1('{0}{1}'.format(loader, loader_args)).hexdigest()

################################################################################

def get_time_series(loader, loader_args):
    '''
    Interrogate the cache for the requested series
    If it doesn't exit, call the loader function from the data_loader module
    with the dictionary args and add it to the cache
    '''

    # For now we just get back from the cache based on whether the exact
    # same call has been made before
    # TODO add support to interrogate a db for whether the data can be found
    # in the cache
    id = get_id(loader, loader_args)
    ts = get_from_cache(id)

    if not ts:
        ts = getattr(data_loader, loader)(**loader_args)
        utils.serialise_obj(ts, get_cache_filename(id))

    return ts

################################################################################

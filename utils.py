import pickle
import logging

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

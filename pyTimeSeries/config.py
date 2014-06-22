import ConfigParser
import os

SERIALISER = 'pickle'
CACHE_FOLDER = 'cache'
CSV_FOLDER = 'csv'
FILEID_TYPE = 'sha1'

DB = 'mongo'

MONGO_FOLDER = 'mongo'
MONGOD_PORT = 27017
MONGO_LOG = 'mongo.log'
MONGO_TIMESERIES_DB = 'timeseries_db'

def set_args(cp):
    global SERIALISER, CACHE_FOLDER, CSV_FOLDER, FILEID_TYPE, DB, MONGO_FOLDER, MONGOD_PORT, MONGO_LOG, MONGO_TIMESERIES_DB
    SERIALISER = cp.get('serialisation', 'serialiser')
    CACHE_FOLDER = cp.get('serialisation', 'cache_folder')
    CSV_FOLDER = cp.get('serialisation', 'csv_folder')
    FILEID_TYPE = cp.get('serialisation', 'fileid_type')

    DB = cp.get('database', 'db')

    MONGO_FOLDER = cp.get('mongo', 'mongo_folder')
    MONGOD_PORT = cp.getint('mongo', 'mongod_port')
    MONGO_LOG = cp.get('mongo', 'mongod_log')
    MONGO_TIMESERIES_DB = cp.get('mongo', 'mongo_timeseries_db')

def set_defaults():
    cp = ConfigParser.ConfigParser()
    cp.readfp(open('defaults.cfg'))
    set_args(cp)

def set_personal():
    cp = ConfigParser.ConfigParser()
    cp.readfp(open('defaults.cfg'))
    cp.read(os.path.expanduser('~/.python-tools.cfg'))
    set_args(cp)

set_personal()

def main():
    pass

if __name__ == '__main__':
    main()

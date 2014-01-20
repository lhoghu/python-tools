import ConfigParser
import os

cp = ConfigParser.ConfigParser()
cp.readfp(open('defaults.cfg'))
cp.read(os.path.expanduser('~/.python-tools.cfg'))

SERIALISER = cp.get('serialisation', 'serialiser')
CACHE_FOLDER = cp.get('serialisation', 'cache_folder')
CSV_FOLDER = cp.get('serialisation', 'csv_folder')

DB = cp.get('database', 'db')

MONGO_FOLDER = cp.get('mongo', 'mongo_folder')
MONGOD_PORT = cp.get('mongo', 'mongod_port')
MONGO_LOG = cp.get('mongo', 'mongod_log')
MONGO_TIMESERIES_DB = cp.get('mongo', 'mongo_timeseries_db')

def main():
    pass

if __name__ == '__main__':
    main()

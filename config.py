import ConfigParser
import os

CACHE_FOLDER = 'cache'
CSV_FOLDER = 'csv'

DB = 'mongo'

MONGO_FOLDER = 'mongo'
MONGOD_PORT = 27021
MONGO_LOG = 'mongo.log'
MONGO_TIMESERIES_DB = 'timeseries_db'

def main():
    config = ConfigParser.ConfigParser()
    config.readfp(open('defaults.cfg'))
    config.read(['site.cfg', os.path.expanduser('~/.python-tools.cfg')])
    CACHE_FOLDER = config.get('folders', 'cache_folder')
    CSV_FOLDER = config.get('folders', 'csv_folder')
    DB = config.get('database', 'db')
    MONGO_FOLDER = config.get('mongo', 'mongo_folder')
    MONGOD_PORT = config.get('mongo', 'mongod_port')
    MONGO_LOG = config.get('mongo', 'mongod_log')
    MONGO_TIMESERIES_DB = config.get('mongo', 'mongo_timeseries_db')

if __name__ == '__main__':
    main()

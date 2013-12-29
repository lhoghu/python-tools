import pymongo
import subprocess
import config
import logging
import bson.objectid

################################################################################

MONGO_SERVICE = 'mongod'
MONGO_HOST = 'localhost'
OBJECT_ID = '_id'

################################################################################

class MongoService():

    def start(self):
        # TODO use --logpath arg on mongod call
        with open(config.MONGO_LOG, 'w') as output:
            proc = subprocess.Popen([MONGO_SERVICE,
                '--dbpath', config.MONGO_FOLDER, 
                '--port', str(config.MONGOD_PORT)],
                stdout=output)

        logging.info('Started mongo server with pid {0}'.format(proc.pid))
        logging.info('Logging output to {0}'.format(config.MONGO_LOG))

        return proc

    def stop(self):
        logging.info('Shutting down mongo server')

        proc = subprocess.Popen([MONGO_SERVICE,
            '--shutdown',
            '--dbpath', config.MONGO_FOLDER, 
            '--port', str(config.MONGOD_PORT)],
            stdout=subprocess.PIPE)

        return proc

    def __enter__(self):
        return self.start()

    def __exit__(self, type, value, traceback):
        return self.stop()

################################################################################

class MongoClient():

    def __init__(self):
        self.client = pymongo.MongoClient(MONGO_HOST, config.MONGOD_PORT)
        self.db = self.client.MONGO_TIMESERIES_DB 

    def __del__(self):
        self.client.disconnect()

    def get_id(self, loader, loader_args):
        '''
        Return the object id associated with a document if it
        exists in the db, otherwise return None
        '''
        document = self.db.loader.find_one(loader_args)
        if document is not None:
            return str(document[OBJECT_ID])
        return None

    def insert(self, loader, loader_args):
        '''
        Insert the loader_args in a document into a collection
        with the name of the loader

        Return the object id of the document
        '''
        object_id = self.db.loader.insert(loader_args)
        return str(object_id)

    def remove(self, loader, loader_args):
        id = self.get_id(loader, loader_args)
        self.db.loader.remove({OBJECT_ID: bson.objectid.ObjectId(id)})

################################################################################

if __name__ == '__main__':
    logging.basicConfig(level='DEBUG')
    import time

    # db = MongoService()
    # db.start()
    # time.sleep(2)
    # db.stop()
    with MongoService():
        time.sleep(2)

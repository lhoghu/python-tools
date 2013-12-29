import pymongo
import subprocess
import config
import logging

################################################################################

MONGO_SERVICE = 'mongod'
MONGO_HOST = 'localhost'

################################################################################

class MongoService():

    def start(self):
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

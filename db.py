import pymongo
import subprocess
import config
import logging
import bson.objectid
from pymongo.son_manipulator import SONManipulator

################################################################################

MONGO_SERVICE = 'mongod'
MONGO_HOST = 'localhost'
OBJECT_ID = '_id'

TIMESERIES_COLLECTION = 'timeseries'

################################################################################

class MongoService():

    def __init__(self):
        pass

    @staticmethod
    def start():
        # TODO use --logpath arg on mongod call
        with open(config.MONGO_LOG, 'w') as output:
            proc = subprocess.Popen([MONGO_SERVICE,
                                     '--dbpath', config.MONGO_FOLDER,
                                     '--port', str(config.MONGOD_PORT)],
                                    stdout=output)

        logging.info('Started mongo server with pid {0}'.format(proc.pid))
        logging.info('Logging output to {0}'.format(config.MONGO_LOG))

        return proc

    @staticmethod
    def stop():
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

def get():
    """
    Get a mongo client instance
    """
    return MongoClient()

################################################################################

class TransformTuple(SONManipulator):

    type_key = '_type'
    tuple_type = 'tuple'
    container_key = 'as_list'

    def encode_tuple(self, tup):
        return {
            self.type_key: self.tuple_type,
            self.container_key: list(tup)
        }

    def decode_tuple(self, document):
        assert document[self.type_key] == self.tuple_type
        return tuple(document[self.container_key])

    def transform_incoming(self, son, collection):
        if son is None:
            return son

        copy = son.copy()
        for (key, value) in copy.items():
            if isinstance(value, tuple):
                copy[key] = self.encode_tuple(value)
            # Make sure we recurse into sub-docs
            elif isinstance(value, dict):
                copy[key] = self.transform_incoming(value, collection)
            elif isinstance(value, list):
                copy[key] = self.transform_incoming_list(value, collection)
        return copy

    def transform_outgoing(self, son, collection):
        if son is None:
            return son

        copy = son.copy()
        for (key, value) in copy.items():
            if isinstance(value, dict):
                if self.type_key in value:
                    if value[self.type_key] == self.tuple_type:
                        copy[key] = self.decode_tuple(value)
                else:
                    # Make sure we recurse into sub-docs
                    copy[key] = self.transform_outgoing(value, collection)
            if isinstance(value, list):
                copy[key] = self.transform_outgoing_list(value, collection)
        return copy

    def transform_incoming_list(self, lst, collection):
        def transform_element(el):
            if isinstance(el, dict):
                return self.transform_incoming(el, collection)
            elif isinstance(el, list):
                return self.transform_incoming_list(el, collection)
            elif isinstance(el, tuple):
                return self.encode_tuple(el)
            return el

        return [transform_element(element) for element in lst]

    def transform_outgoing_list(self, lst, collection):
        def transform_element(el):
            if isinstance(el, dict):
                if self.type_key in el:
                    if el[self.type_key] == self.tuple_type:
                        return self.decode_tuple(el)
                return self.transform_outgoing(el, collection)
            elif isinstance(el, list):
                return self.transform_outgoing_list(el, collection)
            return el

        return [transform_element(element) for element in lst]


################################################################################

class MongoClient():

    def __init__(self):
        self.client = pymongo.MongoClient(MONGO_HOST, config.MONGOD_PORT)
        self.db = self.client.MONGO_TIMESERIES_DB
        self.db.add_son_manipulator(TransformTuple())

    def __del__(self):
        self.client.disconnect()

    def insert(self, collection, doc):
        """
        Insert the doc into the named collection
        See http://api.mongodb.org/python/current/api/pymongo/collection.html

        Return the object id of the document
        """
        object_id = self.db[collection].insert(doc)
        return str(object_id)

    def update(self, collection, query, doc, multi=False):
        """
        Insert the doc into the collection, using the query to find a
        match. If a match is found, the match is updated with the doc.
        If multiple matches are found, only a single doc is updated.
        If no matches are found, a new document is created
        See http://api.mongodb.org/python/current/api/pymongo/collection.html
        @param query: a dict describing the document to be updated
        @param doc: the update values
        @return: a dict describing the outcome of the update
        """
        # TODO come back to creation of query objects... probably a
        # better way to go than transform
        transform = TransformTuple()
        query = transform.transform_incoming(query, collection)
        doc = transform.transform_incoming(doc, collection)
        return self.db[collection].update(query, doc, upsert=True, multi=multi)

    def find(self, collection, doc=None):
        """
        Find matches for the doc in the collection
        @param collection: string name of the collection to search
        @param doc: python dictionary search parameters. If None, will
        return all matches in the collection
        @return: list of matches
        """
        # TODO come back to creation of query objects... probably a
        # better way to go than transform
        transform = TransformTuple()
        doc = transform.transform_incoming(doc, collection)
        cursor = self.db[collection].find(doc)
        return [match for match in cursor]

    def remove(self, collection, doc=None):
        """
        Remove all documents from a collection
        @param collection: The name of the collection to delete from
        @param doc: if doc is not None, remove only documents that match
        the doc
        @return: None
        """
        # TODO come back to creation of query objects... probably a
        # better way to go than transform
        transform = TransformTuple()
        doc = transform.transform_incoming(doc, collection)
        self.db[collection].remove(doc)

    def distinct(self, collection, query):
        """
        Return the distinct records in a collection that match the
        query and return the results in a list
        @param collection:
        @param query:
        @return:
        """
        return self.db[collection].distinct(query)

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

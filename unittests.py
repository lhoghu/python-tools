import unittest
import os
import utils
import math
import datetime
import timeseries
import data_loader
import data_retrieval
import data_structure
import tempfile
import config
import db

################################################################################

class TestUtilsFunctions(unittest.TestCase):
    def test_serialise_deserialise_obj(self):
        """
        This tests the round trip: serialise an object then deserialise
        it and check we get back what we started with
        It's intended as a test for the two functions: 
            utils.serialise_obj
            utils.deserialise_obj
        """
        data = {
            'a': [5.97, 2.97, 8.2502, 4],
            'b': 'hello',
            'c': True
        }
        fd, tmpfile = tempfile.mkstemp(suffix='.py')
        try:
            utils.serialise_obj(data, tmpfile)
            result = utils.deserialise_obj(tmpfile)
        finally:
            os.remove(tmpfile)
        self.assertEqual(data, result)

    def test_offset(self):
        """
        Package together the properties we want for the date offset 
        function into a single test
        """
        # Basic function: single business day offset
        result = utils.offset(datetime.datetime(2013, 11, 11), 1)
        self.assertEqual(result, datetime.datetime(2013, 11, 12))

        # Check one week offset
        result = utils.offset(datetime.datetime(2013, 11, 14), -5)
        self.assertEqual(result, datetime.datetime(2013, 11, 7))

        # Check we move forward across a weekend correctly
        # - if we land on a saturday
        result = utils.offset(datetime.datetime(2013, 11, 13), 3, 'BD')
        self.assertEqual(result, datetime.datetime(2013, 11, 18))

        # - if we land on a sunday
        result = utils.offset(datetime.datetime(2013, 11, 13), 4, 'bd')
        self.assertEqual(result, datetime.datetime(2013, 11, 18))

        # And backward across a weekend 
        result = utils.offset(datetime.datetime(2013, 11, 11), -1)
        self.assertEqual(result, datetime.datetime(2013, 11, 8))

        # Check we bridge two weekends successfully 
        result = utils.offset(datetime.datetime(2013, 11, 11), -7)
        self.assertEqual(result, datetime.datetime(2013, 10, 31))

        # Check one month move
        result = utils.offset(datetime.datetime(2013, 11, 11), -1, 'M')
        self.assertEqual(result, datetime.datetime(2013, 10, 11))

        # Check one month move that lands on a weekend moves to the
        # next business day - also check case insensitivity
        result = utils.offset(datetime.datetime(2013, 11, 12), -1, 'm')
        self.assertEqual(result, datetime.datetime(2013, 10, 11))

        # Check one year move
        result = utils.offset(datetime.datetime(2013, 11, 12), 1, 'Y')
        self.assertEqual(result, datetime.datetime(2014, 11, 12))

        # Check one year move that lands on a weekend moves to the
        # next business day - also check case insensitivity
        result = utils.offset(datetime.datetime(2013, 11, 15), 1, 'y')
        self.assertEqual(result, datetime.datetime(2014, 11, 17))

        # Check exception is thrown if the period isn't recognised
        self.assertRaises(ValueError,
                          utils.offset,
                          datetime.datetime(2013, 11, 11), 1, 'X')

################################################################################

class TestTimeseriesFunctions(unittest.TestCase):
    def test_returns(self):
        ts = [
            (datetime.datetime(2013, 11, 7), 4.53),
            (datetime.datetime(2013, 11, 8), 2.92),
            (datetime.datetime(2013, 11, 11), 1.22),
            (datetime.datetime(2013, 11, 12), 6.30),
            (datetime.datetime(2013, 11, 13), 0.51),
        ]
        ts_returns = timeseries.returns(ts)

        expected_results = [
            (datetime.datetime(2013, 11, 8), (2.92 / 4.53) - 1),
            (datetime.datetime(2013, 11, 11), (1.22 / 2.92) - 1),
            (datetime.datetime(2013, 11, 12), (6.30 / 1.22) - 1),
            (datetime.datetime(2013, 11, 13), (0.51 / 6.30) - 1),
        ]
        self.assertEqual(ts_returns, expected_results)

    def test_log_returns(self):
        ts = [
            (datetime.datetime(2013, 11, 7), 4.53),
            (datetime.datetime(2013, 11, 8), 2.92),
            (datetime.datetime(2013, 11, 11), 1.22),
            (datetime.datetime(2013, 11, 12), 6.30),
            (datetime.datetime(2013, 11, 13), 0.51),
        ]
        ts_log_returns = timeseries.log_returns(ts)

        expected_results = [
            (datetime.datetime(2013, 11, 8), math.log(2.92 / 4.53)),
            (datetime.datetime(2013, 11, 11), math.log(1.22 / 2.92)),
            (datetime.datetime(2013, 11, 12), math.log(6.30 / 1.22)),
            (datetime.datetime(2013, 11, 13), math.log(0.51 / 6.30)),
        ]
        self.assertEqual(ts_log_returns, expected_results)

    def test_mean(self):
        ts = [
            (datetime.datetime(2013, 10, 31), 4.53),
            (datetime.datetime(2013, 11, 1), 3.87),
            (datetime.datetime(2013, 11, 4), -2.89),
            (datetime.datetime(2013, 11, 5), -0.18),
            (datetime.datetime(2013, 11, 6), 1.36),
            (datetime.datetime(2013, 11, 7), 6.32),
            (datetime.datetime(2013, 11, 8), 0.51),
            (datetime.datetime(2013, 11, 11), -5.98),
            (datetime.datetime(2013, 11, 12), -6.30),
            (datetime.datetime(2013, 11, 13), 0.51),
        ]

        mean = timeseries.mean(ts)

        expected_result = [(datetime.datetime(2013, 11, 13),
                            (4.53 + 3.87 - 2.89 - 0.18 + 1.36 +
                             6.32 + 0.51 - 5.98 - 6.30 + 0.51) / 10.0)]
        self.assertEqual(mean, expected_result)

    def test_sd(self):
        ts = [
            (datetime.datetime(2013, 10, 31), 4.53),
            (datetime.datetime(2013, 11, 1), 3.87),
            (datetime.datetime(2013, 11, 4), -2.89),
            (datetime.datetime(2013, 11, 5), -0.18),
            (datetime.datetime(2013, 11, 6), 1.36),
            (datetime.datetime(2013, 11, 7), 6.32),
            (datetime.datetime(2013, 11, 8), 0.51),
            (datetime.datetime(2013, 11, 11), -5.98),
            (datetime.datetime(2013, 11, 12), -6.30),
            (datetime.datetime(2013, 11, 13), 0.51),
        ]

        sd = timeseries.sd(ts)

        ave = (4.53 + 3.87 - 2.89 - 0.18 + 1.36 +
               6.32 + 0.51 - 5.98 - 6.30 + 0.51) / 10.0
        var = ((4.53 - ave) ** 2 + (3.87 - ave) ** 2 + (-2.89 - ave) ** 2 +
               (-0.18 - ave) ** 2 + (1.36 - ave) ** 2 + (6.32 - ave) ** 2 +
               (0.51 - ave) ** 2 + (-5.98 - ave) ** 2 + (-6.30 - ave) ** 2 +
               (0.51 - ave) ** 2) / 10.0

        expected_result = [(datetime.datetime(2013, 11, 13), var ** 0.5)]
        self.assertEqual(sd, expected_result)

    def test_zscore(self):
        ts = [
            (datetime.datetime(2013, 10, 31), 4.53),
            (datetime.datetime(2013, 11, 1), 3.87),
            (datetime.datetime(2013, 11, 4), -2.89),
            (datetime.datetime(2013, 11, 5), -0.18),
            (datetime.datetime(2013, 11, 6), 1.36),
            (datetime.datetime(2013, 11, 7), 6.32),
            (datetime.datetime(2013, 11, 8), 0.51),
            (datetime.datetime(2013, 11, 11), -5.98),
            (datetime.datetime(2013, 11, 12), -6.30),
            (datetime.datetime(2013, 11, 13), 0.51),
        ]

        zsc = timeseries.zscore(ts)

        ave = (4.53 + 3.87 - 2.89 - 0.18 + 1.36 +
               6.32 + 0.51 - 5.98 - 6.30 + 0.51) / 10.0
        var = ((4.53 - ave) ** 2 + (3.87 - ave) ** 2 + (-2.89 - ave) ** 2 +
               (-0.18 - ave) ** 2 + (1.36 - ave) ** 2 + (6.32 - ave) ** 2 +
               (0.51 - ave) ** 2 + (-5.98 - ave) ** 2 + (-6.30 - ave) ** 2 +
               (0.51 - ave) ** 2) / 10.0
        expected_result = [(datetime.datetime(2013, 11, 13),
                            (0.51 - ave) / (var ** 0.5))]
        self.assertEqual(zsc, expected_result)

    def test_min(self):
        ts = [
            (datetime.datetime(2013, 10, 31), 4.53),
            (datetime.datetime(2013, 11, 1), 3.87),
            (datetime.datetime(2013, 11, 4), -2.89),
            (datetime.datetime(2013, 11, 5), -0.18),
            (datetime.datetime(2013, 11, 6), 1.36),
            (datetime.datetime(2013, 11, 7), 6.32),
            (datetime.datetime(2013, 11, 8), 0.51),
            (datetime.datetime(2013, 11, 11), -5.98),
            (datetime.datetime(2013, 11, 12), -6.30),
            (datetime.datetime(2013, 11, 13), 0.51),
        ]

        ts_min = timeseries.min(ts)
        expected_result = [(datetime.datetime(2013, 11, 12), -6.30)]
        self.assertEqual(ts_min, expected_result)

    def test_max(self):
        ts = [
            (datetime.datetime(2013, 10, 31), 4.53),
            (datetime.datetime(2013, 11, 1), 3.87),
            (datetime.datetime(2013, 11, 4), -2.89),
            (datetime.datetime(2013, 11, 5), -0.18),
            (datetime.datetime(2013, 11, 6), 1.36),
            (datetime.datetime(2013, 11, 7), 6.32),
            (datetime.datetime(2013, 11, 8), 0.51),
            (datetime.datetime(2013, 11, 11), -5.98),
            (datetime.datetime(2013, 11, 12), -6.30),
            (datetime.datetime(2013, 11, 13), 0.51),
        ]

        ts_max = timeseries.max(ts)
        expected_result = [(datetime.datetime(2013, 11, 7), 6.32)]
        self.assertEqual(ts_max, expected_result)

    def test_common_dates(self):
        """
        Test the common_dates correctly returns an ordered result on the
        date intersection
        """
        x = [
            (datetime.datetime(2013, 10, 31), 4.53),
            (datetime.datetime(2013, 11, 4), -2.89),
            (datetime.datetime(2013, 11, 5), -0.18),
            (datetime.datetime(2013, 11, 6), 1.36),
            (datetime.datetime(2013, 11, 7), 6.32),
            (datetime.datetime(2013, 11, 12), -6.30)
        ]
        # Deliberately put dates slightly out of order
        y = [
            (datetime.datetime(2013, 10, 31), 8.53),
            (datetime.datetime(2013, 11, 1), -9.87),
            (datetime.datetime(2013, 11, 6), 11.36),
            (datetime.datetime(2013, 11, 5), 1.18),
            (datetime.datetime(2013, 11, 7), -6.32),
            (datetime.datetime(2013, 11, 8), 3.51),
            (datetime.datetime(2013, 11, 12), -2.30),
            (datetime.datetime(2013, 11, 13), 3.51)
        ]

        merged = timeseries.common_dates(x, y)

        expected_result = [
            (datetime.datetime(2013, 10, 31), 4.53, 8.53),
            (datetime.datetime(2013, 11, 5), -0.18, 1.18),
            (datetime.datetime(2013, 11, 6), 1.36, 11.36),
            (datetime.datetime(2013, 11, 7), 6.32, -6.32),
            (datetime.datetime(2013, 11, 12), -6.30, -2.30)
        ]
        self.assertEqual(merged, expected_result)

################################################################################

class TestDataLoaderFunctions(unittest.TestCase):
    data_folder = './testdata'

    def test_transform_treasuries_data(self):
        """
        Test the transformation of the raw data received from the fed
        treasury website into the form we use downstream
        """
        test_data = os.path.join(self.data_folder,
                                 'test_transform_treasuries_data.data.py')
        data = utils.deserialise_obj(test_data)
        result = data_loader.transform_treasuries_data(data)

        test_result = os.path.join(self.data_folder,
                                   'test_transform_treasuries_data.result.py')
        base_result = utils.deserialise_obj(test_result)

        self.assertEqual(result, base_result)

    def test_transform_yahoo_timeseries(self):
        """
        Test the transformation of the raw data received from yahoo
        into the form we use downstream
        """
        test_data = os.path.join(self.data_folder,
                                 'test_transform_yahoo_timeseries.data.py')
        data = utils.deserialise_obj(test_data)
        result = data_loader.transform_yahoo_timeseries(data)

        test_result = os.path.join(self.data_folder,
                                   'test_transform_yahoo_timeseries.result.py')
        base_result = utils.deserialise_obj(test_result)

        self.assertEqual(result, base_result)

    def test_transform_google_timeseries(self):
        """
        Test the transformation of the raw data received from google
        into the form we use downstream
        """
        test_data = os.path.join(self.data_folder,
                                 'test_transform_google_timeseries.data.py')
        data = utils.deserialise_obj(test_data)
        result = data_loader.transform_google_timeseries(data)

        test_result = os.path.join(self.data_folder,
                                   'test_transform_google_timeseries.result.py')
        base_result = utils.deserialise_obj(test_result)

        self.assertEqual(result, base_result)

################################################################################

class TestDataRetrievalFunctions(unittest.TestCase):
    # Set a cache folder for the unit test and a cache file that we
    # can remove in tearDown in case the test fails prior to clearing 
    # the cache
    cache_folder = './testdata/cache'
    cache_id = ''

    def setUp(self):
        self.restore_cache_folder = config.CACHE_FOLDER
        config.CACHE_FOLDER = self.cache_folder

    def tearDown(self):
        cache_file = data_retrieval.get_cache_filename(self.cache_id)
        if os.path.exists(cache_file):
            os.remove(cache_file)
        config.CACHE_FOLDER = self.restore_cache_folder

    def test_get_time_series(self):
        """
        End to end test of the following functions
            get_from_cache
            get_time_series
            clear_cache
        """
        symbol = 'TGTT'
        loader = 'download_mock_series'
        loader_args = {
            'symbol': symbol,
            'start': datetime.datetime(2012, 11, 11),
            'end': datetime.datetime(2013, 11, 11)
        }

        # Get the id used to store the file in the cache and assign it
        # so we can delete it in tearDown
        series_id = data_retrieval.get_id(loader, loader_args)
        self.cache_id = series_id

        # Check the item isn't in the cache to start with
        # If the test fails here, just delete the file in the cache folder
        self.assertFalse(data_retrieval.get_from_cache(series_id))

        # Retrieve the series
        result = data_retrieval.get_time_series(loader, loader_args)

        # Check the contents look ok: expect that the loader args are set
        # as metadata on the time series, but check 
        # data_loader.download_mock_series for how the test result is 
        # actually created
        self.assertEqual(loader_args, result[data_structure.ID])

        # Check we can now retrieve the id from the cache
        cached_result = data_retrieval.get_from_cache(series_id)
        self.assertEqual(loader_args, cached_result[data_structure.ID])

        # Remove it from the cache
        data_retrieval.clear_cache(series_id)

        # Check it's gone
        self.assertFalse(data_retrieval.get_from_cache(series_id))


class TestMongoDataRetrievalFunctions(unittest.TestCase):
    mongo_folder = './testdata/mongo'
    mongo_db = 'mongo'
    mongo_service = None
    cache_id = ''

    def setUp(self):
        self.restore_mongo_folder = config.MONGO_FOLDER
        config.MONGO_FOLDER = self.mongo_folder

        self.restore_db = config.DB
        config.DB = self.mongo_db

        self.mongo_service = db.MongoService()
        self.mongo_service.start()
        import time

        time.sleep(1)

    def tearDown(self):
        if self.cache_id != '':
            db_client = data_retrieval.get_db()

            import bson.objectid

            db_client.db[config.MONGO_TIMESERIES_DB]['download_mock_series'].remove(
                {'_id': bson.objectid.ObjectId(self.cache_id)})

        self.mongo_service.stop()

        cache_file = data_retrieval.get_cache_filename(self.cache_id)
        if os.path.exists(cache_file):
            os.remove(cache_file)
        config.MONGO_FOLDER = self.restore_mongo_folder
        config.MONGO_DB = self.restore_db

    def test_insert_find(self):
        """
        Test basis insert/find mechanic: insert a doc and check that
        find returns it
        """
        client = db.get()
        collection = 'test_insert_find'
        doc_1 = dict(a=8, b='text', c=True, d=[5.654, 2.757, 3.495])
        doc_2 = dict(b='string', c=True, d=[4.237, 'bleep'])

        # Make sure the collection is empty to start with
        client.remove(collection)

        match = client.find(collection, doc_1)
        self.assertEqual(0, len(match))

        # Insert the doc
        client.insert(collection, doc_1)

        # Check it's gone in
        match = client.find(collection, doc_1)
        self.assertEqual(1, len(match))
        for k, _ in doc_1.iteritems():
            self.assertEqual(doc_1[k], match[0][k])

        # Insert a second doc
        client.insert(collection, doc_2)

        # Check it's gone in
        match = client.find(collection, doc_2)
        self.assertEqual(1, len(match))
        for k, _ in doc_2.iteritems():
            self.assertEqual(doc_2[k], match[0][k])

        # Check we can still retrieve doc_1
        match = client.find(collection, doc_1)
        self.assertEqual(1, len(match))
        for k, _ in doc_1.iteritems():
            self.assertEqual(doc_1[k], match[0][k])

        # Check we have two docs in there (i.e. that the find
        # with no doc arg is working
        match = client.find(collection)
        self.assertEqual(2, len(match))

        client.remove(collection)
        match = client.find(collection)
        self.assertEqual(0, len(match))

    def test_tuple(self):
        client = db.get()
        collection = 'test_tuple'

        client.remove(collection)

        t = ('a', 3)
        doc = dict(tup=t)
        client.insert(collection, doc)

        match = client.find(collection)
        self.assertEqual(1, len(match))
        self.assertEqual(t, match[0]['tup'])

        client.remove(collection)

    def test_list_tuple(self):
        client = db.get()
        collection = 'test_list_tuple'

        client.remove(collection)

        list_of_tuple = [('a', 3), (datetime.datetime(2014, 1, 1), True)]
        doc = dict(tup=list_of_tuple)
        client.insert(collection, doc)

        match = client.find(collection)
        self.assertEqual(1, len(match))
        self.assertEqual(list_of_tuple, match[0]['tup'])

        client.remove(collection)

    def test_dict_of_list_of_tuple(self):
        client = db.get()
        collection = 'test_dict_of_list_of_tuple'

        client.remove(collection)

        doc = {
            'id': {
                'date': datetime.datetime(2014, 1, 1),
                'label': 'text'
            },
            'tuple_content': {
                'a_tuple': (4, 7),
                'a_boolean': True
            },
            'list_tuple_content': [(3, 2), (6, 3), (9.3805, 2.126)],
            'nested_tuple_content': ['a string', {
                'a_key': 'a value',
                'a_list_of_tuples': [('ein', 3, False), (4, 2)]
            }]
        }

        client.insert(collection, doc)
        match = client.find(collection)
        self.assertEqual(1, len(match))

        for k, _ in doc.iteritems():
            self.assertEqual(doc[k], match[0][k])

    @unittest.skip('Ignore during mongo dev')
    def test_get_time_series_mongo(self):
        """
        End to end test of the following functions
            get_from_cache
            get_time_series
            clear_cache
        """
        symbol = 'TGTT'
        loader = 'download_mock_series'
        loader_args = {
            'symbol': symbol,
            'start': datetime.datetime(2012, 11, 11),
            'end': datetime.datetime(2013, 11, 11)
        }

        # Check we can load a db client instance
        db_client = data_retrieval.get_db()
        self.assertIsNotNone(db_client)

        # Check the id is not in the db to start with
        id = data_retrieval.get_id(loader, loader_args)
        if id is not None:
            db_client.remove(loader, loader_args)

        id = data_retrieval.get_id(loader, loader_args)
        self.assertIsNone(id)

        # Retrieve the series
        result = data_retrieval.get_time_series(loader, loader_args)

        # Check the id now exists in the db
        id = data_retrieval.get_id(loader, loader_args)
        self.assertIsNotNone(id)
        self.cache_id = id

        # Check the contents look ok: expect that the loader args are set
        # as metadata on the time series, but check 
        # data_loader.download_mock_series for how the test result is 
        # actually created
        self.assertEqual(loader_args, result[data_structure.ID])

        # Check we can now retrieve the id from the cache
        cached_result = data_retrieval.get_from_cache(id)
        self.assertEqual(loader_args, cached_result[data_structure.ID])

        # Remove it from the cache
        data_retrieval.clear_cache(id)

        # Check it's gone from the cache...
        self.assertFalse(data_retrieval.get_from_cache(id))

        # ... and the db
        db_client.remove(loader, loader_args)
        id = data_retrieval.get_id(loader, loader_args)
        self.assertIsNone(id)

################################################################################

if __name__ == '__main__':
    # import logging
    # logging.basicConfig(level='DEBUG')
    unittest.main()

################################################################################ 

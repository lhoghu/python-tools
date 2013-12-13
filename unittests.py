import unittest
import os
import utils
import math
import datetime
import timeseries
import data_loader
import data_retrieval
import tempfile
import config

################################################################################

class TestUtilsFunctions(unittest.TestCase):

    def test_serialise_deserialise_obj(self):
        '''
        This tests the round trip: serialise an object then deserialise
        it and check we get back what we started with
        It's intended as a test for the two functions: 
            utils.serialise_obj
            utils.deserialise_obj
        '''
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
        '''
        Package together the properties we want for the date offset 
        function into a single test
        '''
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
                utils.offset, datetime.datetime(2013, 11, 11), 1, 'X')

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
                (datetime.datetime(2013, 11, 8), (2.92/4.53) - 1),
                (datetime.datetime(2013, 11, 11), (1.22/2.92) - 1),
                (datetime.datetime(2013, 11, 12), (6.30/1.22) - 1),
                (datetime.datetime(2013, 11, 13), (0.51/6.30) - 1),
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
                (datetime.datetime(2013, 11, 8), math.log(2.92/4.53)),
                (datetime.datetime(2013, 11, 11), math.log(1.22/2.92)),
                (datetime.datetime(2013, 11, 12), math.log(6.30/1.22)),
                (datetime.datetime(2013, 11, 13), math.log(0.51/6.30)),
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
                6.32 + 0.51 - 5.98 - 6.30 + 0.51)/10.0)]
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
                6.32 + 0.51 - 5.98 - 6.30 + 0.51)/10.0
        var = ((4.53 - ave)**2 + (3.87 - ave)**2 + (-2.89 - ave)**2 + 
                (-0.18 - ave)**2 + (1.36 - ave)**2 + (6.32 - ave)**2 + 
                (0.51 - ave)**2 + (-5.98 - ave)**2 + (-6.30 - ave)**2 + 
                (0.51 - ave)**2)/10.0

        expected_result = [(datetime.datetime(2013, 11, 13), var**0.5)]
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
                6.32 + 0.51 - 5.98 - 6.30 + 0.51)/10.0
        var = ((4.53 - ave)**2 + (3.87 - ave)**2 + (-2.89 - ave)**2 + 
                (-0.18 - ave)**2 + (1.36 - ave)**2 + (6.32 - ave)**2 + 
                (0.51 - ave)**2 + (-5.98 - ave)**2 + (-6.30 - ave)**2 + 
                (0.51 - ave)**2)/10.0
        expected_result = [(datetime.datetime(2013, 11, 13), 
            (0.51 - ave)/(var**0.5))]
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
        '''
        Test the common_dates correctly returns an ordered result on the
        date intersection
        '''
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
        '''
        Test the transformation of the raw data received from the fed
        treasury website into the form we use downstream
        '''
        test_data = os.path.join(self.data_folder,
                'test_transform_treasuries_data.data.py')
        data = utils.deserialise_obj(test_data)
        result = data_loader.transform_treasuries_data(data)

        test_result = os.path.join(self.data_folder, 
                'test_transform_treasuries_data.result.py')
        base_result = utils.deserialise_obj(test_result)

        self.assertEqual(result, base_result)

    def test_transform_yahoo_timeseries(self):
        '''
        Test the transformation of the raw data received from yahoo
        into the form we use downstream
        '''
        test_data = os.path.join(self.data_folder,
                'test_transform_yahoo_timeseries.data.py')
        data = utils.deserialise_obj(test_data)
        result = data_loader.transform_yahoo_timeseries(data, 'IBM')

        test_result = os.path.join(self.data_folder, 
                'test_transform_yahoo_timeseries.result.py')
        base_result = utils.deserialise_obj(test_result)

        self.assertEqual(result, base_result)
    
    def test_transform_google_timeseries(self):
        '''
        Test the transformation of the raw data received from google
        into the form we use downstream
        '''
        test_data = os.path.join(self.data_folder,
                'test_transform_google_timeseries.data.py')
        data = utils.deserialise_obj(test_data)
        result = data_loader.transform_google_timeseries(data, 'GOOG')

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
        '''
        End to end test of the following functions
            get_from_cache
            get_time_series
            clear_cache
        '''
        symbol = 'TGTT'
        loader = 'download_mock_series'
        loader_args = { 
                'symbol': symbol, 
                'start': datetime.datetime(2012, 11, 11),
                'end': datetime.datetime(2013, 11, 11)
                }

        # Get the id used to store the file in the cache and assign it
        # so we can delete it in tearDown
        id = data_retrieval.get_id(loader, loader_args)
        self.cache_file = id

        # Check the item isn't in the cache to start with
        # If the test fails here, just delete the file in the cache folder
        self.assertFalse(data_retrieval.get_from_cache(id))
        
        # Retrieve the series
        result = data_retrieval.get_time_series(loader, loader_args)

        # Check the contents look ok: expect that the loader args are set
        # as metadata on the time series, but check 
        # data_loader.download_mock_series for how the test result is 
        # actually created
        self.assertEqual(loader_args, 
                result[symbol][data_loader.METADATA])

        # Check we can now retrieve the id from the cache
        cached_result = data_retrieval.get_from_cache(id)
        self.assertEqual(loader_args, 
                cached_result[symbol][data_loader.METADATA])
        
        # Remove it from the cache
        data_retrieval.clear_cache(id)

        # Check it's gone
        self.assertFalse(data_retrieval.get_from_cache(id))

################################################################################

if __name__ == '__main__':
    # import logging
    # logging.basicConfig(level='DEBUG')
    unittest.main()

################################################################################ 

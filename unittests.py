import unittest
import os
import utils
import math
import datetime
import timeseries
import data_loader
import tempfile

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

if __name__ == '__main__':
    unittest.main()

################################################################################ 

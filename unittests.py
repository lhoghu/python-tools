import unittest
import os
import utils
import data_loader
import tempfile

class TestUtilsFunctions(unittest.TestCase):

    def test_serialise_deserialise_obj(self):

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

class TestDataLoaderFunctions(unittest.TestCase):

    data_folder = './testdata'
    
    def test_transform_treasuries_data(self):
        test_data = os.path.join(self.data_folder,
                'test_transform_treasuries_data.data.py')
        data = utils.deserialise_obj(test_data)
        result = data_loader.transform_treasuries_data(data)

        test_result = os.path.join(self.data_folder, 
                'test_transform_treasuries_data.result.py')
        base_result = utils.deserialise_obj(test_result)

        self.assertEqual(result, base_result)

if __name__ == '__main__':
    unittest.main()

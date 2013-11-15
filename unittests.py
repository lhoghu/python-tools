import unittest
import os
import utils
import data_loader

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

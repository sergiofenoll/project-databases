from app.user_service.models import User
from app import database as db, user_data_access, data_loader
from app import data_transformer, date_time_transformer, numerical_transformer, one_hot_encoder
import unittest


def _ci(*args: str):
    if len(args) == 1:
        return '"{}"'.format(str(args[0]).replace('"', '""'))
    return ['"{}"'.format(str(arg).replace('"', '""')) for arg in args]


def _cv(*args: str):
    if len(args) == 1:
        return "'{}'".format(str(args[0]).replace("'", "''"))
    return ["'{}'".format(str(arg).replace("'", "''")) for arg in args]


username = "test_username"
password = "test_pass"
firstname = "test_fname"
lastname = "test_lname"
email = "test_email@test.com"
status = "user"
active = True

# Create user_obj to compare with self
user_obj = User(username=username, password=password, firstname=firstname, lastname=lastname, email=email,
                 status=status, active=active)

class TestDataTransform(unittest.TestCase):
    def setUp(self):
        user_data_access.add_user(user_obj)

        data_loader.create_dataset('test_dataset', username)

    def tearDown(self):
        user_data_access.delete_user(username)
        data_loader.delete_dataset('schema-0')

    def test_impute_missing_data_on_average(self):
        # Table
        data_loader.create_table('test-table', 0, ('test1' , 'test2'))
        # Column to int
        data_loader.update_column_type(0, 'test2', 'INT')
        # Data to test with one missing value
        data_loader.insert_row('test-table', 0, ('test1', 'test2'), dict([('test1', '1'), ('test2', 1)]))
        data_loader.insert_row('test-table', 0, ('test1', 'test2'), dict([('test1', '1'), ('test2', 1)]))
        data_loader.insert_row('test-table', 0, ('test1', 'test2'), dict([('test1', '1'), ('test2', 6)]))
        data_loader.insert_row('test-table', 0, ('test1', 'test2'), dict([('test1', '1')]))

        # impute missing data
        data_transformer.impute_missing_data_on_average(0, 'test-table', 'test2')

        # check imputed data
        table = data_loader.get_table(0, 'test-table')

        self.assertEqual(len(table.rows), 4)
        self.assertEqual(len(table.rows[0]), 3)

        self.assertEqual(table.rows[0][2], 1)
        self.assertEqual(table.rows[1][2], 1)
        self.assertEqual(table.rows[2][2], 6)
        self.assertEqual(table.rows[3][2], 4)

    def test_impute_missing_data_on_median(self):
        # Table
        data_loader.create_table('test-table', 0, ('test1', 'test2'))
        # Column to int
        data_loader.update_column_type(0, 'test2', 'INT')
        # Data to test with one missing value
        data_loader.insert_row('test-table', 0, ('test1', 'test2'), dict([('test1', '1'), ('test2', 1)]))
        data_loader.insert_row('test-table', 0, ('test1', 'test2'), dict([('test1', '1'), ('test2', 1)]))
        data_loader.insert_row('test-table', 0, ('test1', 'test2'), dict([('test1', '1'), ('test2', 6)]))
        data_loader.insert_row('test-table', 0, ('test1', 'test2'), dict([('test1', '1'), ('test2', 8)]))
        data_loader.insert_row('test-table', 0, ('test1', 'test2'), dict([('test1', '1')]))

        # impute missing data
        data_transformer.impute_missing_data_on_average(0, 'test-table', 'test2')

        # check imputed data
        table = data_loader.get_table(0, 'test-table')

        self.assertEqual(len(table.rows), 4)
        self.assertEqual(len(table.rows[0]), 3)

        self.assertEqual(table.rows[0][2], 1)
        self.assertEqual(table.rows[1][2], 1)
        self.assertEqual(table.rows[2][2], 6)
        self.assertEqual(table.rows[3][2], 8)
        self.assertEqual(table.rows[4][2], 7)

    def test_find_and_replace(self):
        # Table
        data_loader.create_table('test-table', 0, ('test1'))

        # Data to test replace
        data_loader.insert_row('test-table', 0, list('test1'), dict([('test1', 'appel')]))
        data_loader.insert_row('test-table', 0, list('test1'), dict([('test1', 'citroen')]))

        ''' Replace strings'''
        # Replace ap 'substring' with banaan: appel ==> banaanppel
        data_transformer.find_and_replace(0, 'test-table', 'test1', 'ap', 'banaan', 'substring')

        # Replace citroen 'full replace' with druif: citroen ==> druif
        data_transformer.find_and_replace(0, 'test-table', 'test1', 'citroen', 'druif', 'full replace')

        # check imputed data
        table = data_loader.get_table(0, 'test-table')

        self.assertEqual(len(table.rows), 2)
        self.assertEqual(len(table.rows[0]), 2)

        self.assertEqual(table.rows[0][1], 'banaanppel')
        self.assertEqual(table.rows[1][1], 'druif ')

    def test_find_and_replace_by_regex(self):
        # Table
        data_loader.create_table('test-table', 0, ('test1'))

        # Data to test replace
        data_loader.insert_row('test-table', 0, list('test1'), dict([('test1', 'appel')]))
        data_loader.insert_row('test-table', 0, list('test1'), dict([('test1', 'citroen')]))

        ''' Replace string by regex'''

        data_transformer.find_and_replace_by_regex(0, 'test-table', 'test1', 'a.*', 'banaan')

        # check imputed data
        table = data_loader.get_table(0, 'test-table')

        self.assertEqual(len(table.rows), 2)
        self.assertEqual(len(table.rows[0]), 2)

        self.assertEqual(table.rows[0][1], 'banaan')
        self.assertEqual(table.rows[1][1], 'citroen')


class TestDateTimeTransformer(unittest.TestCase):
    def setUp(self):
        user_data_access.add_user(user_obj)
        data_loader.create_dataset('test_dataset', username)

    def tearDown(self):
        user_data_access.delete_user(username)
        data_loader.delete_dataset('schema-0')

    def test_extract_element_from_date(self):
        # Table
        data_loader.create_table('test-table', 0, ('test'))

        # Data to test replace
        data_loader.insert_row('test-table', 0, list('date'), dict([('date', '8/31/2015')]))

        # Change type to datetime
        data_loader.update_column_type(0, 'test-table', 'test', 'TIMESTAMP')


        # Extract element (MONTH)
        date_time_transformer.extract_element_from_date(0, 'test', 'date', 'MONTH')

        # Check imputed data
        table = data_loader.get_table(0, 'test-table')

        self.assertEqual(len(table.rows), 1)
        self.assertEqual(len(table.rows[0]), 3)

        self.assertEqual(table.rows[0][2], '31')

    def test_extract_date_or_time(self):
        # Table
        data_loader.create_table('test-table', 0, ('test'))

        # Data to test replace
        data_loader.insert_row('test-table', 0, list('date'), dict([('date', '8/31/2015')]))

        # Change type to datetime
        data_loader.update_column_type(0, 'test-table', 'test', 'TIMESTAMP')

        # Extract
        date_time_transformer.extract_date_or_time(0, 'test', 'date', 'DATE')

        # Check imputed data
        table = data_loader.get_table(0, 'test-table')

        self.assertEqual(len(table.rows), 1)
        self.assertEqual(len(table.rows[0]), 3)

        self.assertEqual(table.rows[0][2], '8/31/2015')

'''
class TestNumericalTranformations(unittest.TestCase):
    def setUp(self):
        user_data_access.add_user(user_obj)
        data_loader.create_dataset('test_dataset', username)

    def tearDown(self):
        user_data_access.delete_user(username)
        data_loader.delete_dataset('schema-0')
        
    def test_equal_width_interval(self):
        
    def test_equal_freq_interval(self):
        
    def test_manual_interval(self):
    
    def test_remove_outlier(self):
'''
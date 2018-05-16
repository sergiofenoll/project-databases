import unittest
from app import user_data_access, data_loader
from app import data_transformer, date_time_transformer, numerical_transformer, one_hot_encoder
from app.user_service.models import User

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

    @classmethod
    def setUpClass(cls):
        user_obj = User(username=username, password=password, firstname=firstname, lastname=lastname, email=email,
                        status=status, active=active)

        # Add user to db using UserDataAccess class
        user_data_access.add_user(user_obj)
        data_loader.create_dataset('test_dataset', username)

    @classmethod
    def tearDownClass(cls):
        data_loader.delete_dataset(0)
        user_data_access.delete_user(data_loader, username)

    def test_impute_missing_data_on_average(self):

        # Table
        data_loader.create_table('test-table', 0, ['test1', 'test2'])

        try:
            # Column to int
            data_loader.update_column_type(0, 'test-table', 'test2', 'DOUBLE PRECISION')

            # Data to test with one missing value
            data_loader.insert_row('test-table', 0, ['test1', 'test2'], dict([('test1', '1'), ('test2', 1)]))
            data_loader.insert_row('test-table', 0, ['test1', 'test2'], dict([('test1', '1'), ('test2', 2)]))
            data_loader.insert_row('test-table', 0, ['test1', 'test2'], dict([('test1', '1'), ('test2', 6)]))
            data_loader.insert_row('test-table', 0, ['test1', 'test2'], dict([('test1', '1')]))

            # impute missing data
            data_transformer.impute_missing_data_on_average(0, 'test-table', 'test2')

            # check imputed data
            table = data_loader.get_table(0, 'test-table')

            self.assertEqual(len(table.rows), 4)
            self.assertEqual(len(table.rows[0]), 3)

            self.assertEqual(table.rows[0][2], 1)
            self.assertEqual(table.rows[1][2], 2)
            self.assertEqual(table.rows[2][2], 6)
            self.assertEqual(table.rows[3][2], 3)
        finally:
            data_loader.delete_table('test-table', 0)

    def test_impute_missing_data_on_median(self):
        # Table
        data_loader.create_table('test-table', 0, ['test1', 'test2'])

        try:
            # Column to int
            data_loader.update_column_type(0, 'test-table', 'test2', 'DOUBLE PRECISION')

            # Data to test with one missing value
            data_loader.insert_row('test-table', 0, ['test1', 'test2'], dict([('test1', '1'), ('test2', 1)]))
            data_loader.insert_row('test-table', 0, ['test1', 'test2'], dict([('test1', '1'), ('test2', 1)]))
            data_loader.insert_row('test-table', 0, ['test1', 'test2'], dict([('test1', '1'), ('test2', 6)]))
            data_loader.insert_row('test-table', 0, ['test1', 'test2'], dict([('test1', '1'), ('test2', 8)]))
            data_loader.insert_row('test-table', 0, ['test1', 'test2'], dict([('test1', '1')]))

            # impute missing data
            data_transformer.impute_missing_data_on_average(0, 'test-table', 'test2')

            # check imputed data
            table = data_loader.get_table(0, 'test-table')

            self.assertEqual(len(table.rows), 5)
            self.assertEqual(len(table.rows[0]), 3)

            self.assertEqual(table.rows[0][2], 1)
            self.assertEqual(table.rows[1][2], 1)
            self.assertEqual(table.rows[2][2], 6)
            self.assertEqual(table.rows[3][2], 8)
            self.assertEqual(table.rows[4][2],  4)
        finally:
            data_loader.delete_table('test-table', 0)

    def test_find_and_replace(self):
        # Table
        data_loader.create_table('test-table', 0, ['test'])

        # Data to test replace
        data_loader.insert_row('test-table', 0, ['test'], dict([('test', 'appel')]))
        data_loader.insert_row('test-table', 0, ['test'], dict([('test', 'citroen')]))

        try:
            ''' Replace strings'''
            # Replace ap 'substring' with banaan: appel ==> banaanppel
            data_transformer.find_and_replace(0, 'test-table', 'test', 'ap', 'banaan', 'substring')

            # Replace citroen 'full replace' with druif: citroen ==> druif
            data_transformer.find_and_replace(0, 'test-table', 'test', 'citroen', 'druif', 'full replace')

            # check imputed data
            table = data_loader.get_table(0, 'test-table')

            self.assertEqual(len(table.rows), 2)
            self.assertEqual(len(table.rows[0]), 2)

            self.assertEqual(table.rows[0][1], 'banaanpel')
            self.assertEqual(table.rows[1][1], 'druif')
        finally:
            data_loader.delete_table('test-table', 0)

    def test_find_and_replace_by_regex(self):
        # Table
        data_loader.create_table('test-table', 0, ['test'])

        # Data to test replace
        data_loader.insert_row('test-table', 0, ['test'], dict([('test', 'appel')]))
        data_loader.insert_row('test-table', 0, ['test'], dict([('test', 'citroen')]))

        try:
            ''' Replace string by regex'''
            data_transformer.find_and_replace_by_regex(0, 'test-table', 'test', 'a.*', 'banaan')

            # check imputed data
            table = data_loader.get_table(0, 'test-table')

            self.assertEqual(len(table.rows), 2)
            self.assertEqual(len(table.rows[0]), 2)

            self.assertEqual(table.rows[0][1], 'banaan')
            self.assertEqual(table.rows[1][1], 'citroen')
        finally:
            data_loader.delete_table('test-table', 0)


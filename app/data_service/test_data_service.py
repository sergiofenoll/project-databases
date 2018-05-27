import unittest
from app import user_data_access, data_loader, database as db
from app.user_service.models import User
from app.data_service.models import Dataset, Column, Table, _cv, _ci

username = "test_username"
password = "test_pass"
firstname = "test_fname"
lastname = "test_lname"
email = "test_email@test.com"
status = "user"
active = True


class TestDataService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        user_obj = User(username=username, password=password, firstname=firstname, lastname=lastname, email=email,
                        status=status, active=active)

        # Add user to db using UserDataAccess class
        user_data_access.add_user(user_obj)

    @classmethod
    def tearDownClass(cls):
        user_data_access.delete_user(data_loader, username)

    def test_create_dataset(self):
        name = 'test_dataset'
        owner_id = username
        schema_id = 0
        dataset = Dataset(schema_id, name, 'Default description', username)

        try:
            data_loader.create_dataset(name, owner_id)
            self.assertEqual(dataset, data_loader.get_dataset(schema_id, owner_id))
        finally:
            data_loader.delete_dataset(schema_id)

    def test_delete_dataset(self):
        name = 'test_dataset'
        owner_id = username
        schema_id = 0

        try:
            data_loader.create_dataset(name, owner_id)
        finally:
            data_loader.delete_dataset(schema_id)

    def test_get_dataset_id(self):
        name = 'test_dataset'
        owner_id = username
        schema_id = 0

        try:
            data_loader.create_dataset(name, owner_id)
            gotten_id = int(data_loader.get_dataset_id(name)[0].split('-')[1])
            self.assertEqual(0, gotten_id)
        finally:
            data_loader.delete_dataset(schema_id)

    def test_create_table(self):
        schema_name = 'test-schema'
        table_name = 'test-table'
        columns = ['test-column']
        schema_id = 0
        table = Table(table_name, '', columns)
        try:
            data_loader.create_dataset(schema_name, username)
            data_loader.create_table(table_name, schema_id, [])
            self.assertEqual(table, data_loader.get_table(schema_id, table_name))
        finally:
            data_loader.delete_table(table_name, schema_id)
            data_loader.delete_dataset(schema_id)

    def test_delete_table(self):
        schema_name = 'test-schema'
        table_name = 'test-table'
        schema_id = 0

        try:
            data_loader.create_dataset(schema_name, username)
            data_loader.create_table(table_name, schema_id, [])
        finally:
            data_loader.delete_table(table_name, schema_id)
            data_loader.delete_dataset(schema_id)

    def test_get_table(self):
        schema_name = 'test-schema'
        table_name = 'test-table'
        columns = ['test-column']
        schema_id = 0
        table = Table(table_name, '', columns)
        try:
            data_loader.create_dataset(schema_name, username)
            data_loader.create_table(table_name, schema_id, [])
            self.assertEqual(table, data_loader.get_table(schema_id, table_name))
        finally:
            data_loader.delete_table(table_name, schema_id)
            data_loader.delete_dataset(schema_id)

    def test_table_exists(self):
        schema_name = 'test-schema'
        table_name = 'test-table'
        schema_id = 0
        try:
            data_loader.create_dataset(schema_name, username)
            data_loader.create_table(table_name, schema_id, [])
            data_loader.table_exists(table_name, schema_id)
        finally:
            data_loader.delete_table(table_name, schema_id)
            data_loader.delete_dataset(schema_id)

    def test_create_row(self):
        schema_name = 'test-schema'
        table_name = 'test-table'
        columns = ['test-column']
        values = {'test-column': 'test'}
        schema_id = 0
        try:
            data_loader.create_dataset(schema_name, username)
            data_loader.create_table(table_name, schema_id, columns)
            data_loader.insert_row(table_name, schema_id, columns, values)
        finally:
            data_loader.delete_table(table_name, schema_id)
            data_loader.delete_dataset(schema_id)

    def test_delete_row(self):
        schema_name = 'test-schema'
        table_name = 'test-table'
        columns = ['test-column']
        values = {'test-column': 'test'}
        schema_id = 0
        try:
            data_loader.create_dataset(schema_name, username)
            data_loader.create_table(table_name, schema_id, columns)
            data_loader.insert_row(table_name, schema_id, columns, values)
        finally:
            data_loader.delete_row(schema_id, table_name, [1])
            data_loader.delete_table(table_name, schema_id)
            data_loader.delete_dataset(schema_id)

    def test_create_column(self):
        schema_name = 'test-schema'
        table_name = 'test-table'
        new_column = 'test-column-2'
        new_column_type = 'VARCHAR(255)'
        schema_id = 0
        try:
            data_loader.create_dataset(schema_name, username)
            data_loader.create_table(table_name, schema_id, [])
            data_loader.insert_column(schema_id, table_name, new_column, new_column_type)
        finally:
            data_loader.delete_table(table_name, schema_id)
            data_loader.delete_dataset(schema_id)

    def test_delete_column(self):
        schema_name = 'test-schema'
        table_name = 'test-table'
        new_column = 'test-column'
        new_column_type = 'VARCHAR(255)'
        schema_id = 0
        try:
            data_loader.create_dataset(schema_name, username)
            data_loader.create_table(table_name, schema_id, [])
            data_loader.insert_column(schema_id, table_name, new_column, new_column_type)
        finally:
            data_loader.delete_column(schema_id, table_name, new_column)
            data_loader.delete_table(table_name,schema_id)
            data_loader.delete_dataset(schema_id)

    def test_rename_column(self):
        schema_name = 'test-schema'
        table_name = 'test-table'
        column_name = 'test-column'
        new_column_name = 'new-test-column'
        schema_id = 0
        try:
            data_loader.create_dataset(schema_name, username)
            data_loader.create_table(table_name, schema_id, [column_name])
            data_loader.rename_column(schema_id, table_name, column_name, new_column_name)
        finally:
            data_loader.delete_table(table_name, schema_id)
            data_loader.delete_dataset(schema_id)

    def test_update_column_type(self):
        schema_name = 'test-schema'
        table_name = 'test-table'
        column_name = 'test-column'
        column_type = 'INT'
        schema_id = 0
        try:
            data_loader.create_dataset(schema_name, username)
            data_loader.create_table(table_name, schema_id, [column_name])
            data_loader.update_column_type(schema_id, table_name, column_name, column_type)
        finally:
            data_loader.delete_table(table_name, schema_id)
            data_loader.delete_dataset(schema_id)

    def test_grant_access(self):
        contrib_username = "contrib_test_username"
        contrib_password = "contrib_test_pass"
        contrib_firstname = "contrib_test_fname"
        contrib_lastname = "contrib_test_lname"
        contrib_email = "contrib_test_email@test.com"
        contrib_status = "user"
        contrib_active = True

        user_data_access.add_user(
            User(contrib_username, contrib_password, contrib_firstname, contrib_lastname, contrib_email, contrib_status,
                 contrib_active))

        schema_name = 'test-schema'
        schema_id = 0
        try:
            data_loader.create_dataset(schema_name, username)
            data_loader.grant_access(contrib_username, schema_id)
            self.assertTrue(data_loader.has_access(contrib_username, schema_id))
        finally:
            user_data_access.delete_user(data_loader, contrib_username)
            data_loader.delete_dataset(schema_id)

    def test_remove_access(self):
        contrib_username = "contrib_test_username"
        contrib_password = "contrib_test_pass"
        contrib_firstname = "contrib_test_fname"
        contrib_lastname = "contrib_test_lname"
        contrib_email = "contrib_test_email@test.com"
        contrib_status = "user"
        contrib_active = True

        user_data_access.add_user(
            User(contrib_username, contrib_password, contrib_firstname, contrib_lastname, contrib_email, contrib_status,
                 contrib_active))

        schema_name = 'test-schema'
        schema_id = 0
        try:
            data_loader.create_dataset(schema_name, username)
            data_loader.grant_access(contrib_username, schema_id)
            data_loader.remove_access(contrib_username, schema_id)
        finally:
            user_data_access.delete_user(data_loader, contrib_username)
            data_loader.remove_access(contrib_username, schema_id)
            data_loader.delete_dataset(schema_id)

    def test_has_access(self):
        contrib_username = "contrib_test_username"
        contrib_password = "contrib_test_pass"
        contrib_firstname = "contrib_test_fname"
        contrib_lastname = "contrib_test_lname"
        contrib_email = "contrib_test_email@test.com"
        contrib_status = "user"
        contrib_active = True

        user_data_access.add_user(
            User(contrib_username, contrib_password, contrib_firstname, contrib_lastname, contrib_email, contrib_status,
                 contrib_active))

        schema_name = 'test-schema'
        schema_id = 0
        try:
            data_loader.create_dataset(schema_name, username)
            data_loader.grant_access(contrib_username, schema_id)
            data_loader.remove_access(contrib_username, schema_id)
        finally:
            user_data_access.delete_user(data_loader, contrib_username)
            data_loader.delete_dataset(schema_id)


if __name__ == '__main__':
    unittest.main()

import unittest
from user_data_access import DBConnection, UserDataAccess, User
from config import *


class TestUserDataAccess(unittest.TestCase):

    def connect(self):
        connection = DBConnection(dbname=config_data['dbname'], dbuser=config_data['dbuser'],
                                  dbpass=config_data['dbpass'], dbhost=config_data['dbhost'])
        return connection

    def test_connection(self):
        connection = self.connect()
        connection.close()
        print("Connection: ok")

    def test_add_user_success(self):
        connection = self.connect()

        username = "test_username"
        password = "test_pass"
        firstname = "test_fname"
        lastname = "test_lname"
        email = "test_email@test.com"
        status = "user"
        active = True

        # Create obj to access user data in db
        user_data_access_obj = UserDataAccess(dbconnect=connection)

        # Remove test_user before trying to add
        connection.get_cursor().exectue('DELETE FROM Member WHERE UserName=%s', (username,))
        connection.get_connection().commit()

        # Create user_obj to add to db
        user_obj = User(username=username, password=password, firstname=firstname, lastname=lastname, email=email,
                        status=status, active=active)

        # Add user to db using UserDataAccess class
        user_data_access_obj.add_user(user_obj)

        # Retrieve test_user data from db
        connection.get_cursor().execute(
            'SELECT Username,Pass, FirstName, LastName, Email, Status, Active FROM Member WHERE Username=%s;',
            (username,))
        row = connection.get_cursor().fetchone()

        self.assertIsNotNone(row)

        db_user_obj = User(row['username'], row['pass'], row['firstname'], row['lastname'], row['email'],
                           row['status'], row['active'])
        print(db_user_obj.to_dct())

        self.assertEqual(user_obj, db_user_obj)

        # Remove test_user after test
        connection.get_cursor().exectue('DELETE FROM Member WHERE UserName=%s', (username,))
        connection.get_connection().commit()

    def test_get_user(self):
        connection = self.connect()

        username = "test_username"
        password = "test_pass"
        firstname = "test_fname"
        lastname = "test_lname"
        email = "test_email@test.com"
        status = "user"
        active = True

        # Create obj to access user data in db
        user_data_access_obj = UserDataAccess(dbconnect=connection)

        # Remove test_user before trying to add
        connection.get_cursor().exectue('DELETE FROM Member WHERE UserName=%s', (username,))
        connection.get_connection().commit()

        # Insert user into db
        query = connection.get_cursor().mogrify(
            'INSERT INTO Member(Username,Pass,FirstName,LastName,Email,Status,Active) '
            'VALUES(%s,%s,%s,%s,%s,%s,%s)', (username, password, firstname, lastname, email, status, active,))
        connection.get_cursor().execute(query)
        connection.commit()

        # Create user_obj to compare with db_user_obj
        user_obj = User(username=username, password=password, firstname=firstname, lastname=lastname, email=email,
                        status=status, active=active)

        # Retrieve db_user_obj using UserDataAccess class
        db_user_obj = user_data_access_obj.get_user(username)
        print(db_user_obj.to_dct())

        self.assertEqual(user_obj, db_user_obj)

        # Remove test_user after test
        connection.get_cursor().exectue('DELETE FROM Member WHERE UserName=%s', (username,))
        connection.get_connection().commit()

    def test_get_users(self):
        connection = self.connect()

        username = "test_username"
        password = "test_pass"
        firstname = "test_fname"
        lastname = "test_lname"
        email = "test_email@test.com"
        status = "user"
        active = True

        # Create obj to access user data in db
        user_data_access_obj = UserDataAccess(dbconnect=connection)

        # Remove test_user before trying to add
        connection.get_cursor().exectue('DELETE FROM Member WHERE UserName=%s', (username,))
        connection.get_connection().commit()

        # Insert user into db
        query = connection.get_cursor().mogrify(
            'INSERT INTO Member(Username,Pass,FirstName,LastName,Email,Status,Active) '
            'VALUES(%s,%s,%s,%s,%s,%s,%s)', (username, password, firstname, lastname, email, status, active,))
        connection.get_cursor().execute(query)
        connection.commit()

        # Create user_obj to compare with db_user_obj
        user_obj = User(username=username, password=password, firstname=firstname, lastname=lastname, email=email,
                        status=status, active=active)

        # Retrieve db_user_obj using UserDataAccess class
        db_user_objs = user_data_access_obj.get_users()
        db_user_found = False

        for db_user_obj in db_user_objs:
            if db_user_obj.username == user_obj.username:
                db_user_found = True

                print(db_user_obj.to_dct())

                self.assertEqual(user_obj, db_user_obj)
                break

        self.assertTrue(db_user_found)

        # Remove test_user after test
        connection.get_cursor().exectue('DELETE FROM Member WHERE UserName=%s', (username,))
        connection.get_connection().commit()

    def test_login_user(self):
        connection = self.connect()

        username = "test_username"
        password = "test_pass"
        firstname = "test_fname"
        lastname = "test_lname"
        email = "test_email@test.com"
        status = "user"
        active = True

        # Create obj to access user data in db
        user_data_access_obj = UserDataAccess(dbconnect=connection)

        # Remove test_user before trying to add
        connection.get_cursor().exectue('DELETE FROM Member WHERE UserName=%s', (username,))
        connection.get_connection().commit()

        # Insert user into db
        query = connection.get_cursor().mogrify(
            'INSERT INTO Member(Username,Pass,FirstName,LastName,Email,Status,Active) '
            'VALUES(%s,%s,%s,%s,%s,%s,%s)', (username, password, firstname, lastname, email, status, active,))
        connection.get_cursor().execute(query)
        connection.commit()

        # Login in user, returns password
        db_user_pass = user_data_access_obj.login_user(username)

        self.assertEqual(db_user_pass, password)

        # Remove test_user after test
        connection.get_cursor().exectue('DELETE FROM Member WHERE UserName=%s', (username,))
        connection.get_connection().commit()

        # Try login in using non-existant username, raises exception
        self.assertRaises(Exception, user_data_access_obj.login_user(username))

    def test_alter_user(self):
        #TODO Alter all data,
        #TODO Retrieve using new data == success
        #TODO Retrieve using old data == Exception
        connection = self.connect()

        username = "test_username"
        password = "test_pass"
        firstname = "test_fname"
        lastname = "test_lname"
        email = "test_email@test.com"
        status = "user"
        active = True

        altered_password = "altered_test_pass"
        altered_firstname = "altered_test_fname"
        altered_lastname = "altered_test_lname"
        altered_email = "altered_est_email@test.com"
        altered_status = "admin"
        altered_active = False

        # Create obj to access user data in db
        user_data_access_obj = UserDataAccess(dbconnect=connection)

        # Remove test_user before trying to add
        connection.get_cursor().exectue('DELETE FROM Member WHERE UserName=%s', (username,))
        connection.get_connection().commit()

        # Insert user into db
        query = connection.get_cursor().mogrify(
            'INSERT INTO Member(Username,Pass,FirstName,LastName,Email,Status,Active) '
            'VALUES(%s,%s,%s,%s,%s,%s,%s)', (username, password, firstname, lastname, email, status, active,))
        connection.get_cursor().execute(query)
        connection.commit()

        # Create user_obj and altered_user_obj
        user_obj = User(username=username, password=password, firstname=firstname, lastname=lastname, email=email,
                        status=status, active=active)
        altered_user_obj = User(username=username, password=altered_password, firstname=altered_firstname, lastname=altered_lastname, email=altered_email,
                        status=altered_status, active=altered_active)

        user_data_access_obj.alter_user(altered_user_obj)

        # Retrieve db_user_obj from db
        connection.get_cursor().execute(
            'SELECT Username,Pass, FirstName, LastName, Email, Status, Active FROM Member WHERE Username=%s;',
            (username,))
        row = connection.get_cursor().fetchone()

        self.assertIsNotNone(row)
        db_altered_user_obj = User(row['username'], row['pass'], row['firstname'], row['lastname'], row['email'],
                           row['status'], row['active'])

        self.assertEqual(altered_user_obj, db_altered_user_obj)

        # Remove test_user after test
        connection.get_cursor().exectue('DELETE FROM Member WHERE UserName=%s', (username,))
        connection.get_connection().commit()

        # Try altering non existant user, raises exception
        self.assertRaises(Exception, user_data_access_obj.alter_user(altered_user_obj))
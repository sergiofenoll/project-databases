from app.user_service.models import User
from app import database as db, user_data_access
import unittest


def _ci(*args: str):
    if len(args) == 1:
        return '"{}"'.format(str(args[0]).replace('"', '""'))
    return ['"{}"'.format(str(arg).replace('"', '""')) for arg in args]


def _cv(*args: str):
    if len(args) == 1:
        return "'{}'".format(str(args[0]).replace("'", "''"))
    return ["'{}'".format(str(arg).replace("'", "''")) for arg in args]


def insert_user_with_query(username, password, firstname, lastname, email, active, status):
    # Insert user into db
    query = 'INSERT INTO Member(Username,Pass,FirstName,LastName,Email,Status,Active) VALUES({},{},{},{},{},{},{})'.format(
        *_cv(username, password, firstname, lastname, email, status), active)
    db.engine.execute(query)


def delete_user_with_query(username):
    # Remove test_user after test
    query = 'DELETE FROM Member WHERE UserName={}'.format(_cv(username))
    db.engine.execute(query)


class TestUserService(unittest.TestCase):

    def test_cmp_users(self):
        username = "test_username"
        password = "test_pass"
        firstname = "test_fname"
        lastname = "test_lname"
        email = "test_email@test.com"
        status = "user"
        active = True

        # Create user_obj to compare with self
        user_obj1 = User(username=username, password=password, firstname=firstname, lastname=lastname, email=email,
                         status=status, active=active)
        user_obj2 = User(username=username, password=password, firstname=firstname, lastname=lastname, email=email,
                         status=status, active=active)

        self.assertEqual(user_obj1, user_obj2)

    def test_add_user(self):
        username = "test_username"
        password = "test_pass"
        firstname = "test_fname"
        lastname = "test_lname"
        email = "test_email@test.com"
        status = "user"
        active = True

        # Create user_obj to add to db
        user_obj = User(username=username, password=password, firstname=firstname, lastname=lastname, email=email,
                        status=status, active=active)

        # Add user to db using UserDataAccess class
        user_data_access.add_user(user_obj)

        # Retrieve test_user data from db
        query = 'SELECT * FROM Member WHERE Username={};'.format(_cv(username))

        result = db.engine.execute(query)
        row = result.first()

        if row is not None:
            db_user_obj = User(row['username'], row['pass'], row['firstname'], row['lastname'], row['email'],
                               row['status'], row['active'])
            self.assertEqual(user_obj, db_user_obj)

            # Remove test_user after test
            delete_user_with_query(username)

    def test_get_user(self):
        username = "test_username"
        password = "test_pass"
        firstname = "test_fname"
        lastname = "test_lname"
        email = "test_email@test.com"
        status = "user"
        active = True

        insert_user_with_query(username, password, firstname, lastname, email, active, status)

        # Create user_obj to compare with db_user_obj
        user_obj = User(username=username, password=password, firstname=firstname, lastname=lastname, email=email,
                        status=status, active=active)

        # Retrieve db_user_obj using UserDataAccess class
        db_user_obj = user_data_access.get_user(username)

        self.assertEqual(user_obj, db_user_obj)

        # Delete user after test
        delete_user_with_query(username)

    def test_get_users(self):
        username = "test_username"
        password = "test_pass"
        firstname = "test_fname"
        lastname = "test_lname"
        email = "test_email@test.com"
        status = "user"
        active = True

        # Insert user into dd
        insert_user_with_query(username, password, firstname, lastname, email, active, status)

        # Create user_obj to compare with db_user_obj
        user_obj = User(username=username, password=password, firstname=firstname, lastname=lastname, email=email,
                        status=status, active=active)

        # Retrieve db_user_obj using UserDataAccess class
        db_user_objs = user_data_access.get_users()
        db_user_found = False

        for db_user_obj in db_user_objs:
            if db_user_obj.username == user_obj.username:
                db_user_found = True

                self.assertEqual(user_obj, db_user_obj)
                break

        self.assertTrue(db_user_found)

        # Remove test_user after test
        delete_user_with_query(username)

    def test_login_user(self):
        username = "test_username"
        password = "test_pass"
        firstname = "test_fname"
        lastname = "test_lname"
        email = "test_email@test.com"
        status = "user"
        active = True

        # Insert user into db
        insert_user_with_query(username, password, firstname, lastname, email, active, status)

        # Login in user, returns password
        db_user_pass = user_data_access.login_user(username)

        self.assertEqual(db_user_pass, password)

        # Remove test_user after test
        delete_user_with_query(username)

        # Try login in using non-existant username, raises exception
        self.assertRaises(Exception, user_data_access.login_user, username)

    def test_alter_user(self):
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

        # Insert user into db
        insert_user_with_query(username, password, firstname, lastname, email, active, status)

        # Create altered_user_obj
        altered_user_obj = User(username=username, password=altered_password, firstname=altered_firstname,
                                lastname=altered_lastname, email=altered_email,
                                status=altered_status, active=altered_active)

        user_data_access.alter_user(altered_user_obj)

        # Retrieve db_user_obj from db
        query = 'SELECT Username,Pass, FirstName, LastName, Email, Status, Active FROM Member WHERE Username={};'.format(
            _cv(username))
        result = db.engine.execute(query)
        row = result.first()

        if row is not None:
            db_altered_user_obj = User(row['username'], row['pass'], row['firstname'], row['lastname'], row['email'],
                                       row['status'], row['active'])
            self.assertEqual(altered_user_obj, db_altered_user_obj)

            # Remove test_user after test
            delete_user_with_query(username)
        else:
            assert False

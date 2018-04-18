from app import app
from app import database as db
from data_service.models import _ci, _cv

class User:
    def __init__(self, username, password, firstname, lastname, email, status, active):
        self.username = username
        self.password = password
        self.firstname = firstname
        self.lastname = lastname
        self.email = email
        self.status = status

        # Members required by flask-login
        self.is_active = active
        self.is_authenticated = True
        self.is_anonymous = False

        # Data handling variables
        self.active_schema = ""

    def get_id(self):
        return self.username

    def to_dct(self):
        return {'Username': self.username, 'First name': self.firstname, 'Last name': self.lastname,
                'Email': self.email, 'Status': self.status, 'Active': self.is_active}

    def __eq__(self, other):
        return self.username == other.username and self.password == other.password and self.firstname == other.firstname and self.lastname == other.lastname and self.email == other.email and self.is_active == other.is_active and self.status == other.status


class UserDataAccess:
    def __init__(self):
        pass

    def get_users(self):
        rows = db.engine.execute('SELECT Username, Pass, FirstName, LastName, Email, Status, Active FROM Member;')
        quote_objects = list()
        for row in rows:
            quote_obj = User(row['username'], row['pass'], row['firstname'], row['lastname'], row['email'],
                             row['status'], row['active'])
            quote_objects.append(quote_obj)
        return quote_objects

    def add_user(self, user_obj):
        try:
            query = 'INSERT INTO Member(Username,Pass,FirstName,LastName,Email,Status,Active) VALUES({},{},{},{},{},{},{})'.format(*_cv(
                user_obj.username, user_obj.password, user_obj.firstname, user_obj.lastname, user_obj.email,
            user_obj.status), user_obj.is_active)
            db.engine.execute(query)
            return True
        except Exception as e:
            app.logger.error('[ERROR] Unable to add user!')
            app.logger.exception(e)
            return False

    def login_user(self, username):
        try:
            rows = db.engine.execute("SELECT Pass FROM Member WHERE Username={};", (username,))
            row = rows.first()

            if row is None:
                raise Exception("Wrong username.")

            return row[0]
        except Exception as e:
            app.logger.exception(e)
            raise e

    def get_user(self, user_id):
        rows = db.engine.execute(
            'SELECT * FROM Member WHERE Username={};'.format(_cv(user_id)))
        row = rows.first()

        if row is not None:
            user = User(row['username'], row['pass'], row['firstname'], row['lastname'], row['email'],
                        row['status'], row['active'])
            return user
        else:
            return None


    def alter_user(self, user):
        try:
            query = 'UPDATE Member SET Firstname = {}, Lastname = {}}, Email = {}, Pass = {}, Status = {}, Active = {} WHERE Username={};'.format(*_cv(
                user.firstname, user.lastname, user.email, user.password, user.status, user.is_active, user.username))

            db.engine.execute(query)
            return True
        except Exception as e:
            app.logger.exception(e)
            raise e


    def delete_user(self, data_loader, username):
        """remove user and all of its datasets"""
        # remove user deletes every row that depends on it because of cascade deletion
        try:
            # first drop all schemas owned by the user
            query = 'SELECT id FROM dataset WHERE owner = {}'.format(_cv(username))
            rows = db.engine.execute(query)
            for dataset_id in rows:
                data_loader.delete_dataset(dataset_id[0])
            db.engine.execute('DELETE FROM Member WHERE username = {}'.format(_cv(username)))
            return True
        except Exception as e:
            print('Unable to delete user!')
            print(e)
            return False

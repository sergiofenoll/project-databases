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
    def __init__(self, dbconnect):
        self.dbconnect = dbconnect

    def get_users(self):
        cursor = self.dbconnect.get_cursor()
        cursor.execute('SELECT Username, Pass, FirstName, LastName, Email, Status, Active FROM Member;')
        quote_objects = list()
        for row in cursor:
            quote_obj = User(row['username'], row['pass'], row['firstname'], row['lastname'], row['email'],
                             row['status'], row['active'])
            quote_objects.append(quote_obj)
        return quote_objects

    def add_user(self, user_obj):
        cursor = self.dbconnect.get_cursor()

        try:
            query = cursor.mogrify('INSERT INTO Member(Username,Pass,FirstName,LastName,Email,Status,Active) '
                                   'VALUES(%s,%s,%s,%s,%s,%s,%s)',
                                   (user_obj.username, user_obj.password, user_obj.firstname, user_obj.lastname,
                                    user_obj.email,
                                    user_obj.status, user_obj.is_active,))

            cursor.execute(query)

            self.dbconnect.commit()

            return True
        except Exception as e:
            print('Unable to add user!')
            print(e)
            self.dbconnect.rollback()
            return False

    def login_user(self, username):
        cursor = self.dbconnect.get_cursor()

        try:
            cursor.execute("SELECT Pass FROM Member WHERE Username=%s;", (username,))
            row = cursor.fetchone()

            if row is None:
                raise Exception("Wrong username.")

            return row[0]
        except Exception as e:
            self.dbconnect.rollback()
            raise e

    def get_user(self, id):
        cursor = self.dbconnect.get_cursor()

        cursor.execute(
            'SELECT * FROM Member WHERE Username=%s;', (id,))
        row = cursor.fetchone()

        if row is not None:
            user = User(row['username'], row['pass'], row['firstname'], row['lastname'], row['email'],
                        row['status'], row['active'])
            return user
        else:
            return None

    def alter_user(self, user):
        cursor = self.dbconnect.get_cursor()
        try:
            query = cursor.mogrify(
                'UPDATE Member SET Firstname = %s, Lastname = %s, Email = %s, Pass = %s, Status = %s, Active = %s WHERE Username=%s;',
                (user.firstname, user.lastname, user.email, user.password, user.status, user.is_active, user.username))

            cursor.execute(query)

            self.dbconnect.commit()

            return True
        except Exception as e:
            self.dbconnect.rollback()
            raise e

    def delete_user(self, data_loader, username):
        """remove user and all of its datasets"""

        # remove user deletes every row that depends on it because of cascade deletion

        cursor = self.dbconnect.get_cursor()

        try:
            # first drop all schemas owned by the user
            query = cursor.mogrify('SELECT id FROM dataset WHERE owner = %s',
                                   (username,))

            cursor.execute(query)

            for dataset_id in cursor:
                print(dataset_id[0])
                data_loader.delete_dataset(dataset_id[0])

            query = cursor.mogrify('DELETE FROM Member WHERE username = %s',
                                   (username,))

            cursor.execute(query)

            self.dbconnect.commit()

            return True
        except Exception as e:
            print('Unable to delete user!')
            print(e)
            self.dbconnect.rollback()
            return False


import psycopg2
import psycopg2.extras


class DBConnection:
    def __init__(self, dbname, dbuser, dbhost, dbpass):
        try:
            self.conn = psycopg2.connect(
                "dbname='{}' user='{}' host='{}' password='{}'".format(dbname, dbuser, dbhost, dbpass),
                cursor_factory=psycopg2.extras.DictCursor)
        except Exception as e:
            print('[ERROR] Unable to connect to database')
            print(e)
            raise Exception('Unable to connect to database')

    def close(self):
        self.conn.close()

    def get_connection(self):
        return self.conn

    def get_cursor(self):
        return self.conn.cursor()

    def commit(self):
        return self.conn.commit()

    def rollback(self):
        return self.conn.rollback()


class User:
    def __init__(self, username, password, firstname, lastname, email, status, active):
        self.username = username
        self.password = password
        self.firstname = firstname
        self.lastname = lastname
        self.email = email
        self.status = status
        self.active = active

        # FlaskLogin stuff
        self.is_active = active
        self.is_authenticated = True
        self.is_anonymous = False

        # Data handling variables
        self.active_schema = ""

    def get_id(self):
        return self.username

    def to_dct(self):
        return {'Username': self.username, 'First name': self.firstname, 'Last name': self.lastname,
                'Email': self.email, 'Status': self.status, 'Active': self.active}


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
            cursor.execute('INSERT INTO Member(Username,Pass,FirstName,LastName,Email,Status,Active) '
                                   'VALUES(%s,%s,%s,%s,%s,%s,%s)',
                                   (user_obj.username, user_obj.password, user_obj.firstname, user_obj.lastname,
                                    user_obj.email,
                                    user_obj.status, user_obj.active,))

            self.dbconnect.commit()

            return True
        except:
            print('Unable to add user!')
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
        except:
            self.dbconnect.rollback()
            raise Exception("Something went terribly wrong")

    def get_user(self, id):
        cursor = self.dbconnect.get_cursor()

        cursor.execute(
            'SELECT Username,Pass, FirstName, LastName, Email, Status, Active FROM Member WHERE Username=%s;', (id,))
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
                (user.firstname, user.lastname, user.email, user.password, user.status, user.active, user.username))

            cursor.execute(query)

            self.dbconnect.commit()

            return True
        except Exception as e:
            self.dbconnect.rollback()
            raise e

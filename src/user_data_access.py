import psycopg2
from passlib.hash import sha256_crypt


class DBConnection:
    def __init__(self, dbname, dbuser, dbhost, dbpass):
        try:
            self.conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(dbname, dbuser, dbhost, dbpass))
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

    def to_dct(self):
        return {'Username': self.username, 'First name': self.firstname, 'Last name': self.lastname, 
                'Email': self.email, 'Status': self.status, 'Active': self.active}


class UserDataAccess:
    def __init__(self, dbconnect):
        self.dbconnect = dbconnect

    def get_users(self):
        cursor = self.dbconnect.get_cursor()
        cursor.execute('SELECT Username, FirstName, LastName, Email, Status, Active FROM Member;')
        quote_objects = list()
        for row in cursor:
            quote_obj = User(username=row[0], password="", firstname=row[1], lastname=row[2], email=row[3], status=row[4], active=row[5])
            quote_objects.append(quote_obj)
        return quote_objects

    def add_user(self, user_obj):
        cursor = self.dbconnect.get_cursor()

        try:
            query = cursor.mogrify('INSERT INTO Member(Username,Pass,FirstName,LastName,Email,Status,Active) '
                                   'VALUES(%s,%s,%s,%s,%s,%s,%s)',
                           (user_obj.username, user_obj.password, user_obj.firstname, user_obj.lastname, user_obj.email,
                            user_obj.status, user_obj.active,))

            cursor.execute(query)

            self.dbconnect.commit()

            return True
        except:
            print('Unable to add user!')
            self.dbconnect.rollback()
            return False

    def login_user(self, username, password):
        cursor = self.dbconnect.get_cursor()

        try:
            cursor.execute("SELECT Pass FROM Member Where Username=%s;", (username,))
            row = cursor.fetchone()

            if row is None:
                raise (Exception('Wrong username'))

            retrieved_password = row[0]
            result = sha256_crypt.verify(password, retrieved_password)

            return result
        except psycopg2.ProgrammingError as e:
            print("ProgrammingError")
            print(e.pgerror)
            self.dbconnect.rollback()
            return False
        except Exception as e:
            print(e)
            self.dbconnect.rollback()
            return False

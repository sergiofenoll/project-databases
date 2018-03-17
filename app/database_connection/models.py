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

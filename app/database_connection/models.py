import psycopg2
import psycopg2.extras

from app import app


class DBConnection:
    def __init__(self, dbname, dbuser, dbhost, dbpass, use_dict_factory=True):
        try:
            if use_dict_factory:
                self.conn = psycopg2.connect(
                    "dbname='{}' user='{}' host='{}' password='{}'".format(dbname, dbuser, dbhost, dbpass),
                    cursor_factory=psycopg2.extras.DictCursor)
            else:
                self.conn = psycopg2.connect(
                    "dbname='{}' user='{}' host='{}' password='{}'".format(dbname, dbuser, dbhost, dbpass))
        except Exception as e:
            app.logger.error('[ERROR] Unable to connect to database')
            app.logger.exception(e)
            raise Exception('Unable to connect to database')  # TODO: Why not raise e?

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

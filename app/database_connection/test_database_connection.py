import unittest
from config import *
from app.database_connection.models import DBConnection


class TestDatabaseConnection(unittest.TestCase):

    def connect(self):
        connection = DBConnection(dbname=config_data['dbname'], dbuser=config_data['dbuser'],
                                  dbpass=config_data['dbpass'], dbhost=config_data['dbhost'])
        return connection

    def test_connection(self):
        # If an exception is raised, it will show up in the tests
        connection = self.connect()
        connection.close()


if __name__ == '__main__':
    unittest.main()

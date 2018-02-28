import psycopg2
import psycopg2.extras

class DataLoader:

	def __init__(self, dbconnect):
		self.dbconnect = dbconnect


	def create_dataset(self, name, desc, owner_id):
		'''
		 This method takes a name ('nickname') and description and creates a new schema in the database.
		 This new schema is by default available to the given owner. 
		'''

        # Create the schema
        cursor = self.dbconnect.get_cursor()

        schemaname = name # Note: automatically generate unique handle here

        query = cursor.mogrify('CREATE SCHEMA {0} AUTHORIZATION {1};'.format(schemaname, owner_id))
        cursor.execute(query)

        # Add user to the access table
        query = cursor.mogrify(
            'INSERT INTO Access(id_dataset, id_owner, role) VALUES({0}, {1}, {2});'.format(schemaname, owner_id, 'owner'))
        cursor.execute(query)

        # Add schema to dataset table
        query = cursor.mogrify(
            'INSERT INTO Dataset(id, nickname, metadata) VALUES({0}, {1}, {2});'.format(schemaname, name, desc))
        cursor.execute(query)


    def delete_dataset(self, name):
        '''
         This method deletes a schema (and therefore all contained tables) from the database
        '''

        cursor = self.dbconnect.get_cursor()

        # Get schema handle
        query = cursor.mogrify('SELECT id FROM Dataset WHERE nickname = name')
        cursor.execute(query)
        #TODO: Handle cases for multiple schemas with the same nickname
        schema_id = cursor.fetchone()

        # Delete schema
        query = cursor.mogrify('DROP SCHEMA {0} CASCADE'.format(schema_id))
        cursor.execute(query)

        # Clean up the access & dataset tables
        query = cursor.mogrify('DELETE FROM Access WHERE id_dataset = {0}'.format(schema_id))
        cursor.execute(query)

        query = cursor.mogrify('DELETE FROM Dataset WHERE id = {0}'.format(schema_id))

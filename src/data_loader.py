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

        query = cursor.mogrify('SELECT count(*) FROM Dataset;')
        cursor.execute(query);
        schemaID = cursor.fetchone()[0] # Amount of already existing schemas
        schemaname = "schema-" + str(schemaID)

        try:
            query = cursor.mogrify('CREATE SCHEMA \"{0}\";'.format(schemaname))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Failed to created schema '" + name + "'")
            print(e)
            self.dbconnect.rollback()
            raise e

        # Add schema to dataset table
        try:
            query = cursor.mogrify(
                'INSERT INTO Dataset(id, nickname, metadata) VALUES(\'{0}\', \'{1}\', \'{2}\');'.format(schemaname, name, desc))
            cursor.execute(query)

            # Add user to the access table
            query = cursor.mogrify(
                'INSERT INTO Access(id_dataset, id_user, role) VALUES(\'{0}\', \'{1}\', \'{2}\');'.format(schemaname, owner_id, 'owner'))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Failed to insert dataset '" + name + "' into the database")
            print(e)
            self.dbconnect.rollback()
            raise e


    def delete_dataset(self, name):
        '''
         This method deletes a schema (and therefore all contained tables) from the database
        '''

        cursor = self.dbconnect.get_cursor()

        # Get schema handle
        ids = self.get_dataset_id(name)

        schema_id = "";
        if (len(ids) > 1):
            print("[WARNING] Unhandled situation: multiple datasets with name '" + name + "'")
            raise Exception("Can't delete dataset - multiple schemas found")
        else:
            schema_id = ids[0]

        # Delete schema
        try:
            query = cursor.mogrify('DROP SCHEMA \"{0}\" CASCADE;'.format(schema_id))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Failed to delete schema '" + name + "'")
            print(e)
            self.dbconnect.rollback()
            raise e

        # Clean up the access & dataset tables
        try:
            query = cursor.mogrify('DELETE FROM Access WHERE id_dataset = \'{0}\';'.format(schema_id))
            cursor.execute(query)

            query = cursor.mogrify('DELETE FROM Dataset WHERE id = \'{0}\';'.format(schema_id))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Failed to properly remove dataset '" + name + "'")
            print(e)
            self.dbconnect.rollback()
            raise e


    def get_dataset_id(self, name):
        '''
         This method takes a nickname and returns the associated schema's id.
         If there are multiple schemas with this nickname, all of their ids are returned
         Return value is a list
        '''

        cursor = self.dbconnect.get_cursor()
        query = cursor.mogrify('SELECT id FROM Dataset WHERE nickname = \'{0}\';'.format(name))
        cursor.execute(query)

        ids = list()

        for row in cursor:
            ids.append(row["id"])

        return ids


    def insert(self, table, columns, values):
        cursor = self.dbconnect.get_cursor()

        values_list = [x.strip() for x in values.split(',')]
        new_values = ''
        for value in values_list :
            new_values = new_values + '\'' + value + '\','
        new_values = new_values[:-1]
        print(new_values)
        try:
            query = 'INSERT INTO comeon.' + table + ' (' + columns + ') VALUES (' + new_values + ')'
            print(query)
            cursor.execute(query)
            self.dbconnect.commit()
            return True
        except:
            print('Unable to insert into schema')
            self.dbconnect.rollback()
            return False

    def process_csv(file, schema, connection):
        schema_acces = SchemaDataAccess(connection, schema, True)
        with open(file, "r") as csv:
            first = True
            table = file[:-4]
            print(table)
            columns = ''
            for line in csv:
                if first:
                    columns = line
                    first = False
                    create_table(schema, columns, table, connection)
                else:
                    schema_acces.insert(table, columns, line)
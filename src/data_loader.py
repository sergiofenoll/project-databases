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


    def table_exists(self, name, schema):
        '''
         This method returns a bool representing whether the given table exists
        '''

        cursor = self.dbconnect.get_cursor()

        row = list()
        try:
            query = cursor.mogrify(
                "SELECT EXISTS ( SELECT 1 FROM information_schema.tables " +
                "WHERE  table_schema = \'{0}\' AND table_name = \'{1}\');".format(schema, name))
            cursor.execute(query)
            row = cursor.fetchone()

            return row[0]

        except Exception as e:
            print("[ERROR] Couldn't determine existance of table '" + name + "'")
            print(e)
            raise e



    def create_table(self, name, schema, columns):
        '''
         This method takes a schema, name and a list of columns and creates the corresponding table
        '''

        cursor = self.dbconnect.get_cursor()
        schemaname = schema
        if len(schema) == 0:
            schemaname = "public"
        query = 'CREATE TABLE \"{0}\".\"{1}\" ('.format(schemaname, name)
        
        query += 'id serial primary key' # Since we don't know what the actual primary key should be, just assign an ever increasing id

        for column in columns:
            query = query + ', \n' + column + ' varchar(255)'
        query += '\n);'

        try:
            query = cursor.mogrify(query);
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Failed to create table '" + name + "'")
            print(e)
            self.dbconnect.rollback()
            raise e


    def delete_table(self, name, schema):
        cursor = self.dbconnect.get_cursor()
        schemaname = schema
        if len(schema) == 0:
            schemaname = "public"

        try:
            query = cursor.mogrify('DROP TABLE \"{0}\".\"{1}\";'.format(schemaname, name))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Failed to delete table '" + name + "'")
            print(e)
            self.dbconnect.rollback()
            raise e

    def insert_row(self, table, schema, columns, values):
        '''
         This method takes list of values and adds those to the given table.
        ''' 

        cursor = self.dbconnect.get_cursor()

        new_values = ''
        for value in values:
            new_values = new_values + '\'' + value + '\','
        new_values = new_values[:-1]

        schemaname = schema
        if len(schema) == 0:
            schemaname = "public"

        try:
            value_string = ", ".join(["\'{0}\'".format(x) for x in values])
            column_string = ", ".join(["{0}".format(x) for x in columns])
            query = 'INSERT INTO \"{0}\".\"{1}\"({2}) VALUES ({3});'.format(schemaname, table, column_string, value_string)
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Unable to insert into table '" + table + "'")
            print (e)
            self.dbconnect.rollback()
            raise e

    def process_csv(self, file, schema, tablename, append = False):
        '''
         This method takes a filename for a CSV file and processes it into a table.
         A table name should be provided by the user / caller of this method.
         If append = True, a table should already exist & the data will be added to this table
        '''


        table_exists = self.table_exists(tablename, schema)
        if append and not table_exists:
            print("[ERROR] Appending to non-existent table.")
            return
        elif not append and table_exists:
            print("[ERROR] Cannot overwrite existing table.")
            return

        with open(file, "r") as csv:
            first = True
            columns = ''
            columns_list = list()
            for line in csv:
                if first:
                    columns = line
                    first = False
                    columns_list = [x.strip() for x in columns.split(",")]
                    if not append:
                        self.create_table(tablename, schema, columns_list)
                else:
                    values_list = [x.strip() for x in line.split(",")]
                    self.insert_row(tablename, schema, columns_list, values_list)
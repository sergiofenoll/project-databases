import psycopg2
import psycopg2.extras
from zipfile import ZipFile
import shutil


class Dataset:

    def __init__(self, id, name, desc, owner):
        self.name = name
        self.desc = desc
        self.owner = owner
        self.id = id


class Table:

    def __init__(self, name, desc):
        self.name = name
        self.desc = desc


class DataLoader:

    def __init__(self, dbconnect):
        self.dbconnect = dbconnect

    # Dataset & Data handling (inserting/deleting...)
    def create_dataset(self, name, desc, owner_id):
        '''
         This method takes a name ('nickname') and description and creates a new schema in the database.
         This new schema is by default available to the given owner.
        '''

        # Create the schema
        cursor = self.dbconnect.get_cursor()

        query = cursor.mogrify('SELECT count(*) FROM Dataset;')
        cursor.execute(query)
        schemaID = cursor.fetchone()[0]  # Amount of already existing schemas
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

        # Create 'metadata' table for this dataset
        try:
            metadata_name = "\"" + schemaname + "\"" + ".Metadata"
            query = cursor.mogrify(
                'CREATE TABLE {0}(\n'.format(metadata_name) +
                'name VARCHAR(255) PRIMARY KEY,\n' +
                'description VARCHAR(255)\n' +
                ');')
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Failed to create metadata table for schema '" + name + "'")
            print(e)
            self.dbconnect.rollback()
            raise e

    def delete_dataset(self, schema_id):
        '''
         This method deletes a schema (and therefore all contained tables) from the database
        '''

        cursor = self.dbconnect.get_cursor()

        # Clean up the access & dataset tables
        try:
            query = cursor.mogrify('DELETE FROM Access WHERE id_dataset = \'{0}\';'.format(schema_id))
            cursor.execute(query)

            query = cursor.mogrify('DELETE FROM Dataset WHERE id = \'{0}\';'.format(schema_id))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Failed to properly remove dataset '" + schema_id + "'")
            print(e)
            self.dbconnect.rollback()
            raise e

        # Delete schema
        try:
            query = cursor.mogrify('DROP SCHEMA \"{0}\" CASCADE;'.format(schema_id))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Failed to delete schema '" + schema_id + "'")
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

    def table_exists(self, name, schema_id):
        '''
         This method returns a bool representing whether the given table exists
        '''

        cursor = self.dbconnect.get_cursor()

        try:
            query = cursor.mogrify(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables " +
                "WHERE  table_schema = \'{0}\' AND table_name = \'{1}\');".format(schema_id, name))
            cursor.execute(query)
            row = cursor.fetchone()

            return row[0]

        except Exception as e:
            print("[ERROR] Couldn't determine existence of table '" + name + "'")
            print(e)
            raise e

    def create_table(self, name, schema_id, columns, desc="Default description"):
        '''
         This method takes a schema, name and a list of columns and creates the corresponding table
        '''

        cursor = self.dbconnect.get_cursor()
        schemaname = schema_id

        query = 'CREATE TABLE \"{0}\".\"{1}\" ('.format(schemaname, name)

        query += 'id serial primary key'  # Since we don't know what the actual primary key should be, just assign an ever increasing id

        for column in columns:
            query = query + ', \n' + column + ' varchar(255)'
        query += '\n);'

        try:
            query = cursor.mogrify(query)
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Failed to create table '" + name + "'")
            print(e)
            self.dbconnect.rollback()
            raise e

        # Add metadata for this table
        try:
            metadata_name = "\"" + schema_id + "\"" + ".Metadata"
            query = cursor.mogrify(
                'INSERT INTO {0}(name, description) VALUES(\'{1}\', \'{2}\');'.format(metadata_name, name, desc))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Failed to insert metadata for table '" + name + "'")
            print(e)
            self.dbconnect.rollback()
            raise e

    def delete_table(self, name, schema_id):
        cursor = self.dbconnect.get_cursor()
        schemaname = "\"" + schema_id + "\""
        try:
            query = cursor.mogrify('DROP TABLE {0}.{1};'.format(schemaname, name))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Failed to delete table '" + name + "'")
            print(e)
            self.dbconnect.rollback()
            raise e

        # Delete metadata
        try:
            metadata_name = "\"" + schema_id + "\"" + ".Metadata"
            query = cursor.mogrify('DELETE FROM {0} WHERE name = \'{1}\';'.format(metadata_name, name))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Failed to delete metadata for table '" + name + "'")
            print(e)
            self.dbconnect.rollback()
            raise e

    def insert_row(self, table, schema_id, columns, values):
        '''
         This method takes list of values and adds those to the given table.
        '''

        cursor = self.dbconnect.get_cursor()

        new_values = ''
        for value in values:
            new_values = new_values + '\'' + value + '\','
        new_values = new_values[:-1]

        schemaname = schema_id

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

    # Data uploading handling
    def process_csv(self, file, schema_id, tablename, append=False):
        '''
         This method takes a filename for a CSV file and processes it into a table.
         A table name should be provided by the user / caller of this method.
         If append = True, a table should already exist & the data will be added to this table
        '''

        table_exists = self.table_exists(tablename, schema_id)
        if append and not table_exists:
            print("[ERROR] Appending to non-existent table.")
            return
        elif not append and table_exists:
            print("[ERROR] Cannot overwrite existing table.")
            return

        with open(file, "r") as csv:
            first = True
            columns_list = list()
            for line in csv:
                if first:
                    columns = line
                    first = False
                    columns_list = [x.strip() for x in columns.split(",")]
                    if not append:
                        self.create_table(tablename, schema_id, columns_list)
                else:
                    values_list = [x.strip() for x in line.split(",")]
                    self.insert_row(tablename, schema_id, columns_list, values_list)

    def process_zip(self, file, schema_id):
        '''
         This method takes a ZIP archive filled with CSV files, and processes them individually
         The name of the CSV file will be used as table name. If a table with the same name is found
         the data will be appended
        '''

        try:
            with ZipFile(file) as archive:
                # Extract each file, one by one
                members = archive.infolist()

                for m in members:
                    csv = archive.extract(m, "../output/temp")

                    # Determine if this file should append an already existing table & process
                    tablename = csv.split('.csv')[0]
                    tablename = tablename.split('/')[-1]
                    create_new = not self.table_exists(tablename, schema_id)

                    if create_new:
                        self.process_csv(csv, schema_id, tablename)
                    else:
                        self.process_csv(csv, schema_id, tablename, True)

                # Clean up temp folder
                shutil.rmtree("../output/temp")

        except Exception as e:
            print("[ERROR] Failed to load from .zip archive '" + file + "'")
            print(e)

            # Clean up temp folder
            shutil.rmtree("../output/temp")

            raise e

    # Data access handling
    def get_user_datasets(self, user_id):
        '''
         This method takes a user id (username) and returns a list with the datasets available to this user
        '''

        cursor = self.dbconnect.get_cursor()

        try:
            query = cursor.mogrify(
                'SELECT id, nickname, metadata FROM Dataset ds, Access a WHERE (ds.id = a.id_dataset AND a.id_user = \'{0}\');'.format(user_id))
            cursor.execute(query)

            result = list()
            datasets = [x for x in cursor]
            for row in datasets:
                try:
                    # Find owner for this dataset
                    query = cursor.mogrify(
                        'SELECT a.id_user FROM Dataset ds, Access a WHERE (ds.id = \'{0}\' AND a.id_dataset = ds.id AND a.role = \'owner\');'.format(row['id']))
                    cursor.execute(query)
                    owner = cursor.fetchone()[0]

                    schema_id = row['id'].split('-')[1]
                    result.append(Dataset(schema_id, row['nickname'], row['metadata'], owner))
                except Exception as e:
                    print("[WARNING] Failed to find owner of dataset '" + row['nickname'] + "'")
                    print(e)
                    continue

            return result

        except Exception as e:
            print("[ERROR] Failed to fetch available datasets for user '" + user_id + "'.")
            print(e)
            raise e

    def grant_access(self, user_id, schema_id, role='contributer'):

        try:
            cursor = self.dbconnect.get_cursor()

            query = cursor.mogrify(
                    'INSERT INTO Access(id_dataset, id_user, role) VALUES(\'{0}\', \'{1}\', \'{2}\');'.format(schema_id, user_id, role))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Couldn't grant '" + user_id + "' access to '" + schema_id + "'")
            print(e)
            self.dbconnect.rollback()
            raise e

    def remove_access(self, user_id, schema_id):

        try:
            cursor = self.dbconnect.get_cursor()

            query = cursor.mogrify(
                    'DELETE FROM Access WHERE (id_user = \'{0}\' AND id_dataset = \'{1}\');'.format(user_id, schema_id))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            print("[ERROR] Couldn't remove access rights for '" + user_id + "' from '" + schema_id + "'")
            print(e)
            self.dbconnect.rollback()
            raise e

    def get_dataset(self, id):
        '''
         This method returns a 'Dataset' object according to the requested id
        '''
        cursor = self.dbconnect.get_cursor()

        try:
            schema_id = "schema-" + str(id)
            query = cursor.mogrify(
                'SELECT id, nickname, metadata FROM Dataset ds WHERE ds.id = \'{0}\';'.format(schema_id))
            cursor.execute(query)

            ds = cursor.fetchone()
            return Dataset(id, ds['nickname'], ds['metadata'], "")
        except Exception as e:
            print("[ERROR] Couldn't fetch data for dataset.")
            print(e)
            raise e

    def get_tables(self, id):
        '''
         This method returns a list of 'Table' objects associated with the requested dataset
        '''

        try:
            result = list()
            return result
        except Exception as e:
            print("[ERROR] Couldn't fetch tables for dataset.")
            print(e)
            raise e

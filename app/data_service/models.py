import re
import shutil
import csv
from zipfile import ZipFile

from psycopg2 import sql, IntegrityError

from app import app


class Dataset:

    def __init__(self, id, name, desc, owner, moderators=[]):
        self.name = name
        self.desc = desc
        self.owner = owner
        self.moderators = moderators
        self.id = id

class Column:
    def __init__(self, name, type):
        self.name = name
        self.type = type

class Table:

    def __init__(self, name, desc, rows=None, columns=None):
        self.name = name
        self.desc = desc
        self.rows = rows or []
        self.columns = columns or []


class DataLoader:

    def __init__(self, dbconnect):
        self.dbconnect = dbconnect

    # Dataset & Data handling (inserting/deleting...)
    def create_dataset(self, name, desc, owner_id):
        """
         This method takes a name ('nickname') and description and creates a new schema in the database.
         This new schema is by default available to the given owner.
        """

        # Create the schema
        cursor = self.dbconnect.get_cursor()

        query = cursor.mogrify('SELECT COUNT(*) FROM Available_Schema;')
        cursor.execute(query)
        count = cursor.fetchone()[0]  # Amount of schema gaps

        schemaID = -1

        if count == 0:
            query = cursor.mogrify('SELECT COUNT(*) FROM Dataset;')

            cursor.execute(query)
            schemaID = cursor.fetchone()[0]  # Amount of already existing schemas

        else:
            query = cursor.mogrify('SELECT MIN(id) FROM Available_Schema;')
            cursor.execute(query)
            schemaID = cursor.fetchone()[0]  # smallest id of schema gap

            query = cursor.mogrify('DELETE FROM Available_Schema WHERE id = %s;', (str(schemaID),))
            cursor.execute(query)

        if schemaID == -1:
            app.logger.warning("[WARNING] Finding a unique schema-name failed")
            return False

        schemaname = "schema-" + str(schemaID)

        try:
            query = cursor.mogrify(sql.SQL('CREATE SCHEMA {0};').format(sql.Identifier(schemaname)))

            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            app.logger.error("[ERROR] Failed to created schema '" + name + "'")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

        # Add schema to dataset table
        try:
            query = cursor.mogrify(
                'INSERT INTO Dataset(id, nickname, metadata, owner) VALUES(%s, %s, %s, %s);', (schemaname,
                                                                                               name, desc, owner_id,))
            cursor.execute(query)

            # Add user to the access table
            query = cursor.mogrify(
                'INSERT INTO Access(id_dataset, id_user, role) VALUES(%s, %s, %s);', (schemaname,
                                                                                      owner_id,
                                                                                      'owner',))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            app.logger.error("[ERROR] Failed to insert dataset '" + name + "' into the database")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

    def delete_dataset(self, schema_id):
        """
         This method deletes a schema (and therefore all contained tables) from the database
        """

        cursor = self.dbconnect.get_cursor()

        # Clean up the access & dataset tables
        try:
            id = schema_id.split('-')[1]
            query = cursor.mogrify('INSERT INTO Available_Schema (id) VALUES (%s)', (id,))
            cursor.execute(query)

            query = cursor.mogrify('DELETE FROM Dataset WHERE id = %s;', (str(schema_id),))
            cursor.execute(query)

            query = cursor.mogrify(sql.SQL('DROP SCHEMA IF EXISTS {0} CASCADE').format(sql.Identifier(schema_id)))
            cursor.execute(query)

            # check if there are datasets. If not, clean available_schema
            query = cursor.mogrify('SELECT COUNT(*) FROM Dataset;')
            cursor.execute(query)
            count = cursor.fetchone()[0]  # Amount of already existing schemas
            if count == 0:
                query = cursor.mogrify('TRUNCATE Available_Schema;')
                cursor.execute(query)

            self.dbconnect.commit()
        except Exception as e:
            app.logger.error("[ERROR] Failed to properly remove dataset '" + schema_id + "'")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

        # Delete schema
        try:
            query = cursor.mogrify(sql.SQL('DROP SCHEMA IF EXISTS {0} CASCADE;').format(sql.Identifier(schema_id)))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            app.logger.error("[ERROR] Failed to delete schema '" + schema_id + "'")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

    def get_dataset_id(self, name):
        """
         This method takes a nickname and returns the associated schema's id.
         If there are multiple schemas with this nickname, all of their ids are returned
         Return value is a list
        """

        cursor = self.dbconnect.get_cursor()
        query = cursor.mogrify('SELECT id FROM Dataset WHERE nickname = %s;', (name,))
        cursor.execute(query)

        ids = list()

        for row in cursor:
            ids.append(row["id"])

        return ids

    def table_exists(self, name, schema_id):
        """
         This method returns a bool representing whether the given table exists
        """

        cursor = self.dbconnect.get_cursor()

        try:
            query = cursor.mogrify(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables " +
                "WHERE  table_schema = %s AND table_name = %s);", (str(schema_id), name,))
            cursor.execute(query)
            row = cursor.fetchone()

            return row[0]

        except Exception as e:
            app.logger.error("[ERROR] Couldn't determine existence of table '" + name + "'")
            app.logger.exception(e)
            raise e

    def create_table(self, name, schema_id, columns, desc="Default description"):
        """
         This method takes a schema, name and a list of columns and creates the corresponding table
        """

        cursor = self.dbconnect.get_cursor()
        schema_name = 'schema-' + str(schema_id)

        query = 'CREATE TABLE {0}.{1} ('

        query += 'id serial primary key'  # Since we don't know what the actual primary key should be, just assign an id

        for column in columns:
            query = query + ', \n\"' + column + '\" varchar(255)'
        query += '\n);'

        query = sql.SQL(query).format(sql.Identifier(schema_name), sql.Identifier(name))

        try:
            query = cursor.mogrify(query)
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            app.logger.error("[ERROR] Failed to create table '" + name + "'")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

        # Add metadata for this table
        try:

            query = cursor.mogrify('INSERT INTO metadata VALUES(%s, %s, %s);', (schema_name,name, desc,))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            app.logger.error("[ERROR] Failed to insert metadata for table '" + name + "'")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

    def delete_table(self, name, schema_id):
        cursor = self.dbconnect.get_cursor()
        try:
            query = cursor.mogrify(
                sql.SQL('DROP TABLE {0}.{1};').format(sql.Identifier(schema_id), sql.Identifier(name)))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            app.logger.error("[ERROR] Failed to delete table '" + name + "'")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

        # Delete metadata
        try:
            query = cursor.mogrify('DELETE FROM metadata WHERE id_table = %s;', (name,))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            app.logger.error("[ERROR] Failed to delete metadata for table '" + name + "'")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

    def delete_row(self, schema_id, table_name, row_ids):
        cursor = self.dbconnect.get_cursor()
        schema_name = 'schema-' + str(schema_id)
        try:
            for row_id in row_ids:
                query = cursor.mogrify(sql.SQL('DELETE FROM {0}.{1} WHERE id=%s;').format(sql.Identifier(schema_name),
                                                                                          sql.Identifier(table_name)),
                                       (row_id,))
                cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            app.logger.error("[ERROR] Unable to delete row from table '" + table_name + "'")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

    def delete_column(self, schema_id, table_name, column_name):
        cursor = self.dbconnect.get_cursor()
        schema_name = 'schema-' + str(schema_id)
        try:
            query = cursor.mogrify(sql.SQL('ALTER TABLE {0}.{1} DROP COLUMN {2};').format(sql.Identifier(schema_name),
                                                                                          sql.Identifier(table_name),
                                                                                          sql.Identifier(column_name)))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            app.logger.error("[ERROR] Unable to delete column from table '" + table_name + "'")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

    def insert_row(self, table, schema_id, columns, values):
        """
         This method takes list of values and adds those to the given table.
        """

        cursor = self.dbconnect.get_cursor()
        schemaname = 'schema-' + str(schema_id)
        try:
            query = cursor.mogrify(sql.SQL(
                'INSERT INTO {0}.{1}({2}) VALUES %s;').format(sql.Identifier(schemaname), sql.Identifier(table),
                                                              sql.SQL(', ').join(
                                                                  sql.Identifier(column_name) for column_name in
                                                                  columns)), (tuple(values),))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            app.logger.error("[ERROR] Unable to insert row into table '" + table + "'")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

    def insert_column(self, schema_id, table_name, column_name, column_type):
        cursor = self.dbconnect.get_cursor()
        schema_name = 'schema-' + str(schema_id)
        try:
            query = cursor.mogrify(
                sql.SQL('ALTER TABLE {}.{} ADD {} ' + column_type + ' NULL ;').format(sql.Identifier(schema_name),
                                                                                 sql.Identifier(table_name),
                                                                                 sql.Identifier(column_name)))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            app.logger.error("[ERROR] Unable to insert column into table '{}'".format(table_name))
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

    def update_column_type(self, schema_id, table_name, column_name, column_type):
        cursor = self.dbconnect.get_cursor()
        schema_name = 'schema-' + str(schema_id)
        try:
            query = cursor.mogrify(
                sql.SQL('ALTER TABLE {}.{} ALTER {}  TYPE ' + column_type + ' USING {}::' + column_type + ' ;').format(sql.Identifier(schema_name),
                                                                                         sql.Identifier(table_name),
                                                                                         sql.Identifier(column_name),
                                                                                         sql.Identifier(column_name)))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            app.logger.error("[ERROR] Unable to update column type in table '{}'".format(table_name))
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

    # Data uploading handling
    def process_csv(self, file, schema_id, tablename, append=False):
        """
         This method takes a filename for a CSV file and processes it into a table.
         A table name should be provided by the user / caller of this method.
         If append = True, a table should already exist & the data will be added to this table
        """

        table_exists = self.table_exists(tablename, schema_id)
        if append and not table_exists:
            app.logger.error("[ERROR] Appending to non-existent table.")
            return
        elif not append and table_exists:
            app.logger.error("[ERROR] Cannot overwrite existing table.")
            return

        with open(file, "r") as csv:
            first = True
            columns = list()
            for line in csv:
                if first and not append:
                    first = False
                    columns = line.strip().split(',')
                    self.create_table(tablename, schema_id, columns)
                else:
                    values_list = [x.strip() for x in line.split(",")]
                    self.insert_row(tablename, schema_id, columns, values_list)

    def process_zip(self, file, schema_id):
        """
         This method takes a ZIP archive filled with CSV files, and processes them individually
         The name of the CSV file will be used as table name. If a table with the same name is found
         the data will be appended
        """

        try:

            with ZipFile(file) as archive:
                # Extract each file, one by one
                members = archive.infolist()

                for m in members:

                    csv = archive.extract(m, "../output/temp")

                    # Determine if this file should append an already existing table & process
                    tablename = m.filename.split('.csv')[0]
                    tablename = tablename.split('/')[-1]

                    create_new = not self.table_exists(tablename, schema_id)

                    if create_new:
                        self.process_csv(csv, schema_id, tablename)
                    else:
                        self.process_csv(csv, schema_id, tablename, True)

                # Clean up temp folder
                shutil.rmtree("../output/temp")

        except Exception as e:
            app.logger.error("[ERROR] Failed to load from .zip archive '" + file + "'")
            app.logger.exception(e)

            # Clean up temp folder
            shutil.rmtree("../output/temp")

            raise e

    def process_dump(self, file, schema_id):
        """
         This method takes a SQL dump file and processes the INSERT statements,
         either by creating tables and filling them or by filling pre-existing tables.
         All other statements (DELETE, DROP, ...) won't be executed.
        """

        with open(file, 'r') as dump:
            # Read the file as a string, split on ';' and check each statement individually
            for statement in dump.read().strip().split(';'):
                if len(statement.split()) and statement.split()[0] == 'INSERT':  # Only handle INSERT statements
                    # NOTE: An INSERT statement looks like this:
                    # INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...);

                    tablename = statement.split()[2]
                    values_list = list()
                    for values_tuple in re.findall(r'\(.*?\)', statement[statement.find('VALUES'):]):
                        # Tuple is any match of the above regex, e.g. (values1, values2, values3, ...)
                        # after VALUES, i.e. the column names (if they're given) aren't matched
                        values_list.append(re.sub(r'\s|\(|\)', '', values_tuple).split(','))

                    if re.search(r'{}\s\(.*?\)\sVALUES'.format(tablename), statement):
                        # Matches 'table_name (column1, column2, column3, ...) VALUES', \s stands for whitespace
                        columns = re.sub(r'\s|\(|\)', '',
                                         re.findall(r'\(.*?\)', statement[:statement.find('VALUES')])[0]).split(',')
                    else:
                        columns = ['col' + str(i) for i in range(1, len(values_list[0]) + 1)]

                    if not self.table_exists(tablename, schema_id):
                        self.create_table(tablename, schema_id, columns)
                    for values in values_list:
                        self.insert_row(tablename, schema_id, columns, values)

    # Data access handling
    def get_user_datasets(self, user_id):
        """
         This method takes a user id (username) and returns a list with the datasets available to this user
        """

        cursor = self.dbconnect.get_cursor()

        try:
            query = cursor.mogrify(
                'SELECT id, nickname, metadata FROM Dataset ds, Access a WHERE (ds.id = a.id_dataset AND a.id_user = %s);',
                (user_id,))
            cursor.execute(query)

            result = list()
            datasets = [x for x in cursor]
            for row in datasets:
                try:
                    ds_id = row['id']
                    # Find owner for this dataset
                    query = cursor.mogrify(
                        'SELECT a.id_user FROM Dataset ds, Access a WHERE (ds.id = %s AND a.id_dataset = ds.id AND a.role = \'owner\');',
                        (row['id'],))
                    cursor.execute(query)
                    owner = cursor.fetchone()[0]

                    # Find moderators for this dataset
                    query = cursor.mogrify(
                        'SELECT a.id_user FROM Dataset ds, Access a WHERE (ds.id = \'{0}\' AND a.id_dataset = ds.id AND a.role = \'moderator\');'.format(
                            ds_id))
                    cursor.execute(query)
                    moderators = [x for x in cursor]

                    schema_id = ds_id.split('-')[1]
                    result.append(Dataset(schema_id, row['nickname'], row['metadata'], owner, moderators))
                except Exception as e:
                    # TODO: Why is this a warning instead of an error? If we can't find the owner of a dataset,
                    # TODO: i.e. that user has been deleted but his dataset remains, shouldn't that be an error?
                    app.logger.warning("[WARNING] Failed to find owner of dataset '" + row['nickname'] + "'")
                    app.logger.exception(e)
                    continue

            return result

        except Exception as e:
            app.logger.error("[ERROR] Failed to fetch available datasets for user '" + user_id + "'.")
            app.logger.exception(e)
            raise e

    def get_dataset_access(self, schema_id, offset=0, limit='ALL', ordering=None, search=None):
        """
         This method returns a table with the users that have access to this dataset
        """

        try:
            cursor = self.dbconnect.get_cursor()

            # TODO: Stole this cheeky bit from another method so if we decide to fix it there, don't forget to fix this too
            ordering_query = ''
            if ordering is not None:
                # ordering tuple is of the form (columns, asc|desc)
                ordering_query = 'ORDER BY "{}" {}'.format(*ordering)

            schema_name = 'schema-' + str(schema_id)
            search_query = ''
            if search is not None:
                search_query = "WHERE id_dataset='{0}' and (id_user LIKE '%{1}%' or role LIKE '%{1}%')".format(
                    schema_name, search)
            else:
                search_query = "WHERE id_dataset='{0}'".format(schema_name)

            query = cursor.mogrify(
                'SELECT * FROM Access {} {} LIMIT {} OFFSET {};'.format(search_query, ordering_query, limit, offset))
            cursor.execute(query)

            table_name = "schema-" + str(schema_id) + "_access"

            table = Table(table_name, '', columns=self.get_column_names(schema_id, table_name)[1:])
            for row in cursor:
                table.rows.append(row[1:])

            return table

        except Exception as e:
            app.logger.error("[ERROR] Couldn't fetch dataset access")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

    def grant_access(self, user_id, schema_id, role='contributer'):

        try:
            cursor = self.dbconnect.get_cursor()

            schema_id = 'schema-' + str(schema_id);

            query = cursor.mogrify(
                'INSERT INTO Access(id_dataset, id_user, role) VALUES(%s, %s, %s);', (schema_id, user_id, role,))
            cursor.execute(query)
            self.dbconnect.commit()
        except IntegrityError as e:
            app.logger.warning("[WARNING] User " + str(user_id) + " doesn't exists. No access granted")
            app.logger.exception(e)
            self.dbconnect.rollback()
        except Exception as e:
            app.logger.error("[ERROR] Couldn't grant '" + str(user_id) + "' access to '" + str(schema_id) + "'")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

    def remove_access(self, user_id, schema_id):

        schema_name = 'schema-' + str(schema_id)
        try:
            cursor = self.dbconnect.get_cursor()

            query = cursor.mogrify(
                'DELETE FROM Access WHERE (id_user = %s AND id_dataset = %s);', (user_id, schema_name,))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            app.logger.error("[ERROR] Couldn't remove access rights for '" + user_id + "' from '" + schema_id + "'")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

    def has_access(self, user_id, id):

        try:
            cursor = self.dbconnect.get_cursor()
            schema_id = "schema-" + str(id)
            query = cursor.mogrify("SELECT * FROM access WHERE id_user=%s and id_dataset=%s;", (user_id,schema_id,))
            cursor.execute(query)
            for _ in cursor:
                return True
            else:
                return False

        except Exception as e:
            app.logger.error("[ERROR] Couldn't find if '" + user_id + "' has access to '" + schema_id + "'")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

    def get_dataset(self, id):
        """
         This method returns a 'Dataset' object according to the requested id
        """

        cursor = self.dbconnect.get_cursor()

        try:
            schema_id = "schema-" + str(id)
            query = cursor.mogrify(
                'SELECT id, nickname, metadata FROM Dataset ds WHERE ds.id = %s;', (schema_id,))
            cursor.execute(query)
            ds = cursor.fetchone()

            # Find owner for this dataset
            query = cursor.mogrify(
                'SELECT a.id_user FROM Dataset ds, Access a WHERE (ds.id = \'{0}\' AND a.id_dataset = ds.id AND a.role = \'owner\');'.format(
                    schema_id))
            cursor.execute(query)
            owner = cursor.fetchone()[0]

            # Find moderators for this dataset
            query = cursor.mogrify(
                'SELECT a.id_user FROM Dataset ds, Access a WHERE (ds.id = \'{0}\' AND a.id_dataset = ds.id AND a.role = \'moderator\');'.format(
                    schema_id))
            cursor.execute(query)
            moderators = [x[0] for x in cursor]

            return Dataset(id, ds['nickname'], ds['metadata'], owner, moderators)
        except Exception as e:
            app.logger.error("[ERROR] Couldn't fetch data for dataset.")
            app.logger.exception(e)
            raise e

    def get_tables(self, schema_id):
        """
         This method returns a list of 'Table' objects associated with the requested dataset
        """

        cursor = self.dbconnect.get_cursor()

        try:

            # Get all tables from the metadata table in the schema
            schema_name = "schema-" + str(schema_id)
            query = cursor.mogrify('SELECT id_table,metadata FROM metadata WHERE id_dataset=%s;',(schema_name,))
            cursor.execute(query)

            tables = list()
            for row in cursor:
                t = Table(row['id_table'], row['metadata'])
                tables.append(t)

            return tables
        except Exception as e:
            app.logger.error("[ERROR] Couldn't fetch tables for dataset.")
            app.logger.exception(e)
            raise e

    def get_table(self, schema_id, table_name, offset=0, limit='ALL', ordering=None):
        """
         This method returns a list of 'Table' objects associated with the requested dataset
        """

        cursor = self.dbconnect.get_cursor()
        try:
            # Get all tables from the metadata table in the schema
            ordering_query = ''
            if ordering is not None:
                # ordering tuple is of the form (columns, asc|desc)
                ordering_query = 'ORDER BY "{}" {}'.format(*ordering)
            query = cursor.mogrify(
                'SELECT * FROM "{}"."{}" {} LIMIT {} OFFSET %s;'.format('schema-' + str(schema_id), table_name,
                                                                        ordering_query, limit), (offset,))
            cursor.execute(query)

            table = Table(table_name, '', columns=self.get_column_names_and_types(schema_id, table_name))  # Hack-n-slash
            for row in cursor:
                table.rows.append(row)  # skip the system id TODO: find a better solution, this feels like a hack
            return table

        except Exception as e:
            app.logger.error("[ERROR] Couldn't fetch table for dataset.")
            app.logger.exception(e)
            raise e

    def get_column_names(self, schema_id, table_name):
        """
         This method returns a list of column names associated with the given table
        """
        cursor = self.dbconnect.get_cursor()

        try:

            schema = "schema-" + str(schema_id)

            query = cursor.mogrify(
                'SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s;', (
                    schema, table_name,))
            cursor.execute(query)
            result = list()
            for row in cursor:
                result.append(row[0])

            return result

        except Exception as e:
            app.logger.error("[ERROR] Couldn't fetch column names for table '" + table_name + "'.")
            app.logger.exception(e)
            raise e

    def get_column_names_and_types(self, schema_id, table_name):
        """
         This method returns a list of column names associated with the given table
        """
        cursor = self.dbconnect.get_cursor()

        try:

            schema = "schema-" + str(schema_id)

            query = cursor.mogrify(
                'SELECT column_name, data_type FROM information_schema.columns WHERE table_schema=%s AND table_name=%s;', (
                    schema, table_name,))
            cursor.execute(query)
            result = list()
            for row in cursor:
                type = row[1]
                if type == "double precision":
                    type = "double"
                elif (type == "timestamp without time zone" or type == "timestamp with time zone"):
                    type = "timestamp"
                elif type == "character varying":
                    type = "string"

                result.append(Column(row[0], type))

            return result

        except Exception as e:
            app.logger.error("[ERROR] Couldn't fetch column names/types for table '" + table_name + "'.")
            app.logger.exception(e)
            raise e

    def update_dataset_metadata(self, schema_id, new_name, new_desc):
        schema_name = "schema-" + str(schema_id)

        try:
            cursor = self.dbconnect.get_cursor()
            query = cursor.mogrify('UPDATE dataset SET (metadata, nickname) = (%s,%s) WHERE id=%s',(new_desc, new_name,schema_name,))
            cursor.execute(query)

        except Exception as e:
            app.logger.error("[ERROR] Couldn't update dataset metadata.")
            app.logger.exception(e)
            raise e

    def update_table_metadata(self, schema_id, old_table_name, new_table_name, new_desc):
        schema_name = "schema-" + str(schema_id)

        try:
            cursor = self.dbconnect.get_cursor()
            query = cursor.mogrify('UPDATE metadata SET (id_table, metadata) = (%s,%s)'
                                   ' WHERE id_dataset=%s AND id_table=%s',(new_table_name, new_desc, schema_name, old_table_name))
            cursor.execute(query)

            if new_table_name != old_table_name:
                query = cursor.mogrify(sql.SQL('ALTER TABLE {}.{} RENAME TO {};').format(sql.Identifier(schema_name), sql.Identifier(old_table_name),sql.Identifier(new_table_name)))
                cursor.execute(query)


        except Exception as e:
            app.logger.error("[ERROR] Couldn't update table metadata for table " + old_table_name + ".")
            app.logger.exception(e)
            raise e

    # Data export handling
    def export_table(self, filename, schema_id, tablename, separator=",", quote_char="\"", empty_char=""):
        """
         This method return the path to a table exported as a CSV file (that could later be used as input again).
        """

        # Failsafe
        if separator == None or separator == "":
            separator = ","
        if quote_char == None or quote_char == "":
            quote_char = "\""
        if empty_char == None:
            empty_char = ""

        with open(filename, "w") as output:

            line = ""

            # First write the column names
            columns = self.get_column_names(schema_id, tablename)

            csvwriter = csv.writer(output, delimiter=separator, quotechar=quote_char, quoting=csv.QUOTE_ALL, )

            csvwriter.writerow(columns)

            table = self.get_table(schema_id, tablename)
            # Replace empty entries by empty_char
            for r_x in range(len(table.rows)):
                for e_x in range(len(table.rows[r_x])):
                    if table.rows[r_x][e_x] == None or table.rows[r_x][e_x] == "":
                        table.rows[r_x][e_x] = empty_char

            csvwriter.writerows(table.rows)

        return filename

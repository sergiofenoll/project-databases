import csv
import re
import shutil
from datetime import datetime
from zipfile import ZipFile

from psycopg2 import IntegrityError

from app import app, database as db
from app.history.models import History

history = History()


def _ci(*args: str):
    if len(args) == 1:
        return '"{}"'.format(str(args[0]).replace('"', '""'))
    return ['"{}"'.format(str(arg).replace('"', '""')) for arg in args]


def _cv(*args: str):
    if len(args) == 1:
        return "'{}'".format(str(args[0]).replace("'", "''"))
    return ["'{}'".format(str(arg).replace("'", "''")) for arg in args]


class Dataset:
    def __init__(self, id, name, desc, owner, moderators=None):
        self.name = name
        self.desc = desc
        self.owner = owner
        self.moderators = moderators or []
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
    def __init__(self):
        pass

    # Dataset & Data handling (inserting/deleting...)
    def create_dataset(self, name, owner_id, desc="Default description",):
        """
         This method takes a name ('nickname') and description and creates a new schema in the database.
         This new schema is by default available to the given owner.
        """

        # Create the schema
        rows = db.engine.execute('SELECT COUNT(*) FROM Available_Schema;')
        count = rows.first()[0]  # Amount of schema gaps

        schema_id = -1

        if count == 0:
            rows = db.engine.execute('SELECT COUNT(*) FROM Dataset;')
            schema_id = rows.first()[0]  # Amount of already existing schemas

        else:
            rows = db.engine.execute('SELECT MIN(id) FROM Available_Schema;')
            schema_id = rows.first()[0]  # smallest id of schema gap
            db.engine.execute('DELETE FROM Available_Schema WHERE id = {};'.format(str(schema_id)))

        if schema_id == -1:
            app.logger.warning("[WARNING] Finding a unique schema-name failed")
            return False

        schema_name = "schema-" + str(schema_id)

        try:
            db.engine.execute('CREATE SCHEMA {};'.format(_ci(schema_name)))
        except Exception as e:
            app.logger.error("[ERROR] Failed to created schema '" + name + "'")
            app.logger.exception(e)
            raise e

        # Add schema to dataset table
        try:
            db.engine.execute(
                'INSERT INTO Dataset(id, nickname, metadata, owner) VALUES({}, {}, {}, {});'.format(
                    *_cv(schemaname, name, desc, owner_id)))

            # Add user to the access table
            db.engine.execute(
                'INSERT INTO Access(id_dataset, id_user, role) VALUES({}, {}, {});'.format(
                    *_cv(schemaname, owner_id, 'owner')))
        except Exception as e:
            app.logger.error("[ERROR] Failed to insert dataset '" + name + "' into the database")
            app.logger.exception(e)
            raise e

    def delete_dataset(self, schema_id):
        """
         This method deletes a schema (and therefore all contained tables) from the database
        """

        # Clean up the access & dataset tables
        try:
            id = schema_id.split('-')[1]
            db.engine.execute('INSERT INTO Available_Schema (id) VALUES ({})'.format(_cv(id)))

            db.engine.execute('DELETE FROM Dataset WHERE id = {};'.format(_cv(str(schema_id))))

            db.engine.execute('DROP SCHEMA IF EXISTS {} CASCADE;'.format(_ci(schema_id)))

            # check if there are datasets. If not, clean available_schema
            rows = db.engine.execute('SELECT COUNT(*) FROM Dataset;')
            count = rows.first()[0]  # Amount of already existing schemas
            if count == 0:
                db.engine.execute('TRUNCATE Available_Schema;')

        except Exception as e:
            app.logger.error("[ERROR] Failed to properly remove dataset '" + schema_id + "'")
            app.logger.exception(e)
            raise e

        # Delete schema
        try:
            db.engine.execute('DROP SCHEMA IF EXISTS {} CASCADE;'.format(_ci(schema_id)))
        except Exception as e:
            app.logger.error("[ERROR] Failed to delete schema '" + schema_id + "'")
            app.logger.exception(e)
            raise e

    def get_dataset_id(self, name):
        """
         This method takes a nickname and returns the associated schema's id.
         If there are multiple schemas with this nickname, all of their ids are returned
         Return value is a list
        """

        rows = db.engine.execute('SELECT id FROM Dataset WHERE nickname = {};'.format(_cv(name)))

        ids = list()

        for row in rows:
            ids.append(row["id"])

        return ids

    def table_exists(self, name, schema_id):
        """
         This method returns a bool representing whether the given table exists
        """
        try:
            rows = db.engine.execute(
                'SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE  table_schema = {} AND table_name = {});'.format(
                    *_cv(str(schema_id), name)))
            row = rows.first()

            return row[0]

        except Exception as e:
            app.logger.error("[ERROR] Couldn't determine existence of table '" + name + "'")
            app.logger.exception(e)
            raise e

    def create_table(self, name, schema_id, columns, desc="Default description"):
        """
         This method takes a schema, name and a list of columns and creates the corresponding table
        """
        schema_name = 'schema-' + str(schema_id)

        query = 'CREATE TABLE {}.{} ('

        query += 'id serial primary key'  # Since we don't know what the actual primary key should be, just assign an id

        for column in columns:
            query = query + ', \n\"' + column + '\" varchar(255)'
        query += '\n);'

        raw_table_name = "_raw_" + name
        raw_table_query = query.format(*_ci(schema_name, raw_table_name))

        query = query.format(*_ci(schema_name, name))

        try:
            db.engine.execute(query)
            db.engine.execute(raw_table_query)
        except Exception as e:
            app.logger.error("[ERROR] Failed to create table '" + name + "'")
            app.logger.exception(e)
            raise e

        # Log action to history
        history.log_action(schema_id, name, datetime.now(), 'Created table')

        # Add metadata for this table
        try:
            db.engine.execute('INSERT INTO metadata VALUES({}, {}, {});'.format(*_cv(schema_name, name, desc)))
        except Exception as e:
            app.logger.error("[ERROR] Failed to insert metadata for table '" + name + "'")
            app.logger.exception(e)
            raise e

    def delete_table(self, name, schema_id):
        try:
            db.engine.execute('DROP TABLE {}.{};'.format(*_ci(schema_id, name)))
            db.engine.execute('DROP TABLE {}.{};'.format(*_ci(schema_id, "_raw_" + name)))
        except Exception as e:
            app.logger.error("[ERROR] Failed to delete table '" + name + "'")
            app.logger.exception(e)
            raise e

        # Delete metadata
        try:
            db.engine.execute('DELETE FROM metadata WHERE id_table = {};'.format(_cv(name)))
        except Exception as e:
            app.logger.error("[ERROR] Failed to delete metadata for table '" + name + "'")
            app.logger.exception(e)
            raise e

        # Delete history
        try:
            schema_name = 'schema-' + str(schema_id)
            db.engine.execute(
                'DELETE FROM HISTORY WHERE id_dataset={} AND id_table={};'.format(*_cv(schema_name, name)))
        except Exception as e:
            app.logger.error("[ERROR] Failed to delete metadata for table '" + name + "'")
            app.logger.exception(e)
            raise e

    def delete_row(self, schema_id, table_name, row_ids):
        schema_name = 'schema-' + str(schema_id)
        try:
            for row_id in row_ids:
                db.engine.execute('DELETE FROM {}.{} WHERE id={};'.format(*_ci(schema_name, table_name), _cv(row_id)))
                # Log action to history
                history.log_action(schema_id, table_name, datetime.now(), 'Deleted row #' + str(row_id))
        except Exception as e:
            app.logger.error("[ERROR] Unable to delete row from table '" + table_name + "'")
            app.logger.exception(e)
            raise e

    def delete_row_predicate(self, schema_id, table_name, predicates):
        """
         Accepts a list of predicates to delete rows on. 
         Predicates are of the form:
         [[AND | OR] [COLUMN] [CONDITION] [VALUE]]
         (where condition can be a logical operator, contains etc.)
        """
        schema_name = 'schema-' + str(schema_id)

        # Gather all ids for rows to be deleted
        query_select = 'SELECT id FROM {0}.{1} WHERE ('.format(*_ci(schema_name, table_name))
        where_queries = list()
        for p_ix in range(len(predicates)):
            predicate = predicates[p_ix]
            # Special conditions first
            if predicate[2] == "CONTAINS":
                predicate[2] = "LIKE"
                predicate[3] = "%%" + predicate[3] + "%%"
            # TODO: make this safe!
            if (p_ix == 0):
                q = '{0} {1} {2}'.format(_ci(predicate[1]), predicate[2], _cv(str(predicate[3])))
                where_queries.append(q)
            else:
                q = '{0} {1} {2} {3}'.format(predicate[0], _ci(predicate[1]), predicate[2], _cv(str(predicate[3])))
                where_queries.append(q)
        query = query_select + ' '.join(where_queries) + ');'

        try:
            result = db.engine.execute(query)

            to_delete = [r['id'] for r in result]

            # Pass ids to 'traditional' delete_row
            self.delete_row(schema_id, table_name, to_delete)

        except Exception as e:
            app.logger.error('[ERROR] Unable to fetch rows to delete from ' + table_name)
            app.logger.exception(e)

    def delete_column(self, schema_id, table_name, column_name):
        schema_name = 'schema-' + str(schema_id)
        try:
            db.engine.execute('ALTER TABLE {}.{} DROP COLUMN {};'.format(*_ci(schema_name, table_name, column_name)))
        except Exception as e:
            app.logger.error("[ERROR] Unable to delete column from table '" + table_name + "'")
            app.logger.exception(e)
            raise e

        # Log action to history
        history.log_action(schema_id, table_name, datetime.now(), 'Deleted column ' + column_name)

    def insert_row(self, table, schema_id, columns, values, file_upload=False):
        """
         This method takes dict of values and adds those to the given table.
         The entries in values look like: {column_name: column_value}
        """
        schemaname = 'schema-' + str(schema_id)
        # Create column/value tuples in proper order
        column_tuple = list()
        value_tuple = list()
        for col in values.keys():
            if values[col] != '':
                column_tuple.append(col)
                value_tuple.append(values[col])
        try:
            query = 'INSERT INTO {}.{}({}) VALUES ({});'.format(*_ci(schemaname, table),
                                                                ', '.join(
                                                                    _ci(column_name) for column_name in column_tuple),
                                                                ', '.join(_cv(value) for value in value_tuple))
            db.engine.execute(query)
        except Exception as e:
            app.logger.error("[ERROR] Unable to insert row into table '" + table + "'")
            app.logger.exception(e)
            raise e

        # Log action to history
        if not file_upload:
            history.log_action(schema_id, table, datetime.now(), 'Added row with values ' + ' '.join(values))

    def insert_column(self, schema_id, table_name, column_name, column_type):
        schema_name = 'schema-' + str(schema_id)
        try:
            db.engine.execute(
                'ALTER TABLE {}.{} ADD {} {} NULL;'.format(*_ci(schema_name, table_name, column_name), column_type))
        except Exception as e:
            app.logger.error("[ERROR] Unable to insert column into table '{}'".format(table_name))
            app.logger.exception(e)
            raise e

        # Log action to history
        history.log_action(schema_id, table_name, datetime.now(), 'Added column with name ' + column_name)

    def rename_column(self, schema_id, table_name, column_name, new_column_name):
        schema_name = 'schema-' + str(schema_id)
        try:
            db.engine.execute(
                'ALTER TABLE {0}.{1} RENAME {2} TO {3}'.format(
                    *_ci(schema_name, table_name, column_name, new_column_name)))
        except Exception as e:
            app.logger.error(
                "[ERROR] Unable to rename column '{0}' to '{1}' in table '{2}'".format(column_name, new_column_name,
                                                                                       table_name))
            app.logger.exception(e)

    def update_column_type(self, schema_id, table_name, column_name, column_type):
        schema_name = 'schema-' + str(schema_id)
        db.engine.execute('ALTER DATABASE userdb SET datestyle TO "ISO, MDY";')
        try:
            db.engine.execute(
                'ALTER TABLE {0}.{1} ALTER {2} TYPE {3} USING {2}::{3};'.format(
                    *_ci(schema_name, table_name, column_name), column_type))
        except Exception as e:
            app.logger.error("[ERROR] Unable to update column type in table '{}'".format(table_name))
            app.logger.exception(e)
            raise e

        # Log action to history
        history.log_action(schema_id, table_name, datetime.now(),
                           'Updated column ' + column_name + ' to have type ' + column_type)

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

        import pandas as pd

        # TODO: Test if this works
        raw_tablename = '_raw_' + tablename
        schema_name = 'schema-' + str(schema_id)

        with open(file, "r") as csv:
            for line in csv:
                if not append:
                    columns = line.strip().split(',')
                    self.create_table(tablename, schema_id, columns)
                break

        df = pd.read_csv(file)
        df.to_sql(name=tablename, con=db.engine, schema=schema_name, index=False, if_exists='append')
        df.to_sql(name=raw_tablename, con=db.engine, schema=schema_name, index=False, if_exists='append')

        # with open(file, "r") as csv:
        #     first = True
        #     columns = list()
        #     for line in csv:
        #         if first and not append:
        #             first = False
        #             columns = line.strip().split(',')
        #             self.create_table(tablename, schema_id, columns)
        #         else:
        #             raw_name = "_raw_" + tablename
        #             values_list = [x.strip() for x in line.split(",")]
        #             values = dict()
        #             for c in range(len(columns)):
        #                 values[columns[c]] = values_list[c]
        #             self.insert_row(tablename, schema_id, columns, values, True)
        #             self.insert_row(raw_name, schema_id, columns, values, True)

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
                        val_dict = dict()
                        for c_ix in range(len(columns)):
                            val_dict[columns[c_ix]] = values[c_ix]
                        self.insert_row(tablename, schema_id, columns, val_dict, True)

    # Data access handling
    def get_user_datasets(self, user_id):
        """
         This method takes a user id (username) and returns a list with the datasets available to this user
        """
        try:
            rows = db.engine.execute(
                'SELECT id, nickname, metadata FROM Dataset ds, Access a WHERE (ds.id = a.id_dataset AND a.id_user = {});'.format(
                    _cv(user_id)))

            result = list()
            datasets = [x for x in rows]
            for row in datasets:
                try:
                    ds_id = row['id']
                    # Find owner for this dataset
                    rows = db.engine.execute(
                        "SELECT a.id_user FROM Dataset ds, Access a WHERE (ds.id = {} AND a.id_dataset = ds.id AND a.role = 'owner');".format(
                            _cv(row['id'])))
                    owner = rows.first()[0]

                    # Find moderators for this dataset
                    rows = db.engine.execute(
                        "SELECT a.id_user FROM Dataset ds, Access a WHERE (ds.id = {} AND a.id_dataset = ds.id AND a.role = 'moderator');".format(
                            _cv(ds_id)))
                    moderators = [x for x in rows]

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
            # TODO: Stole this cheeky bit from another method so if we decide to fix it there, don't forget to fix this too
            ordering_query = ''
            if ordering is not None:
                # ordering tuple is of the form (columns, asc|desc)
                ordering_query = 'ORDER BY {} {}'.format(_ci(ordering[0]), ordering[1])

            schema_name = 'schema-' + str(schema_id)
            search_query = ''
            if search is not None:
                search_query = "WHERE id_dataset={0} and (id_user LIKE '%%{1}%%' or role LIKE '%%{1}%%')".format(
                    _cv(schema_name), search.replace("'", "''"))
            else:
                search_query = "WHERE id_dataset={}".format(_cv(schema_name))

            rows = db.engine.execute(
                'SELECT * FROM Access {} {} LIMIT {} OFFSET {};'.format(search_query, ordering_query, limit, offset))

            table_name = "schema-" + str(schema_id) + "_access"

            table = Table(table_name, '', columns=self.get_column_names(schema_id, table_name)[1:])
            for row in rows:
                table.rows.append(row[1:])

            return table

        except Exception as e:
            app.logger.error("[ERROR] Couldn't fetch dataset access")
            app.logger.exception(e)
            raise e

    def grant_access(self, user_id, schema_id, role='contributer'):

        try:
            schema_id = 'schema-' + str(schema_id);

            db.engine.execute(
                'INSERT INTO Access(id_dataset, id_user, role) VALUES({}, {}, {});'.format(
                    *_cv(schema_id, user_id, role)))
        except IntegrityError as e:
            app.logger.warning("[WARNING] User " + str(user_id) + " doesn't exists. No access granted")
            app.logger.exception(e)
        except Exception as e:
            app.logger.error("[ERROR] Couldn't grant '" + str(user_id) + "' access to '" + str(schema_id) + "'")
            app.logger.exception(e)
            raise e

    def remove_access(self, user_id, schema_id):
        schema_name = 'schema-' + str(schema_id)
        try:
            db.engine.execute(
                'DELETE FROM Access WHERE (id_user = {} AND id_dataset = {});'.format(*_cv(user_id, schema_name)))
        except Exception as e:
            app.logger.error("[ERROR] Couldn't remove access rights for '" + user_id + "' from '" + schema_id + "'")
            app.logger.exception(e)
            raise e

    def has_access(self, user_id, id):
        schema_id = "schema-" + str(id)
        try:
            rows = db.engine.execute(
                "SELECT * FROM access WHERE id_user={} AND id_dataset={};".format(*_cv(user_id, schema_id)))
            for _ in rows:
                return True
            else:
                return False

        except Exception as e:
            app.logger.error("[ERROR] Couldn't find if '" + user_id + "' has access to '" + schema_id + "'")
            app.logger.exception(e)
            raise e

    def get_dataset(self, id):
        """
         This method returns a 'Dataset' object according to the requested id
        """
        try:
            schema_id = "schema-" + str(id)
            rows = db.engine.execute(
                'SELECT id, nickname, metadata FROM Dataset ds WHERE ds.id = {};'.format(_cv(schema_id)))
            ds = rows.first()

            # Find owner for this dataset
            rows = db.engine.execute(
                "SELECT a.id_user FROM Dataset ds, Access a WHERE (ds.id = {} AND a.id_dataset = ds.id AND a.role = 'owner');".format(
                    _cv(schema_id)))
            owner = rows.first()[0]

            # Find moderators for this dataset
            rows = db.engine.execute(
                "SELECT a.id_user FROM Dataset ds, Access a WHERE (ds.id = {} AND a.id_dataset = ds.id AND a.role = 'moderator');".format(
                    _cv(schema_id)))
            moderators = [x[0] for x in rows]

            return Dataset(id, ds['nickname'], ds['metadata'], owner, moderators)
        except Exception as e:
            app.logger.error("[ERROR] Couldn't fetch data for dataset.")
            app.logger.exception(e)
            raise e

    def get_tables(self, schema_id):
        """
         This method returns a list of 'Table' objects associated with the requested dataset
        """
        try:
            # Get all tables from the metadata table in the schema
            schema_name = "schema-" + str(schema_id)
            rows = db.engine.execute(
                'SELECT id_table,metadata FROM metadata WHERE id_dataset={};'.format(_cv(schema_name)))

            tables = list()
            for row in rows:
                t = Table(row['id_table'], row['metadata'])
                tables.append(t)

            return tables
        except Exception as e:
            app.logger.error("[ERROR] Couldn't fetch tables for dataset.")
            app.logger.exception(e)
            raise e

    def get_table(self, schema_id, table_name, offset=0, limit='ALL', ordering=None, search=None):
        """
         This method returns a list of 'Table' objects associated with the requested dataset
        """
        try:

            columns = self.get_column_names(schema_id, table_name)

            schema_name = 'schema-' + str(schema_id)
            # Get all tables from the metadata table in the schema
            ordering_query = ''
            if ordering is not None:
                # ordering tuple is of the form (columns, asc|desc)
                ordering_query = 'ORDER BY {} {}'.format(_ci(ordering[0]), ordering[1])

            search_query = ''
            if search is not None and search != '':
                search_query = "WHERE (";
                # Fill in the search for every column except ID
                for col in columns[1:]:
                    search_query += "{}::text LIKE '%%{}%%' OR ".format(_ci(col), search)
                search_query = search_query[:-3] + ")"

            rows = db.engine.execute(
                'SELECT * FROM {}.{} {} {} LIMIT {} OFFSET {};'.format(*_ci(schema_name, table_name), search_query,
                                                                       ordering_query, limit, offset))
            table = Table(table_name, '',
                          columns=self.get_column_names_and_types(schema_id, table_name))  # Hack-n-slash
            for row in rows:
                table.rows.append(list(row))
            return table

        except Exception as e:
            app.logger.error("[ERROR] Couldn't fetch table for dataset.")
            app.logger.exception(e)
            raise e

    def get_column_names(self, schema_id, table_name):
        """
         This method returns a list of column names associated with the given table
        """
        try:
            schema = "schema-" + str(schema_id)
            rows = db.engine.execute(
                'SELECT column_name FROM information_schema.columns WHERE table_schema={} AND table_name={};'.format(
                    *_cv(schema, table_name)))
            result = list()
            for row in rows:
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
        try:
            schema = "schema-" + str(schema_id)
            rows = db.engine.execute(
                'SELECT column_name, data_type FROM information_schema.columns WHERE table_schema={} AND table_name={};'.format(
                    *_cv(schema, table_name)))
            result = list()
            for row in rows:
                type = row[1]
                if type == "double precision":
                    type = "double"
                elif type == "timestamp without time zone" or type == "timestamp with time zone":
                    type = "timestamp"
                elif type == "character varying":
                    type = "text"
                elif type == "bigint":
                    type = "integer"
                result.append(Column(row[0], type))
            return result
        except Exception as e:
            app.logger.error("[ERROR] Couldn't fetch column names/types for table '" + table_name + "'.")
            app.logger.exception(e)
            raise e

    def update_dataset_metadata(self, schema_id, new_name, new_desc):
        schema_name = "schema-" + str(schema_id)
        try:
            db.engine.execute('UPDATE dataset SET (metadata, nickname) = ({} , {}) WHERE id={}'.format(
                *_cv(new_desc, new_name, schema_name, )))

        except Exception as e:
            app.logger.error("[ERROR] Couldn't update dataset metadata.")
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

    def update_table_metadata(self, schema_id, old_table_name, new_table_name, new_desc):
        schema_name = "schema-" + str(schema_id)
        try:
            db.engine.execute(
                'UPDATE metadata SET (id_table, metadata) = ({}, {}) WHERE id_dataset={} AND id_table={}'.format(
                    *_cv(new_table_name, new_desc, schema_name, old_table_name)))
            if new_table_name != old_table_name:
                db.engine.execute(
                    'ALTER TABLE {}.{} RENAME TO {};'.format(*_ci(schema_name, old_table_name, new_table_name)))
                raw_table_old_name = "_raw_" + old_table_name
                raw_table_new_name = "_raw_" + new_table_name
                db.engine.execute(
                    'ALTER TABLE {}.{} RENAME TO {};'.format(*_ci(schema_name, raw_table_old_name, raw_table_new_name)))
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

    def get_numerical_statistic(self, schema_id, table_name, column, function):

        """" calculate average of column """
        try:
            schema_name = 'schema-' + str(schema_id)
            rows = db.engine.execute(
                'SELECT ' + function + '( "{}" ) FROM "{}"."{}"'.format(column, schema_name, table_name))
            stat = rows.first()[0]
            if not stat:
                stat = 0
            return stat

        except Exception as e:
            app.logger.error("[ERROR] Unable to calculate {} for column {}".format(function.lower(), column))
            app.logger.exception(e)
            raise e

    def calculate_most_common_value(self, schema_id, table_name, column):
        """" calculate most common value of a  column """
        try:
            schema_name = 'schema-' + str(schema_id)
            rows = db.engine.execute('SELECT "{}", COUNT(*) AS counted '
                                     'FROM "{}"."{}" '
                                     'WHERE "{}" IS NOT NULL '
                                     'GROUP BY "{}" '
                                     'ORDER BY counted DESC, "{}" '
                                     'LIMIT 1'.format(column, schema_name, table_name, column, column, column))
            value = None
            for x in rows:
                value = x[0]
            return value

        except Exception as e:
            app.logger.error("[ERROR] Unable to calculate most commen value for column {}".format(column))
            app.logger.exception(e)
            raise e

    def calculate_amount_of_empty_elements(self, schema_id, table_name, column):
        """" calculate most common value of a  column """
        try:
            schema_name = 'schema-' + str(schema_id)
            rows = db.engine.execute('SELECT COUNT(*) AS counted '
                                     'FROM "{}"."{}" '
                                     'WHERE "{}" IS NULL;'.format(schema_name, table_name, column))
            value = None
            for x in rows:
                value = x[0]
            return value

        except Exception as e:
            app.logger.error("[ERROR] Unable to calculate most commen value for column {}".format(column))
            app.logger.exception(e)
            raise e

    def get_statistics_for_column(self, schema_id, table_name, column, numerical):
        """calculate statistics of a column"""

        stats = list()

        if numerical:
            stats.append(["Average", self.get_numerical_statistic(schema_id, table_name, column, "AVG")])
            stats.append(["Minimum", self.get_numerical_statistic(schema_id, table_name, column, "MIN")])
            stats.append(["Maximum", self.get_numerical_statistic(schema_id, table_name, column, "MAX")])

        stats.append(["Most common value", self.calculate_most_common_value(schema_id, table_name, column, )])
        stats.append(
            ["Amount of empty elements", self.calculate_amount_of_empty_elements(schema_id, table_name, column, )])
        return stats

    def get_statistics_for_all_columns(self, schema_id, table_name, columns):
        """calculate statistics of a column"""
        stats = list()
        for column in columns:
            type = column.type
            numerical = False
            if type == "integer" or type == "double" or type == "real":
                numerical = True

            stats.append([column.name, self.get_statistics_for_column(schema_id, table_name, column.name, numerical)])
        return stats

    def revert_back_to_raw_data(self, schema_id, table_name):
        schema_name = "schema-" + str(schema_id)
        try:
            db.engine.execute('DROP TABLE {}.{};'.format(*_ci(schema_name, table_name)))
        except Exception as e:
            app.logger.error("[ERROR] Couldn't convert back to raw data")
            app.logger.exception(e)
            raise e

        try:
            raw_table_name = "_raw_" + table_name

            db.engine.execute(
                'CREATE TABLE {}.{} AS TABLE {}.{}'.format(*_ci(schema_name, table_name, schema_name, raw_table_name)))

            # Log action to history
            history.log_action(schema_id, table_name, datetime.now(), 'Reverted to raw data')

        except Exception as e:
            app.logger.error("[ERROR] Couldn't convert back to raw data")
            app.logger.exception(e)
            raise e

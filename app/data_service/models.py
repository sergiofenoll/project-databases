import csv
import re
import shutil
import pandas as pd
from datetime import datetime
from zipfile import ZipFile
from psycopg2 import IntegrityError

from app import app, database as db, ACTIVE_USER_TIME_SECONDS, BACKUP_LIMIT
from app.history.models import History
from app.data_transform.helpers import create_serial_sequence

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
    def __init__(self, id, name, desc, owner, moderators=None, active_users_count=0):
        self.name = name
        self.desc = desc
        self.owner = owner
        self.moderators = moderators or []
        self.id = id
        self.active_users_count = active_users_count

    def __eq__(self, other):
        return self.name == other.name and self.desc == other.desc and self.owner == other.owner and self.id == other.id


class Column:
    def __init__(self, name, type):
        self.name = name
        self.type = type


class Table:
    def __init__(self, name, desc, rows=None, columns=None, active_users_count=0, total_size=0):
        self.name = name
        self.desc = desc
        self.rows = rows or []
        self.columns = columns or []
        self.active_users_count = active_users_count
        self.total_size = total_size

    def __eq__(self, other):
        return self.name == other.name and self.desc == other.desc


class ActiveUserHandler:
    def __init__(self):
        pass

    def remove_inactive_users_in_tables(self):
        """ remove all the users who aren't active in a table from the list"""
        try:
            db.engine.execute(
                'DELETE FROM Active_In_Table WHERE EXTRACT(EPOCH FROM (now() - last_active)) > {};'.format(
                    ACTIVE_USER_TIME_SECONDS))
        except Exception as e:
            app.logger.error("[ERROR] Unable to remove all the users who aren't active in a table from the list")
            app.logger.exception(e)
            raise e

    def make_user_active_in_table(self, schema_id, table_name, user_id):
        """" make a user active in a table """
        try:
            schema_name = "schema-" + str(schema_id)
            exists = db.engine.execute(
                'SELECT EXISTS(SELECT 1 FROM Active_In_Table WHERE id_dataset = {} AND id_table = {} and id_user = {});'
                    .format(*_cv(schema_name, table_name, user_id)))
            if exists.first()[0]:
                db.engine.execute(
                    'UPDATE Active_In_Table VALUES SET last_active=NOW() '
                    'WHERE id_dataset = {} AND id_table = {} and id_user = {}'.format(
                        *_cv(schema_name, table_name, user_id)))
            else:
                db.engine.execute(
                    'INSERT INTO Active_In_Table VALUES ({},{},{},NOW());'.format(
                        *_cv(schema_name, table_name, user_id)))
        except Exception as e:
            app.logger.error("[ERROR] Unable to make a user active in dataset {}".format(table_name))
            app.logger.exception(e)
            raise e

    def make_user_active_in_dataset(self, schema_id, user_id):
        """" make a user active in a table """
        try:
            schema_name = "schema-" + str(schema_id)
            ex = db.engine.execute(
                'SELECT EXISTS(SELECT 1 FROM Active_In_Table WHERE id_dataset = {} AND id_table IS NULL and id_user = {});'
                    .format(*_cv(schema_name, user_id)))
            exists = ex.first()[0]
            if exists:
                db.engine.execute(
                    'UPDATE Active_In_Table VALUES SET last_active=NOW() '
                    'WHERE id_dataset = {} AND id_table IS NULL and id_user = {}'.format(
                        *_cv(schema_name, user_id)))
            else:
                db.engine.execute(
                    'INSERT INTO Active_In_Table VALUES ({},NULL,{},NOW());'.format(*_cv(schema_name, user_id)))
        except Exception as e:
            app.logger.error("[ERROR] Unable to make a user active in dataset")
            app.logger.exception(e)
            raise e

    def get_active_users_in_table(self, schema_id, table_name, user_id):
        """ give the names of active users in table"""
        try:
            self.remove_inactive_users_in_tables()
            schema_name = "schema-" + str(schema_id)
            rows = db.engine.execute(
                'SELECT DISTINCT id_user FROM Active_In_Table WHERE id_dataset = {} AND id_table = {} AND id_user <> {};'
                    .format(*_cv(schema_name, table_name, user_id)))

            active_users = list()
            for row in rows:
                active_users.append(row["id_user"])
            return active_users

        except Exception as e:
            app.logger.error("[ERROR] Unable to get active users in table {}".format(table_name))
            app.logger.exception(e)
            raise e

    def active_users_in_table_count_excluding_requesting_user(self, schema_id, table_name, user_id):
        """ give the amount of active users in a table without the requesting user"""
        try:
            schema_name = "schema-" + str(schema_id)
            self.remove_inactive_users_in_tables()
            count = db.engine.execute(
                'SELECT COUNT( DISTINCT id_user) FROM Active_In_Table WHERE id_dataset = {} AND id_table = {} and id_user <> {};'
                    .format(*_cv(schema_name, table_name, user_id)))
            return count.first()[0]
        except Exception as e:
            app.logger.error("[ERROR] Unable to get count of active users in table {}".format(table_name))
            app.logger.exception(e)
            raise e

    def active_users_in_dataset_count_excluding_requesting_user(self, schema_id, user_id):
        """ give the amount of active users in a dataset without the requesting user"""
        try:
            schema_name = "schema-" + str(schema_id)
            self.remove_inactive_users_in_tables()
            count = db.engine.execute(
                'SELECT COUNT(DISTINCT id_user) FROM Active_In_Table WHERE id_dataset = {} and id_user <> {};'
                    .format(*_cv(schema_name, user_id)))
            return count.first()[0]
        except Exception as e:
            app.logger.error("[ERROR] Unable to get count of active users in dataset")
            app.logger.exception(e)
            raise e

    def remove_active_states_of_user(self, user_id):
        """ remove all the users who aren't active in a table from the list"""
        try:
            db.engine.execute(
                'DELETE FROM Active_In_Table WHERE id_user = {};'.format(_cv(user_id)))
        except Exception as e:
            app.logger.error("[ERROR] Unable to remove active states of user {}".format(user_id))
            app.logger.exception(e)
            raise e


class DataLoader:
    def __init__(self):
        pass

    # Dataset & Data handling (inserting/deleting...)
    def create_dataset(self, name, owner_id, desc="Default description", ):
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
            raise Exception

        # Add schema to dataset table
        try:
            db.engine.execute(
                'INSERT INTO Dataset(id, nickname, metadata, owner) VALUES({}, {}, {}, {});'.format(
                    *_cv(schema_name, name, desc, owner_id)))

            # Add user to the access table
            db.engine.execute(
                'INSERT INTO Access(id_dataset, id_user, role) VALUES({}, {}, {});'.format(
                    *_cv(schema_name, owner_id, 'owner')))

        except Exception as e:
            app.logger.error("[ERROR] Failed to insert dataset '" + name + "' into the database")
            app.logger.exception(e)
            raise Exception

    def delete_dataset(self, schema_id):
        """
         This method deletes a schema (and therefore all contained tables) from the database
        """

        # Clean up the access & dataset tables
        try:
            schema_name = "schema-" + str(schema_id)
            db.engine.execute('INSERT INTO Available_Schema (id) VALUES ({})'.format(_cv(schema_id)))

            db.engine.execute('DELETE FROM Dataset WHERE id = {};'.format(_cv(schema_name)))

            db.engine.execute('DROP SCHEMA IF EXISTS {} CASCADE;'.format(_ci(schema_name)))

            # check if there are datasets. If not, clean available_schema
            rows = db.engine.execute('SELECT COUNT(*) FROM Dataset;')
            count = rows.first()[0]  # Amount of already existing schemas
            if count == 0:
                db.engine.execute('TRUNCATE Available_Schema;')

        except Exception as e:
            app.logger.error("[ERROR] Failed to properly remove dataset '" + schema_id + "'")
            app.logger.exception(e)

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
        schema_name = 'schema-' + str(schema_id)
        try:
            rows = db.engine.execute(
                'SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE  table_schema = {} AND table_name = {});'.format(
                    *_cv(schema_name, name)))
            row = rows.first()

            return row[0]

        except Exception as e:
            app.logger.error("[ERROR] Couldn't determine existence of table '" + name + "'")
            app.logger.exception(e)
            raise e

    def create_table(self, name, schema_id, columns, desc="Default description", raw=False, metadata_only=False):
        """
         This method takes a schema, name and a list of columns and creates the corresponding table
        """

        connection = db.engine.connect()
        transaction = connection.begin()
        try:
            schema_name = 'schema-' + str(schema_id)
            raw_table_name = "_raw_" + name
            if not metadata_only:
                query = 'CREATE TABLE {}.{} ('

                query += 'id serial primary key'  # Since we don't know what the actual primary key should be, just assign an id

                for column in columns:
                    query = query + ', \n\"' + column + '\" varchar(255)'
                query += '\n);'

                raw_table_query = query.format(*_ci(schema_name, raw_table_name))

                query = query.format(*_ci(schema_name, name))

                try:
                    connection.execute(query)
                    if raw:
                        connection.execute(raw_table_query)
                except Exception as e:
                    app.logger.error("[ERROR] Failed to create table '" + name + "'")
                    app.logger.exception(e)
                    raise e

            # Add metadata for this table
            try:
                connection.execute('INSERT INTO metadata VALUES({}, {}, {});'.format(*_cv(schema_name, name, desc)))
            except Exception as e:
                app.logger.error("[ERROR] Failed to insert metadata for table '" + name + "'")
                app.logger.exception(e)
                raise e

            # Log action to history
            history.log_action(schema_id, name, datetime.now(), 'Created table',
                               'DROP TABLE IF EXISTS {}.{};'.format(*_ci(schema_name, name)) +
                               'DROP TABLE IF EXISTS {}.{};'.format(*_ci(schema_name, raw_table_name)) +
                               'DELETE FROM METADATA WHERE ID_DATASET={} AND ID_TABLE={};'.format(
                                   *_cv(schema_name, name)) +
                               'DELETE FROM HISTORY WHERE ID_DATASET={} AND ID_TABLE={};'.format(
                                   *_cv(schema_name, name)))

            transaction.commit()

        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Failed to delete table '" + name + "'")
            app.logger.exception(e)
            raise e

    def delete_table(self, name, schema_id):
        connection = db.engine.connect()
        transaction = connection.begin()
        try:
            schema_name = 'schema-' + str(schema_id)
            # Delete table
            table_query = 'DROP TABLE {}.{};'.format(*_ci(schema_name, name))
            raw_table_query = 'DROP TABLE IF EXISTS {}.{};'.format(*_ci(schema_name, "_raw_" + name))
            connection.execute(table_query)
            connection.execute(raw_table_query)

            # Delete metadata
            metadata_query = 'DELETE FROM metadata WHERE id_table = {};'.format(_cv(name))
            connection.execute(metadata_query)

            # Delete history
            history_query = 'DELETE FROM HISTORY WHERE id_dataset={} AND id_table={};'.format(*_cv(schema_name, name))

            # Delete backups
            backups = self.get_backups(schema_id, name)
            for backup in backups:
                self.delete_backup(schema_id, name, backup)

            connection.execute(history_query)

            transaction.commit()
        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Failed to delete table '" + name + "'")
            app.logger.exception(e)
            raise e

    def copy_table(self, name, schema_id, copy_name):
        """ Copies the content of table 'name' to a new table 'copy_name' in the same schema"""
        schema_name = 'schema-' + str(schema_id)
        try:
            db.engine.execute(
                'CREATE TABLE {0}.{1} AS SELECT * FROM {0}.{2}'.format(_ci(schema_name), _ci(copy_name), _ci(name)))
        except Exception as e:
            app.logger.error("[ERROR] Unable to create copy of table {}".format(name))
            app.logger.exception(e)
            raise e

    def delete_row(self, schema_id, table_name, row_ids, add_history=True):
        schema_name = 'schema-' + str(schema_id)
        try:
            for row_id in row_ids:

                column_tuple = self.get_column_names(schema_id, table_name)
                value_tuple = db.engine.execute(
                    'SELECT * FROM {}.{} WHERE id={};'.format(*_ci(schema_name, table_name),
                                                              _cv(row_id))).fetchone()[1:]

                db.engine.execute('DELETE FROM {}.{} WHERE id={};'.format(*_ci(schema_name, table_name), _cv(row_id)))
                # Log action to history
                values_query = 'DEFAULT'

                for value in value_tuple:
                    values_query += ', '

                    if value is None:
                        values_query += 'NULL'
                    else:
                        values_query += _cv(value)
                if add_history:
                    inverse_query = 'INSERT INTO {}.{}({}) VALUES ({});'.format(*_ci(schema_name, table_name),
                                                                                ', '.join(
                                                                                    _ci(column_name) for column_name in
                                                                                    column_tuple),
                                                                                values_query)
                    history.log_action(schema_id, table_name, datetime.now(), 'Deleted row #' + str(row_id),
                                       inverse_query)
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
            if p_ix == 0:
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
            self.delete_row(schema_id, table_name, to_delete, False)

            column_tuple = self.get_column_names(schema_name, table_name)
            inverse_query = ''
            for row_id in to_delete:
                value_tuple = db.engine.execute(
                    'SELECT * FROM {}.{} WHERE id={};'.format(*_ci(schema_name, table_name),
                                                              _cv(row_id))).fetchone()[1:]
                inverse_query += 'INSERT INTO {}.{}({}) VALUES ({});'.format(*_ci(schema_name, table_name),
                                                                             ', '.join(
                                                                                 _ci(column_name) for column_name in
                                                                                 column_tuple),
                                                                             ', '.join(_cv(value) for value in
                                                                                       value_tuple))
            history.log_action(schema_id, table_name, datetime.now(), 'Deleted rows on predicate', inverse_query)
        except Exception as e:
            app.logger.error('[ERROR] Unable to fetch rows to delete from ' + table_name)
            app.logger.exception(e)

    def delete_column(self, schema_id, table_name, column_name):
        schema_name = 'schema-' + str(schema_id)

        column_type = db.engine.execute(
            "select data_type from information_schema.columns where table_name = '{}' and column_name='{}';".format(
                table_name, column_name)).fetchone()[0]

        # create inverse query
        inverse_query = 'ALTER TABLE {}.{} ADD COLUMN {} {} NULL;'.format(*_ci(schema_name, table_name, column_name),
                                                                          column_type)
        inverse_query += 'UPDATE {}.{} SET {} = CASE id\n'.format(*_ci(schema_name, table_name, column_name))
        idx_list = []
        for idx, val in db.engine.execute(
                'SELECT id, {} FROM {}.{}'.format(*_ci(column_name, schema_name, table_name))):
            idx_list.append(idx)
            inverse_query += ' WHEN {} THEN {}'.format(*_cv(idx, val))
        inverse_query += ' END WHERE id IN ({})\n'.format(', '.join(_cv(idx) for idx in idx_list))
        try:
            db.engine.execute(
                'ALTER TABLE {}.{} DROP COLUMN IF EXISTS {};'.format(*_ci(schema_name, table_name, column_name)))
        except Exception as e:
            app.logger.error("[ERROR] Unable to delete column from table '" + table_name + "'")
            app.logger.exception(e)
            raise e

        # Log action to history
        history.log_action(schema_id, table_name, datetime.now(), 'Deleted column ' + column_name, inverse_query)

    def insert_row(self, table, schema_id, columns, values, add_history=True):
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
        if add_history:
            row_id = db.engine.execute('SELECT MAX(id) FROM {}.{}'.format(*_ci(schemaname, table))).fetchone()[0]
            inverse_query = 'DELETE FROM {}.{} WHERE id={};'.format(*_ci(schemaname, table), _cv(row_id))
            history.log_action(schema_id, table, datetime.now(), 'Added row with values ' + ' '.join(values),
                               inverse_query)

    def insert_column(self, schema_id, table_name, column_name, column_type):
        schema_name = 'schema-' + str(schema_id)
        try:
            db.engine.execute(
                'ALTER TABLE {}.{} ADD IF NOT EXISTS {} {} NULL;'.format(*_ci(schema_name, table_name, column_name),
                                                                         column_type))
        except Exception as e:
            app.logger.error("[ERROR] Unable to insert column into table '{}'".format(table_name))
            app.logger.exception(e)
            raise e

        # Log action to history
        inverse_query = 'ALTER TABLE {}.{} DROP COLUMN IF EXISTS {}'.format(*_ci(schema_name, table_name, column_name))
        history.log_action(schema_id, table_name, datetime.now(), 'Added column with name ' + column_name,
                           inverse_query)

    def rename_column(self, schema_id, table_name, column_name, new_column_name):
        schema_name = 'schema-' + str(schema_id)
        try:
            if not new_column_name or new_column_name.isspace():
                raise Exception("Can't rename column to empty string")
            db.engine.execute(
                'ALTER TABLE {0}.{1} RENAME {2} TO {3}'.format(
                    *_ci(schema_name, table_name, column_name, new_column_name)))
        except Exception as e:
            app.logger.error(
                "[ERROR] Unable to rename column '{0}' to '{1}' in table '{2}'".format(column_name, new_column_name,
                                                                                       table_name))
            app.logger.exception(e)
            raise e

        # Log action to history
        inverse_query = 'ALTER TABLE {}.{} RENAME {} TO {}'.format(*_ci(schema_name, table_name, new_column_name,
                                                                        column_name))
        history.log_action(schema_id, table_name, datetime.now(),
                           'Renamed column {} to {}'.format(column_name, new_column_name, inverse_query))

    def update_column_type(self, schema_id, table_name, column_name, column_type):
        schema_name = 'schema-' + str(schema_id)
        db.engine.execute('ALTER DATABASE userdb SET datestyle TO "ISO, MDY";')
        old_column_type = db.engine.execute(
            "select data_type from information_schema.columns where table_name = '{}' and column_name='{}';".format(
                table_name, column_name)).fetchone()[0]
        try:
            db.engine.execute(
                'ALTER TABLE {0}.{1} ALTER {2} TYPE {3} USING {2}::{3};'.format(
                    *_ci(schema_name, table_name, column_name), column_type))
        except Exception as e:
            app.logger.error("[ERROR] Unable to update column type in table '{}'".format(table_name))
            app.logger.exception(e)
            raise e

        # Log action to history
        inverse_query = 'ALTER TABLE {0}.{1} ALTER {2} TYPE {3} USING {2}::{3};'.format(
            *_ci(schema_name, table_name, column_name), old_column_type)
        history.log_action(schema_id, table_name, datetime.now(),
                           'Updated column ' + column_name + ' to have type ' + column_type, inverse_query)

    # Data uploading handling
    def process_csv(self, file, schema_id, tablename, table_description='Default description', append=False,
                    type_deduction=False):
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

        raw_tablename = '_raw_' + tablename
        schema_name = 'schema-' + str(schema_id)

        with open(file, "r") as csv:
            for line in csv:
                if not append:
                    columns = line.strip().split(',')
                    if not type_deduction:
                        self.create_table(tablename, schema_id, columns, desc=table_description, raw=True)
                    else:
                        # Fancy
                        self.create_table(tablename, schema_id, columns, desc=table_description, metadata_only=True)
                break

        df = pd.read_csv(file)
        for column in df.columns:
            if pd.api.types.is_string_dtype(df[column]):
                df[column] = pd.to_datetime(df[column], errors='ignore')
        df.index.name = 'id'
        df.to_sql(name=tablename, con=db.engine, schema=schema_name, index=type_deduction, if_exists='append')
        df.to_sql(name=raw_tablename, con=db.engine, schema=schema_name, index=type_deduction, if_exists='append')
        if type_deduction:
            create_serial_sequence(schema_name, tablename)

    def process_zip(self, file, schema_id, type_deduction=False):
        """
         This method takes a ZIP archive filled with CSV files, and processes them individually
         The name of the CSV file will be used as table name. If a table with the same name is found
         the data will be appended
        """
        connection = db.engine.connect()
        transaction = connection.begin()
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
                        self.process_csv(csv, schema_id, tablename, type_deduction=type_deduction)
                    else:
                        self.process_csv(csv, schema_id, tablename, True, type_deduction=type_deduction)

                # Clean up temp folder
                shutil.rmtree("../output/temp")
                transaction.commit()

        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Failed to load from .zip archive '" + file + "'")
            app.logger.exception(e)

            # Clean up temp folder
            shutil.rmtree("../output/temp")

            raise e

    def process_dump(self, file, schema_id, table_name, table_description='Default description', ):

        """
         This method takes a SQL dump file and processes the INSERT statements,
         either by creating tables and filling them or by filling pre-existing tables.
         All other statements (DELETE, DROP, ...) won't be executed.
        """
        connection = db.engine.connect()
        transaction = connection.begin()
        try:
            with open(file, 'r') as dump:
                # Read the file as a string, split on ';' and check each statement individually
                for statement in dump.read().strip().split(';'):
                    if len(statement.split()) and statement.split()[0] == 'INSERT':  # Only handle INSERT statements
                        # NOTE: An INSERT statement looks like this:
                        # INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...);

                        tablename = statement.split()[2] or table_name
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
                            self.create_table(tablename, schema_id, columns, True, desc=table_description)
                        for values in values_list:
                            val_dict = dict()
                            for c_ix in range(len(columns)):
                                val_dict[columns[c_ix]] = values[c_ix]
                            self.insert_row(tablename, schema_id, columns, val_dict, False)
            transaction.commit()

        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Failed to load from sql dump")
            app.logger.exception(e)

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

                    # Find active users in this dataset
                    active_users = ActiveUserHandler().active_users_in_dataset_count_excluding_requesting_user(
                        schema_id, user_id)

                    result.append(Dataset(schema_id, row['nickname'], row['metadata'], owner, moderators, active_users))
                except Exception as e:
                    app.logger.warning("[ERROR] Failed to find owner of dataset '" + row['nickname'] + "'")
                    app.logger.exception(e)
                    continue

            return result

        except Exception as e:
            app.logger.error("[ERROR] Failed to fetch available datasets for user '" + user_id + "'.")
            app.logger.exception(e)

    def get_dataset_access(self, schema_id, offset=0, limit='ALL', ordering=None, search=None):
        """
         This method returns a table with the users that have access to this dataset
        """

        try:
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

    def grant_access(self, user_id, schema_id, role='contributor'):

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
            exists = db.engine.execute("SELECT EXISTS(SELECT 1 FROM access WHERE id_user={} AND id_dataset={});".format(
                *_cv(user_id, schema_id)))
            return exists.first()[0]

        except Exception as e:
            app.logger.error("[ERROR] Couldn't find if '" + user_id + "' has access to '" + schema_id + "'")
            app.logger.exception(e)
            raise e

    def get_dataset(self, id, user_id=None):
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

            # Find active users in this dataset
            if user_id:
                active_users = ActiveUserHandler().active_users_in_dataset_count_excluding_requesting_user(id, user_id)
                return Dataset(id, ds['nickname'], ds['metadata'], owner, moderators, active_users)
            return Dataset(id, ds['nickname'], ds['metadata'], owner, moderators)
        except Exception as e:
            app.logger.error("[ERROR] Couldn't fetch data for dataset.")
            app.logger.exception(e)
            raise e

    def get_tables(self, schema_id, user_id):
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
                active_users = ActiveUserHandler().active_users_in_table_count_excluding_requesting_user(schema_id, row[
                    'id_table'], user_id)
                t = Table(row['id_table'], row['metadata'], active_users_count=active_users)
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
                search_query = "WHERE ("
                # Fill in the search for every column except ID
                for col in columns[1:]:
                    search_query += "{}::text LIKE '%%{}%%' OR ".format(_ci(col), search)
                search_query = search_query[:-3] + ")"

            rows = db.engine.execute(
                'SELECT * FROM {}.{} {} {} LIMIT {} OFFSET {};'.format(*_ci(schema_name, table_name), search_query,
                                                                       ordering_query, limit, offset))

            # Get total size (of unfiltered table)
            size_query = 'SELECT count(*) FROM {}.{}'.format(*_ci(schema_name, table_name))
            size_result = db.engine.execute(size_query)
            table_size = [r[0] for r in size_result][0]

            table = Table(table_name, '',
                          columns=self.get_column_names_and_types(schema_id, table_name), total_size=table_size)
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
            if not new_name or new_name.isspace():
                raise Exception("Can't rename table to empty string")
            db.engine.execute('UPDATE dataset SET (metadata, nickname) = ({} , {}) WHERE id={}'.format(
                *_cv(new_desc, new_name, schema_name, )))

        except Exception as e:
            app.logger.error("[ERROR] Couldn't update dataset metadata.")
            app.logger.exception(e)
            raise e

    def update_table_metadata(self, schema_id, old_table_name, new_table_name, new_desc):
        schema_name = "schema-" + str(schema_id)
        try:
            if not new_table_name or new_table_name.isspace():
                raise Exception("Can't rename table to empty string")
            db.engine.execute(
                'UPDATE metadata SET (id_table, metadata) = ({}, {}) WHERE id_dataset={} AND id_table={}'.format(
                    *_cv(new_table_name, new_desc, schema_name, old_table_name)))
            db.engine.execute(
                'UPDATE history SET id_table={} WHERE id_dataset={} and id_table={}'.format(
                    *_cv(new_table_name, schema_name, old_table_name)))
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

    # Statistics
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
            app.logger.error("[ERROR] Unable to calculate most common value for column {}".format(column))
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
            app.logger.error("[ERROR] Unable to calculate most amount of empty elements for column {}".format(column))
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

    # Raw data & backups
    def revert_back_to_raw_data(self, schema_id, table_name):
        schema_name = "schema-" + str(schema_id)
        connection = db.engine.connect()
        transaction = connection.begin()
        try:
            connection.execute('DROP TABLE {}.{};'.format(*_ci(schema_name, table_name)))
            raw_table_name = "_raw_" + table_name
            connection.execute(
                'CREATE TABLE {}.{} AS TABLE {}.{}'.format(*_ci(schema_name, table_name, schema_name, raw_table_name)))
            connection.execute(
                'DELETE FROM HISTORY WHERE ID_DATASET={0} AND ID_TABLE={1} AND ACTION_ID<>(SELECT MIN(ACTION_ID) FROM HISTORY WHERE ID_DATASET={0} AND ID_TABLE={1});'.format(
                    *_cv(schema_name, table_name)))
            transaction.commit()
            create_serial_sequence(schema_name, table_name)
        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Couldn't convert back to raw data")
            app.logger.exception(e)
            raise e

    def backup_available(self, schema_id, table_name):
        """ returns true if the back up limit is not yet reached"""
        return len(self.get_backups(schema_id, table_name)) < BACKUP_LIMIT

    def make_backup(self, schema_id, table_name, note=""):
        """ Makes a backup of the table in its current state, if there's still room.
            Backups are given the name '_<table_name>_backup_<timestamp>'
        """
        schema_name = "schema-" + str(schema_id)
        connection = db.engine.connect()
        transaction = connection.begin()
        try:
            timestamp = datetime.now()
            # Format time to leave out microseconds
            timestamp = timestamp.replace(microsecond=0)
            backup_name = '_{}_backup_{}'.format(table_name, timestamp)

            # First check if there aren't too many backups already
            if len(self.get_backups(schema_id, table_name)) >= BACKUP_LIMIT:
                app.logger.warning("[WARNING] Couldn't make backup; limit reached.")
                raise Exception("Backup limit reached.")

            self.copy_table(table_name, schema_id, backup_name)

            backup_query = 'INSERT INTO Backups VALUES ({}, {}, {}, {}, {})'.format(
                *_cv(schema_name, table_name, backup_name, timestamp, note))

            connection.execute(backup_query)
            transaction.commit()

        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Couldn't make backup of table {}".format(table_name))
            app.logger.exception(e)
            raise e

    def get_backups(self, schema_id, table_name):
        """ Returns list with timestamps (as strings) for available backups for given table.
            (This info is fetched from the 'Backups' table)
        """
        schema_name = "schema-" + str(schema_id)
        try:
            query = 'SELECT timestamp FROM Backups WHERE id_dataset = {} AND table_name = {}'.format(
                *_cv(schema_name, table_name))
            rows = db.engine.execute(query)

            timestamps = [str(ts[0]) for ts in rows]
            return timestamps
        except Exception as e:
            app.logger.error("[ERROR] Couldn't fetch backups for table '{}'".format(table_name))
            app.logger.exception(e)
            raise e

    def restore_backup(self, schema_id, table_name, timestamp):
        """ Restores a backup given a table & a timestamp """
        schema_name = "schema-" + str(schema_id)
        connection = db.engine.connect()
        transaction = connection.begin()
        try:
            connection.execute('DROP TABLE {}.{};'.format(*_ci(schema_name, table_name)))
            backup_name = '_{}_backup_{}'.format(table_name, timestamp)
            connection.execute(
                'CREATE TABLE {}.{} AS TABLE {}.{}'.format(*_ci(schema_name, table_name, schema_name, backup_name)))
            connection.execute(
                "DELETE FROM HISTORY WHERE ID_DATASET={} AND ID_TABLE={} AND DATE>'{}';".format(
                    *_cv(schema_name, table_name), timestamp))
            transaction.commit()
            create_serial_sequence(schema_name, table_name)
        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Couldn't restore backup for table '{}'".format(table_name))
            app.logger.exception(e)
            raise e

    def delete_backup(self, schema_id, table_name, timestamp):
        """ Deletes the approriate backup table from the dataset """
        schema_name = "schema-" + str(schema_id)
        connection = db.engine.connect()
        transaction = connection.begin()
        try:
            backup_name = '_{}_backup_{}'.format(table_name, timestamp)
            query = 'DELETE FROM Backups WHERE id_dataset = {} AND backup_name = {};'.format(
                *_cv(schema_name, backup_name))
            connection.execute(query)
            history.log_action(schema_id, table_name, datetime.now(), "Deleted backup {}.".format(timestamp))

            query = 'DROP TABLE IF EXISTS {}.{};'.format(
                *_ci(schema_name, backup_name))
            connection.execute(query)
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Couldn't delete backup.")
            app.logger.exception(e)
            raise e

    def get_backup_info(self, schema_id, table_name, timestamp):
        """ Returns the info (the note) for a given backup """
        schema_name = "schema-" + str(schema_id)
        try:
            backup_name = '_{}_backup_{}'.format(table_name, timestamp)
            query = 'SELECT note FROM Backups WHERE id_dataset = {} AND backup_name = {}'.format(
                *_cv(schema_name, backup_name))
            result = db.engine.execute(query)
            note = ""
            for row in result:
                if row[0] is not None:
                    note = row[0]
            return note
        except Exception as e:
            app.logger.error("[ERROR] Couldn't get info for backup.")
            app.logger.exception(e)
            raise e


class TableJoinPair:
    def __init__(self, table1_name, table2_name, table1_column, table2_column, relation_operator):
        self.table1_name = table1_name
        self.table2_name = table2_name
        self.table1_column = table1_column
        self.table2_column = table2_column
        self.relation_operator = relation_operator
        self.swapped = False
        self.table1_count = 0
        self.table2_count = 0

    def set_table1_count(self, table_count):
        self.table1_count = table_count

    def set_table2_count(self, table_count):
        self.table2_count = table_count

    def swap(self):
        self.swapped = not self.swapped

        self.table1_name, self.table2_name = self.table2_name, self.table1_name
        self.table1_column, self.table2_column = self.table2_column, self.table1_column

    def get_table_column(self, table_num):
        if table_num == 'table1':
            return self.table1_column
        if table_num == 'table2':
            return self.table2_column

    def get_new_table_name(self, table_num):
        if table_num == 'table1':
            table_name = self.table1_name
            table_count = self.table1_count
        if table_num == 'table2':
            table_name = self.table2_name
            table_count = self.table2_count

        return table_name + str(table_count)

    def get_new_column_name(self, table_num, column_name):
        return self.get_new_table_name(table_num) + '_' + column_name


class TableJoiner:
    def __init__(self, data_loader):
        self.data_loader = data_loader

    def safe_relation_operator(self, relation_operator):
        return relation_operator in ("<", "<=", ">", ">=", "=", "<>")

    def prepare_table_pairs(self, table_pairs):
        """
          This method preprocesses the table pairs for easy joining (sorts and counts table occurences)
        """
        checked_tables = list()
        pair_not_inserted = False
        sorted_pairs = list()

        table_occurence_counter = dict()

        def add_to_table_counter(table_name):
            if table_name not in table_occurence_counter:
                table_occurence_counter[table_name] = 0
            else:
                table_occurence_counter[table_name] += 1

        # Loop over pairs while
        #   there are still pairs to process
        #   1 pair has been processed this loop
        while len(table_pairs) != 0 and not pair_not_inserted:
            # Iterate over pairs left to find one that is possible to join
            for table_pair in table_pairs:
                if not self.safe_relation_operator(table_pair.relation_operator):
                    return None
                elif len(checked_tables) == 0:
                    checked_tables.append(table_pair.table1_name)
                    checked_tables.append(table_pair.table2_name)
                    table_pairs.remove(table_pair)
                    sorted_pairs.append(table_pair)

                    add_to_table_counter(table_pair.table1_name)
                    table_pair.set_table1_count(table_occurence_counter[table_pair.table1_name])
                    add_to_table_counter(table_pair.table2_name)
                    table_pair.set_table2_count(table_occurence_counter[table_pair.table2_name])

                elif table_pair.table1_name in checked_tables or table_pair.table2_name in checked_tables:
                    if table_pair.table1_name not in checked_tables:
                        table_pair.swap()

                    checked_tables.append(table_pair.table1_name)
                    checked_tables.append(table_pair.table2_name)
                    table_pairs.remove(table_pair)
                    sorted_pairs.append(table_pair)

                    add_to_table_counter(table_pair.table2_name)
                    table_pair.set_table2_count(table_occurence_counter[table_pair.table2_name])

                else:
                    return None

        return sorted_pairs

    def table_join_create_selection_query(self, dataset_id, table_pairs):
        # Selection query
        selection_query = 'SELECT'

        # selection of columns in first table in located in 'FROM {}' part of query
        add_comma = False

        for column_name in self.data_loader.get_column_names(dataset_id, table_pairs[0].table1_name):
            if column_name == 'id':
                continue
            new_column_name = table_pairs[0].get_new_column_name('table1', column_name)
            if add_comma:
                selection_query += ','
            else:
                add_comma = True

            selection_query += ' {}.{} AS {}'.format(*_ci(
                table_pairs[0].get_new_table_name('table1'),
                column_name,
                new_column_name))

        # selection of columns joined on the first table
        for table_pair in table_pairs:
            table_name = table_pair.table2_name

            for column_name in self.data_loader.get_column_names(dataset_id, table_name):
                if column_name == 'id':
                    continue
                new_column_name = table_pair.get_new_column_name('table2', column_name)
                selection_query += ', {}.{} as {}'.format(*_ci(
                    table_pair.get_new_table_name('table2'),
                    column_name,
                    new_column_name))

        return selection_query

    def table_join_create_join_query(self, dataset_id, table_name, table_pairs):

        schemaname = "schema-" + str(dataset_id)

        # Join query
        join_query = ' INTO {}.{}'.format(*_ci(schemaname, table_name))
        top_table_name = table_pairs[0].table1_name
        join_query += ' FROM {}.{} AS {}'.format(*_ci(schemaname, top_table_name,
                                                      table_pairs[0].get_new_table_name('table1')))

        for table_pair in table_pairs:
            join_query += ' INNER JOIN {}.{}'.format(*_ci(schemaname,
                                                          table_pair.table2_name))

            # Use table as 'table_nameNUM'
            join_query += ' {}'.format(_ci(table_pair.get_new_table_name('table2')))

            if table_pair.swapped:
                table1 = 'table2'
                table2 = 'table1'
            else:
                table1 = 'table1'
                table2 = 'table2'

            # Join ON 'new_table1_name'.'column' 'operator' 'new_table2_name'.'column'
            join_query += ' ON {}.{}'.format(*_ci(table_pair.get_new_table_name(table1),
                                                  table_pair.get_table_column(table1)))
            join_query += table_pair.relation_operator
            join_query += '{}.{}'.format(*_ci(table_pair.get_new_table_name(table2),
                                              table_pair.get_table_column(table2)))

        join_query += ';'

        return join_query

    def table_join_unique_id_query(self, dataset_id, table_name):
        schema_id = "schema-" + str(dataset_id)

        add_unique_id_query = 'ALTER TABLE {0}.{1} ADD id SERIAL; ALTER TABLE {0}.{1} ADD PRIMARY KEY (id);'.format(
            *_ci(schema_id, table_name))

        return add_unique_id_query

    def reorder_column_query(self, dataset_id, temp_table_name, new_table_name):

        schema_name = "schema-" + str(dataset_id)

        column_names = self.data_loader.get_column_names(dataset_id, temp_table_name)

        new_table_query = 'CREATE TABLE {}.{} AS SELECT \"id\"'.format(*_ci(schema_name, new_table_name))

        for column_name in column_names:
            if column_name != 'id':
                new_table_query += ', {}'.format(_ci(column_name))

        new_table_query += 'FROM {}.{};'.format(*_ci(schema_name, temp_table_name))

        drop_old_table_query = 'DROP TABLE {}.{};'.format(*_ci(schema_name, temp_table_name))

        return new_table_query + drop_old_table_query

    def join_multiple_tables(self, dataset_id, table_name, table_desc, table_pairs):
        """
         This method will attempt to join multiple tables into 1 new table
        """
        table_pairs = self.prepare_table_pairs(table_pairs)

        schema_name = 'schema-' + str(dataset_id)
        temp_joined_table = '_join_table'

        try:
            if self.data_loader.table_exists(table_name, schema_name):
                raise Exception("There already exists a table with the name '" + table_name + "'")
            if table_pairs is None:
                raise Exception("Given tables could not be joined into 1 table")

        except Exception as e:
            raise (e)

        schema_name = "schema-" + str(dataset_id)

        # Join table
        connection = db.engine.connect()
        transaction = connection.begin()
        try:
            selection_query = self.table_join_create_selection_query(dataset_id, table_pairs)
            join_query = self.table_join_create_join_query(dataset_id, '_join_table', table_pairs)

            query = selection_query + join_query
            connection.execute(query)
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Failed to join tables into table '" + table_name + "'")
            app.log_exception(e)
            raise e

        # Unique id
        try:
            query = 'ALTER TABLE {0}.{1} ADD id SERIAL; ALTER TABLE {0}.{1} ADD PRIMARY KEY (id);'.format(
                *_ci(schema_name, temp_joined_table))

            db.engine.execute(query)
        except Exception as e:
            app.logger.error("[ERROR] Failed to add unique id to table '" + table_name + "'")
            app.log_exception(e)
            raise e

        # Reorder columns
        try:
            query = self.reorder_column_query(dataset_id, temp_joined_table, table_name)
            db.engine.execute(query)
        except Exception as e:
            app.logger.error("[ERROR] Failed to reorder columns in table '" + table_name + "'")
            app.log_exception(e)
            raise e

        # Metadata
        try:
            query = 'INSERT INTO metadata VALUES({}, {}, {});'.format(
                *_cv(schema_name, table_name, table_desc))
            db.engine.execute(query)
        except Exception as e:
            app.logger.error("[ERROR] Failed to add metadata for table '" + table_name + "'")
            app.log_exception(e)
            raise e

        # Raw data
        transaction = connection.begin()
        try:
            query = 'SELECT * INTO {0}.{1} FROM {0}.{2};'.format(*_ci(schema_name, '_raw_' + table_name, table_name))
            connection.execute(query)
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Failed to create raw data for table '" + table_name + "'")
            app.log_exception(e)
            raise e

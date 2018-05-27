from datetime import datetime
from statistics import median

import pandas as pd
import recordlinkage
from recordlinkage.preprocessing import clean

from app import app, database as db
from app.data_transform.helpers import create_serial_sequence
from app.data_service.models import DataLoader, Table
from app.history.models import History


def _ci(*args: str):
    if len(args) == 1:
        return '"{}"'.format(str(args[0]).replace('"', '""'))
    return ['"{}"'.format(str(arg).replace('"', '""')) for arg in args]


def _cv(*args: str):
    if len(args) == 1:
        return "'{}'".format(str(args[0]).replace("'", "''"))
    return ["'{}'".format(str(arg).replace("'", "''")) for arg in args]


history = History()


class DataTransformer:
    def __init__(self):
        pass

    def impute_missing_data_on_average(self, schema_id, table, column):
        """" impute missing data based on the average"""
        try:
            schema_name = 'schema-' + str(schema_id)
            rows = db.engine.execute('SELECT AVG({}) FROM {}.{};'.format(*_ci(column, schema_name, table)))

            average = rows.first()[0]
            if not average:
                average = 0

            columns = DataLoader().get_column_names_and_types(schema_id, table)
            type = ""

            for column_t in columns:
                if column_t.name == column:
                    type = column_t.type
                    break
            if type == "integer":
                average = int(round(average))

            null_rows = [row['id'] for row in db.engine.execute(
                'SELECT id from {}.{} WHERE {} IS NULL;'.format(*_ci(schema_name, table, column))).fetchall()]

            db.engine.execute('UPDATE {0}.{1} SET {2} = {3} WHERE {2} IS NULL;'.format(*_ci(schema_name, table, column),
                                                                                       _cv(average)))
            inverse_query = 'UPDATE {}.{} SET {} = NULL WHERE id in ({});'.format(*_ci(schema_name, table, column), ', '.join(_cv(row) for row in null_rows))
            history.log_action(schema_id, table, datetime.now(), 'Imputed missing data on average', inverse_query)

        except Exception as e:
            app.logger.error("[ERROR] Unable to impute missing data for column {} by average".format(column))
            app.logger.exception(e)
            raise e

    def impute_missing_data_on_median(self, schema_id, table, column):
        """" impute missing data based on the average"""
        try:
            schema_name = 'schema-' + str(schema_id)

            rows = db.engine.execute('SELECT {} FROM {}.{};'.format(*_ci(column, schema_name, table)))
            values = list()
            for value in rows:
                if value[0] is not None:
                    values.append(value[0])

            if (len(values)) == 0:
                median_val = 0
            else:
                median_val = median(values)

            null_rows = [row['id'] for row in db.engine.execute(
                'SELECT id from {}.{} WHERE {} IS NULL;'.format(*_ci(schema_name, table, column))).fetchall()]
            db.engine.execute('UPDATE {0}.{1} SET {2} = {3} WHERE {2} IS NULL;'.format(*_ci(schema_name, table, column),
                                                                                       _cv(median_val)))
            inverse_query = 'UPDATE {}.{} SET {} = NULL WHERE id in ({});'.format(*_ci(schema_name, table, column), ', '.join(_cv(row) for row in null_rows))
            history.log_action(schema_id, table, datetime.now(), 'Imputed missing data on median', inverse_query)

        except Exception as e:
            app.logger.error("[ERROR] Unable to impute missing data for column {} by median".format(column))
            app.logger.exception(e)
            raise e

    def impute_missing_data_on_value(self, schema_id, table, column, value, function):
        """" impute missing data based on the average"""
        try:
            schema_name = 'schema-' + str(schema_id)

            null_rows = [row['id'] for row in db.engine.execute(
                'SELECT id from {}.{} WHERE {} IS NULL;'.format(*_ci(schema_name, table, column))).fetchall()]
            db.engine.execute('UPDATE {0}.{1} SET {2} = {3} WHERE {2} IS NULL;'.format(*_ci(schema_name, table, column),
                                                                                       _cv(value)))
            inverse_query = 'UPDATE {}.{} SET {} = NULL WHERE id in ({});'.format(*_ci(schema_name, table, column), ', '.join(_cv(row) for row in null_rows))
            history.log_action(schema_id, table, datetime.now(), 'Imputed missing data on ' + function.lower(), inverse_query)

        except Exception as e:
            app.logger.error("[ERROR] Unable to impute missing data for column {}".format(column))
            app.logger.exception(e)
            raise e

    def impute_missing_data(self, schema_id, table, column, function, custom_value=None):
        """"impute missing data based on the average"""
        if function == "AVG":
            return self.impute_missing_data_on_average(schema_id, table, column)
        elif function == "MEDIAN":
            return self.impute_missing_data_on_median(schema_id, table, column)
        elif function == "MCV":
            value = DataLoader().calculate_most_common_value(schema_id, table, column)
            return self.impute_missing_data_on_value(schema_id, table, column, value, "most common value")
        elif function == "CUSTOM":
            return self.impute_missing_data_on_value(schema_id, table, column, custom_value, "custom value")
        else:
            app.logger.error("[ERROR] Unable to impute missing data for column {}".format(column))
            raise Exception

    def find_and_replace(self, schema_id, table, column, to_be_replaced, replacement, replacement_function):
        """" find and replace """
        try:
            schema_name = 'schema-' + str(schema_id)
            query = ""
            updated_rows = list()
            if replacement_function == "substring":
                updated_rows = [row['id'] for row in db.engine.execute('SELECT id FROM {}.{} WHERE {} LIKE {};'.format(
                    *_ci(schema_name, table, column), _cv('%%'+to_be_replaced+'%%'))).fetchall()]
                query = 'UPDATE {0}.{1} SET {2} = REPLACE({2}, {3}, {4});'.format(*_ci(schema_name, table, column),
                                                                                  *_cv(to_be_replaced, replacement))
            elif replacement_function == "full replace":
                updated_rows = [row['id'] for row in db.engine.execute('SELECT id FROM {}.{} WHERE {}={};'.format(
                    *_ci(schema_name, table, column), _cv(to_be_replaced))).fetchall()]
                query = 'UPDATE {0}.{1} SET {2} = {3} WHERE {2} = {4};'.format(*_ci(schema_name, table, column),
                                                                               *_cv(replacement, to_be_replaced))
            else:
                app.logger.error("[ERROR] Unable to perform find and replace")
            db.engine.execute(query)
            if replacement_function == 'substring':
                inverse_query = ''
                for row_id in updated_rows:
                    inverse_query += 'UPDATE {0}.{1} SET {2} = REPLACE({2}, {3}, {4}) WHERE id = {5};'.format(
                            *_ci(schema_name, table, column), *_cv(replacement, to_be_replaced), row_id)
            else:
                inverse_query = ''
                for row_id in updated_rows:
                    inverse_query += 'UPDATE {}.{} SET {} = {} WHERE id = {};'.format(*_ci(schema_name, table, column),
                                                                                      *_cv(to_be_replaced, row_id))
            history.log_action(schema_id, table, datetime.now(), 'Used find and replace', inverse_query)
        except Exception as e:
            app.logger.error("[ERROR] Unable to perform find and replace")
            app.logger.exception(e)
            raise e

    def find_and_replace_by_regex(self, schema_id, table, column, regex, replacement):
        """" find and replace """
        try:
            regex = regex.replace('%', '%%')

            schema_name = 'schema-' + str(schema_id)
            query = 'UPDATE {0}.{1} SET {2} = regexp_replace({2}, {3}, {4});'.format(*_ci(schema_name, table, column),
                                                                                    *_cv(regex, replacement))
            updated_rows = [(row['id'], row[column]) for row in db.engine.execute('SELECT id, {2} FROM {0}.{1} WHERE {2} LIKE {3};'.format(
                *_ci(schema_name, table, column), _cv(regex))).fetchall()]
            inverse_query = 'UPDATE {}.{} SET {} = CASE id '.format(*_ci(schema_name, table, column))
            row_ids = []
            for row_id, original_data in updated_rows:
                row_ids.append(row_id)
                inverse_query += 'WHEN {} THEN {} '.format(*_cv(row_id, original_data))
            inverse_query += 'END WHERE id IN ({});'.format(', '.join(_cv(row_id) for row_id in row_ids))
            db.engine.execute(query)
            history.log_action(schema_id, table, datetime.now(), 'Used find and replace', inverse_query)
        except Exception as e:
            app.logger.error("[ERROR] Unable to perform find and replace by regex")
            app.logger.exception(e)
            raise e


class DateTimeTransformer:
    def __init__(self):
        pass

    def extract_element_from_date(self, schema_id, table, column, element):
        """" Return element of date out of timestamp"""
        try:
            schema_name = 'schema-' + str(schema_id)
            new_column = column + ' (' + element + ')'
            data_loader = DataLoader()
            data_loader.insert_column(schema_id, table, new_column, "double precision", False)
            db.engine.execute(
                'UPDATE {}.{} SET {} = (EXTRACT({} FROM {}::TIMESTAMP));'.format(*_ci(schema_name, table, new_column),
                                                                                  element, _ci(column)))
        except Exception as e:
            app.logger.error("[ERROR] Unable to extract " + element + " from column '{}'".format(column))
            app.logger.exception(e)
            raise e

        # Log action to history
        inverse_query = 'ALTER TABLE {}.{} DROP COLUMN IF EXISTS {};'.format(*_ci(schema_name, table, new_column))
        history.log_action(schema_id, table, datetime.now(), 'Extracted ' + element + ' from column ' + column, inverse_query)

    def extract_date_or_time(self, schema_id, table, column, element):
        """extract date or time from datetime type"""
        try:
            schema_name = 'schema-' + str(schema_id)
            new_column = column + ' (' + element + ')'
            data_loader = DataLoader()
            data_loader.insert_column(schema_id, table, new_column, "varchar(255)", False)
            db.engine.execute(
                'UPDATE {0}.{1} SET {2} = {3}::{4};'.format(*_ci(schema_name, table, new_column, column), element))
        except Exception as e:
            app.logger.error("[ERROR] Unable to extract " + element + " from column '{}'".format(column))
            app.logger.exception(e)
            raise e

        # Log action to history
        inverse_query = 'ALTER TABLE {}.{} DROP COLUMN IF EXISTS {};'.format(*_ci(schema_name, table, new_column))
        history.log_action(schema_id, table, datetime.now(), 'Extracted ' + element + ' from column ' + column, inverse_query)

    def get_transformations(self):
        trans = ["extract day of week", "extract month", "extract year", "extract date", "extract time"]
        return trans

    def transform(self, schema_id, table, column, operation):
        if operation == "extract day of week":
            return self.extract_element_from_date(schema_id, table, column, "DOW")
        elif operation == "extract month":
            return self.extract_element_from_date(schema_id, table, column, "MONTH")
        elif operation == "extract year":
            return self.extract_element_from_date(schema_id, table, column, "YEAR")
        elif operation == "extract date":
            return self.extract_date_or_time(schema_id, table, column, "DATE")
        elif operation == "extract time":
            return self.extract_date_or_time(schema_id, table, column, "TIME")


class NumericalTransformations:
    def __init__(self):
        pass

    def normalize(self, schema_id, table_name, column_name):
        connection = db.engine.connect()
        transaction = connection.begin()
        try:
            schema_name = 'schema-' + str(schema_id)
            df = pd.read_sql_query('SELECT * FROM {}.{};'.format(*_ci(schema_name, table_name)), db.engine)
            new_column_name = column_name + '_norm'

            df[new_column_name] = df[column_name]
            if df[column_name].std(ddof=0):
                df[new_column_name] = (df[column_name] - df[column_name].mean()) / df[column_name].std(ddof=0)

            df.to_sql(name=table_name, con=db.engine, schema=schema_name, if_exists='replace', index=False)
            create_serial_sequence(schema_name, table_name)

            inverse_query = 'ALTER TABLE {}.{} DROP COLUMN IF EXISTS {};'.format(*_ci(schema_name, table_name, new_column_name))
            history.log_action(schema_id, table_name, datetime.now(),
                    'Normalized data of column {}'.format(column_name), inverse_query)
        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Couldn't normalize data")
            app.logger.exception(e)
            raise e

    def equal_width_interval(self, schema_id, table_name, column_name, num_intervals):
        connection = db.engine.connect()
        transaction = connection.begin()
        try:
            schema_name = 'schema-' + str(schema_id)
            df = pd.read_sql_query('SELECT * FROM {}.{};'.format(*_ci(schema_name, table_name)), db.engine)
            new_column_name = column_name + '_intervals_eq_w_' + str(num_intervals)
            df[new_column_name] = pd.cut(df[column_name], num_intervals, precision=9).apply(str)
            df.to_sql(name=table_name, con=db.engine, schema=schema_name, if_exists='replace', index=False)
            create_serial_sequence(schema_name, table_name)
            transaction.commit()

            inverse_query = 'ALTER TABLE {}.{} DROP COLUMN IF EXISTS {};'.format(*_ci(schema_name, table_name, new_column_name))
            history.log_action(schema_id, table_name, datetime.now(),
                    'Generated equal width intervals for data of column {}'.format(column_name), inverse_query)
        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Couldn't process intervals with equal width")
            app.logger.exception(e)
            raise e

    def equal_freq_interval(self, schema_id, table_name, column_name, num_intervals):
        connection = db.engine.connect()
        transaction = connection.begin()
        try:
            schema_name = 'schema-' + str(schema_id)
            df = pd.read_sql_query('SELECT * FROM {}.{};'.format(*_ci(schema_name, table_name)), db.engine)
            new_column_name = column_name + '_intervals_eq_f_' + str(num_intervals)

            sorted_data = list(df[column_name].sort_values())
            data_length = len(df[column_name])
            interval_size = data_length // num_intervals
            intervals_list = []
            for i in range(0, data_length, interval_size):
                intervals_list.append(sorted_data[i] - (sorted_data[i] / 1000))  #
            df[new_column_name] = pd.cut(df[column_name], intervals_list, precision=9).apply(str)

            df.to_sql(name=table_name, con=db.engine, schema=schema_name, if_exists='replace', index=False)
            create_serial_sequence(schema_name, table_name)

            inverse_query = 'ALTER TABLE {}.{} DROP COLUMN IF EXISTS {};'.format(*_ci(schema_name, table_name, new_column_name))
            history.log_action(schema_id, table_name, datetime.now(),
                    'Generated equal frequency intervals for data of column {}'.format(column_name), inverse_query)
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Couldn't process intervals with equal frequency")
            app.logger.exception(e)
            raise e

    def manual_interval(self, schema_id, table_name, column_name, intervals):
        connection = db.engine.connect()
        transaction = connection.begin()
        try:
            schema_name = 'schema-' + str(schema_id)
            df = pd.read_sql_query('SELECT * FROM {}.{};'.format(*_ci(schema_name, table_name)), db.engine)
            new_column_name = column_name + '_intervals_custom'

            df[new_column_name] = pd.cut(df[column_name], intervals).apply(str)

            df.to_sql(name=table_name, con=db.engine, schema=schema_name, if_exists='replace', index=False)
            create_serial_sequence(schema_name, table_name)

            inverse_query = 'ALTER TABLE {}.{} DROP COLUMN IF EXISTS {};'.format(*_ci(schema_name, table_name, new_column_name))
            history.log_action(schema_id, table_name, datetime.now(),
                    'Generated manual intervals for data of column {}'.format(column_name), inverse_query)
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Couldn't process manuel intervals")
            app.logger.exception(e)
            raise e

    def remove_outlier(self, schema_id, table_name, column_name, value, less_than=False):
        connection = db.engine.connect()
        transaction = connection.begin()
        try:
            schema_name = 'schema-' + str(schema_id)
            if less_than:
                outlier_rows = [row for row in db.engine.execute('SELECT * FROM {}.{} WHERE {} < {};'.format(
                    *_ci(schema_name, table_name, column_name), _cv(value))).fetchall()]
                db.engine.execute(
                    'DELETE FROM {}.{} WHERE {} < {};'.format(*_ci(schema_name, table_name, column_name), _cv(value)))
            else:
                outlier_rows = [row for row in db.engine.execute('SELECT * FROM {}.{} WHERE {} > {};'.format(
                    *_ci(schema_name, table_name, column_name), _cv(value))).fetchall()]
                db.engine.execute(
                    'DELETE FROM {}.{} WHERE {} > {};'.format(*_ci(schema_name, table_name, column_name), _cv(value)))

            inverse_query = ''
            for row in outlier_rows:
                inverse_query += 'INSERT INTO {}.{} VALUES ({});'.format(*_ci(schema_name, table_name), ', '.join(_cv(value) for value in row))
            history.log_action(schema_id, table_name, datetime.now(),
                    'Removed outliers from column {}'.format(column_name), inverse_query)
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Couldn't remove outliers from " + column_name)
            app.logger.exception(e)
            raise e

    def chart_data_numerical(self, schema_id, table_name, column_name):
        schema_name = 'schema-' + str(schema_id)
        df = pd.read_sql_query('SELECT * FROM {}.{};'.format(*_ci(schema_name, table_name)), db.engine)

        intervals = pd.cut(df[column_name], 10).value_counts().sort_index()
        data = {
            'labels': list(intervals.index.astype(str)),
            'data': list(intervals.astype(int)),
            'label': '# Items Per Interval',
            'chart': 'bar'
        }
        return data

    def chart_data_categorical(self, schema_id, table_name, column_name):
        schema_name = 'schema-' + str(schema_id)
        df = pd.read_sql_query('SELECT * FROM {}.{};'.format(*_ci(schema_name, table_name)), db.engine)

        intervals = df[column_name].value_counts().sort_index()
        data = {
            'labels': list(intervals.index.astype(str)),
            'data': list(intervals.astype(str)),
            'label': '# Items Per Slice',
            'chart': 'pie'
        }
        return data


class OneHotEncode:
    def __init__(self, dataloader):
        self.dataloader = dataloader

    def encode(self, schema_id, table_name, column_name):

        schema_name = 'schema-' + str(schema_id)
        connection = db.engine.connect()
        transaction = connection.begin()

        is_categorical = False
        column_types = self.dataloader.get_column_names_and_types(schema_id, table_name)
        for column in column_types:
            if column.name == column_name and column.type == 'text':
                is_categorical = True
                break

        if is_categorical:
            try:
                # SELECT * FROM "schema_name"."table";
                data_query = 'SELECT * FROM {}.{};'.format(*_ci(schema_name, table_name))

                df = pd.read_sql(data_query, con=db.engine)
                ohe = pd.get_dummies(df[column_name])
                df = df.join(ohe)
                df.to_sql(name=table_name, con=db.engine, schema=schema_name, if_exists='replace', index=False)
                create_serial_sequence(schema_name, table_name)

                inverse_query = ''

                if len(ohe.columns) == 0:
                    raise Exception("[ERROR] No values found to encode" + column_name + "' in '." + table_name + "',")
                else:
                    for column in ohe.columns:
                        inverse_query += 'ALTER TABLE {}.{} DROP COLUMN IF EXISTS {};'.format(*_ci(schema_name, table_name, column))
                    history.log_action(schema_id, table_name, datetime.now(), 'Applied One Hot Encoding to column {}'.format(column_name), inverse_query)

            except Exception as e:
                transaction.rollback()
                app.logger.error("[ERROR] Couldn't one_hot_encode  '" + column_name + "' in '." + table_name + "',")
                app.logger.exception(e)
                raise e

class DataDeduplicator:
    def __init__(self, dataloader):
        self.dataloader = dataloader

    def remove_identical_rows(self, schema_id, table_name):
        """ remove identical rows from table for all columns, only row with smallest 'id' remains """

        schema_name = 'schema-' + str(schema_id)

        try:
            # Retrieve id's for identical rows
            identical_rows_query = "SELECT t1.id FROM {0}.{1} as t1, {0}.{1} as t2 WHERE t1.id > t2.id".format(
                *_ci(schema_name, table_name))

            for column_name in self.dataloader.get_column_names(schema_id, table_name):
                if column_name == 'id':
                    continue
                identical_rows_query += " AND t1.{0} = t2.{0}".format(_ci(column_name))

            # Delete the retrieved rows on 'id'
            delete_rows_query = "DELETE FROM {}.{} WHERE id IN ({});".format(*_ci(schema_name, table_name),
                                                                             identical_rows_query)

            db.engine.execute(delete_rows_query)
        except Exception as e:
            app.logger.error("[ERROR] Unable to remove identical rows from table '{}'".format(table_name))
            app.logger.exception(e)
            raise e

    # More advanced dedup

    def create_duplicate_table(self, schema_id, table_name, groups):
        """ Creates a table of (row_id, group_id)"""
        schema_name = 'schema-' + str(schema_id)
        dedup_table_name = '_dedup_' + table_name + "_grouped"

        try:
            drop_dedup_query = "DROP TABLE IF EXISTS {}.{} CASCADE;".format(*_ci(schema_name, dedup_table_name))
            db.engine.execute(drop_dedup_query)

            query = 'CREATE TABLE {}.{} ('
            query += '\n\"id\" integer NOT NULL,'
            query += '\n\"group_id\" integer NOT NULL,'
            query += '\n\"delete\" BOOLEAN NOT NULL'
            query += '\n);\n'
            query = query.format(*_ci(schema_name, dedup_table_name))

            for group_id in range(len(groups)):
                for row_id in groups[group_id]:
                    query += 'INSERT INTO {}.{} VALUES ({}, {}, FALSE);'.format(*_ci(schema_name, dedup_table_name), row_id,
                                                                        group_id+1)

            db.engine.execute(query)
        except Exception as e:
            app.logger.error(
                "[ERROR] Unable to create table for duplicate rows from table '{}'".format(dedup_table_name))
            app.logger.exception(e)
            raise e

    def create_duplicate_view(self, schema_id, table_name, group_id):
        """ Creates a view of given table joined with duplicate table """
        schema_name = 'schema-' + str(schema_id)
        dedup_table_name = '_dedup_' + table_name + "_grouped"
        dedup_view_name = '_dedup_' + table_name + "_view"

        try:
            drop_dedup_query = "DROP VIEW IF EXISTS {}.{};".format(*_ci(schema_name, dedup_view_name))
            db.engine.execute(drop_dedup_query)

            query = "CREATE OR REPLACE VIEW {}.{} AS ".format(*_ci(schema_name, dedup_view_name))
            query += "SELECT t1.*, t2.\"group_id\" FROM {0}.{1} as t1, {0}.{2} as t2 WHERE t1.\"id\"=t2.\"id\" AND \"delete\"=False;".format(
                *_ci(schema_name, table_name, dedup_table_name))

            db.engine.execute(query)
        except Exception as e:
            app.logger.error("[ERROR] Could not create view for \'duplicate\' rows for table '{}'".format(table_name))
            app.logger.exception(e)
            raise e

    def collect_identical_rows_alg(self, schema_id, table_name, sorting_key, fixed_column_names, var_column_names, alg):

        schema_name = 'schema-' + str(schema_id)
        dedup_table_name = '_dedup_' + table_name + "_grouped"

        # TODO When user selects rows to remove, collect in table.
        # Afterwards when finished selecting rows of all clusters, delete those rows (UNDO)

        try:

            # Remove complete duplicates before full dedup
            self.remove_identical_rows(schema_id, table_name, )

            # SELECT id, 'column' FROM "schema_name"."table";
            data_query = 'SELECT * FROM {}.{}'.format(*_ci(schema_name, table_name))
            df = pd.read_sql(data_query, con=db.engine)
            df = df.set_index('id')

            # Clean dataset

            ## Remove leading whitespaces
            #df.columns = df.columns.to_series().apply(lambda x: x.strip())

            if sorting_key not in fixed_column_names:
                fixed_column_names.append(sorting_key)

            string_columns = list(df.select_dtypes(include=['object']).columns)
            numerical_columns = list(df.select_dtypes(include=['int64']).columns)
            numerical_columns.extend(list(df.select_dtypes(include=['float64']).columns))
            date_columns = list(df.select_dtypes(include=['datetime64[ns]']).columns)

            ## Clean string values
            for column_name in string_columns:
                df[column_name] = clean(df[column_name])

            # Indexation step
            indexer = recordlinkage.SortedNeighbourhoodIndex(on=sorting_key, window=3)
            pairs = indexer.index(df)

            # Comparison step
            compare_cl = recordlinkage.Compare()

            ## Exact matches
            for column_name in fixed_column_names:
                compare_cl.exact(column_name, column_name, label=column_name)

            ## Variable matches calculated using an alg (levenshtein / numerical / date)
            for column_name in var_column_names:
                if column_name in numerical_columns:
                    compare_cl.numeric(column_name, column_name, method='linear', offset=10, scale=10)
                elif column_name in date_columns:
                    compare_cl.date(column_name, column_name)
                elif column_name in string_columns:
                    compare_cl.string(column_name, column_name, method=alg, threshold=0.75, label=column_name)

            potential_pairs = compare_cl.compute(pairs, df)

            # Classification step
            kmeans = recordlinkage.KMeansClassifier()
            kmeans.learn(potential_pairs)
            matches = kmeans.predict(potential_pairs)

            if len(matches) == 0:
                return False

            # Grouping step
            ## Group matches (A,B), (B,C) into (A,B,C)
            groups = self.group_matches(matches)


            #TODO Create table _dedup_table_groups
            self.create_duplicate_table(schema_id, table_name, groups)

            return True

        except Exception as e:
            app.logger.error(
                "[ERROR] Unable to generate clusters of duplicate rows from table '{}'".format(dedup_table_name))
            app.logger.exception(e)
            raise e

    def group_matches(self, matches):
        """ Group matches (A,B), (B,C), (D,F) into (A,B,C), (D,F) """
        groups = list()
        join_made = False

        for pair in matches:
            if len(groups) == 0:
                groups.append(set(pair))

            else:
                # Go over each group
                    # join with group if not disjoint
                # if not joined with group, create new group
                inserted_into_group = False
                for group in groups:
                    if not group.isdisjoint(set(pair)):
                        group.update(set(pair))
                        inserted_into_group = True
                        join_made = True
                if not inserted_into_group:
                    groups.append(set(pair))

        if join_made:
            return self.group_matches(groups)
        else:
            return groups

    def get_next_group_id(self, schema_id, table_name):
        """ Get top most group_id"""
        schema_name = "schema-" + str(schema_id)
        dedup_table_name = "_dedup_" + table_name + "_grouped"

        try:
            query = "SELECT group_id FROM (SELECT * FROM {}.{} WHERE \"delete\"=FALSE) t1 ORDER BY t1.\"group_id\" ASC;".format(*_ci(schema_name, dedup_table_name))

            result = db.engine.execute(query)
            return result.fetchone()[0]

        except Exception as e:
            app.logger.error(
                "[ERROR] Unable to retrieve group_id from table '{}'".format(dedup_table_name))
            app.logger.exception(e)
            raise e

    def get_amount_of_cluster(self, schema_id, table_name):
        """ Get amount of clusters remaining in _dedup_table_name' """
        schema_name = "schema-" + str(schema_id)
        dedup_table_name = "_dedup_" + table_name + "_grouped"

        try:
            query = "SELECT COUNT(\"group_id\") FROM (SELECT * FROM {}.{} WHERE \"delete\"=FALSE) t1 GROUP BY t1.\"group_id\";".format(
                *_ci(schema_name, dedup_table_name))

            result = db.engine.execute(query)

            amount = result.fetchone()

            if amount is None:
                return 0
            else:
                return amount[0]

        except Exception as e:
            app.logger.error("[ERROR] Unable to get amount of clusters from table '{}'".format(dedup_table_name))
            app.logger.exception(e)
            raise e

    def remove_cluster(self, schema_id, table_name, group_id):
        """ Remove the given cluster (group of rows grouped on 'cluster_id' from _dedup_'table_name'_grouped' """
        schema_name = "schema-" + str(schema_id)
        dedup_table_name = "_dedup_" + table_name + "_grouped"

        try:
            query = "DELETE FROM {}.{} WHERE \"group_id\"={} and \"delete\"=FALSE;".format(*_ci(schema_name, dedup_table_name), _cv(group_id))

            db.engine.execute(query)
        except Exception as e:
            app.logger.error("[ERROR] Unable to remove cluster from table '{}'".format(dedup_table_name))
            app.logger.exception(e)
            raise e

    def delete_dedup_table(self, schema_id, table_name):
        """ Remove the given cluster (group of rows grouped on 'cluster_id') from _dedup_'table_name'_grouped """
        schema_name = "schema-" + str(schema_id)
        dedup_table_name = "_dedup_" + table_name + "_grouped"

        try:
            query = "DROP TABLE IF EXISTS {}.{} CASCADE ".format(*_ci(schema_name, dedup_table_name))

            db.engine.execute(query)
        except Exception as e:
            app.logger.error("[ERROR] Unable to delete dedup table '{}'".format(dedup_table_name))
            app.logger.exception(e)
            raise e

    def delete_dedup_view(self, schema_id, table_name):
        """ Remove the given cluster (group of rows grouped on 'cluster_id') from _dedup_'table_name'_grouped """
        schema_name = "schema-" + str(schema_id)
        dedup_view_name = "_dedup_" + table_name + "_view"

        try:
            query = "DROP VIEW IF EXISTS {}.{};".format(*_ci(schema_name, dedup_view_name))

            db.engine.execute(query)
        except Exception as e:
            app.logger.error("[ERROR] Unable to delete dedup view '{}'".format(dedup_view_name))
            app.logger.exception(e)
            raise e

    def add_rows_to_delete(self, schema_id, table_name, row_ids):
        """ Sets 'delete' column on true"""
        schema_name = 'schema-' + str(schema_id)
        dedup_table_grouped = '_dedup_' + table_name + "_grouped"

        try:
            if len(row_ids) != 0:
                query = ''
                for row_id in row_ids:
                    query += "UPDATE {}.{} SET \"delete\"=TRUE  WHERE \"id\"={};".format(
                        *_ci(schema_name, dedup_table_grouped), row_id)

                # Execute them updates
                db.engine.execute(query)
        except Exception as e:
            app.logger.error("[ERROR] Unable mark rows for deletion in '{}'".format(dedup_table_grouped))
            app.logger.exception(e)
            raise e

    def remove_rows_from_table(self, schema_id, table_name):
        """ Delete rows in _dedup_'table_name'_to_delete from the table """
        schema_name = 'schema-' + str(schema_id)
        dedup_table_grouped = '_dedup_' + table_name + "_grouped"

        try:
            # Fetch all rows to be deleted
            rows_to_delete = db.engine.execute('SELECT * FROM {0}.{1} WHERE \"id\" in (SELECT \"id\" FROM {0}.{2} WHERE \"delete\"=TRUE);'.format(
                *_ci(schema_name, table_name, dedup_table_grouped))).fetchall()

            row_ids = list()
            inverse_query = ''

            for row in rows_to_delete:
                row_ids.append(row['id'])
                inverse_query += 'INSERT INTO {}.{} VALUES ({});'.format(*_ci(schema_name, table_name),
                                                                             ', '.join(_cv(value) for value in row))

            history.log_action(schema_id, table_name, datetime.now(), 'Deduplicated table', inverse_query)

            self.dataloader.delete_row(schema_id, table_name, row_ids, False)

        except Exception as e:
            app.logger.error("[ERROR] Could not remove \'duplicate\' rows for table '{}'".format(table_name))
            app.logger.exception(e)
            raise e

    def get_cluster(self, schema_id, table_name, group_id, offset=0, limit='ALL', ordering=None, search=None):
        """ Returns a 'Table' object associated with requested dataset and group_id"""

        dedup_table_view = "_dedup_" + table_name + "_view"
        schema_name = 'schema-' + str(schema_id)

        try:
            self.create_duplicate_view(schema_id, table_name, group_id)
            columns = self.dataloader.get_column_names(schema_id, dedup_table_view)

            # Get all tables from the metadata table in the schema
            ordering_query = ''
            if ordering is not None:
                # ordering tuple is of the form (columns, asc|desc)
                ordering_query = 'ORDER BY {} {}'.format(_ci(ordering[0]), ordering[1])

            search_query = "WHERE ("
            # Search on the given group id
            search_query += "{}::text LIKE '{}' OR ".format(_ci("group_id"), group_id)
            if search is not None and search != '':
                # Fill in the search for every column except ID
                for col in columns[1:-1]:
                    search_query += "{}::text LIKE '%%{}%%' OR ".format(_ci(col), search)
            search_query = search_query[:-3] + ")"
            rows = db.engine.execute(
                'SELECT * FROM {}.{} {} {} LIMIT {} OFFSET {};'.format(*_ci(schema_name, dedup_table_view), search_query,
                                                                       ordering_query, limit, offset))
            table = Table(table_name, '',
                          columns=self.dataloader.get_column_names_and_types(schema_id, dedup_table_view))  # Hack-n-slash
            for row in rows:
                table.rows.append(list(row))

            self.delete_dedup_view(schema_id, table_name)

            return table
        except Exception as e:
            self.delete_dedup_view(search, table_name)
            app.logger.error("[ERROR] Couldn't fetch table for dataset.")
            app.logger.exception(e)
            raise e

    def process_remaining_duplicates(self, schema_id, table_name):
        """ For each duplicate group, marks all for deletion but first entry """

        schema_name = 'schema-' + str(schema_id)
        dedup_table_grouped = '_dedup_' + table_name + "_grouped"


        try:
            while self.get_amount_of_cluster(schema_id, table_name) != 0:
                query = "SELECT id FROM {}.{} WHERE \"group_id\"={}".format(*_ci(schema_name, dedup_table_grouped), self.get_next_group_id(schema_id, table_name))

                rows = db.engine.execute(query)

                ids_to_delete = list()
                for row in rows:
                    ids_to_delete.append(row[0])
                ids_to_delete.pop(0)

                self.add_rows_to_delete(schema_id, table_name, ids_to_delete)
                self.remove_cluster(schema_id, table_name, self.get_next_group_id(schema_id, table_name))

        except Exception as e:
            app.logger.error("[ERROR] Couldn't mark remaining duplicates for deletion.")
            app.logger.exception(e)
            raise e


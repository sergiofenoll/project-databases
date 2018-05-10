from datetime import datetime
from statistics import median

import pandas as pd

from app import app, database as db
from app.data_service.models import DataLoader
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
            rows = db.engine.execute('SELECT AVG({}) FROM {}.{}'.format(*_ci(column, schema_name, table)))

            average = rows.first()[0]
            if not average:
                average = 0

            db.engine.execute('UPDATE {0}.{1} SET {2} = {3} WHERE {2} IS NULL;'.format(*_ci(schema_name, table, column),
                                                                                       _cv(average)))
            history.log_action(schema_id, table, datetime.now(), 'Imputed missing data on average')

        except Exception as e:
            app.logger.error("[ERROR] Unable to impute missing data for column {} by average".format(column))
            app.logger.exception(e)
            raise e

    def impute_missing_data_on_median(self, schema_id, table, column):
        """" impute missing data based on the average"""
        try:
            schema_name = 'schema-' + str(schema_id)

            rows = db.engine.execute('SELECT {} FROM {}.{}'.format(*_ci(column, schema_name, table)))
            values = list()
            for value in rows:
                if value[0] is not None:
                    values.append(value[0])

            if (len(values)) == 0:
                median_val = 0
            else:
                median_val = median(values)

            db.engine.execute('UPDATE {0}.{1} SET {2} = {3} WHERE {2} IS NULL;'.format(*_ci(schema_name, table, column),
                                                                                       _cv(median_val)))
            history.log_action(schema_id, table, datetime.now(), 'Imputed missing data on median')

        except Exception as e:
            app.logger.error("[ERROR] Unable to impute missing data for column {} by median".format(column))
            app.logger.exception(e)
            raise e

    def impute_missing_data(self, schema_id, table, column, function):
        """"impute missing data based on the average"""
        if function == "AVG":
            return self.impute_missing_data_on_average(schema_id, table, column)
        elif function == "MEDIAN":
            return self.impute_missing_data_on_median(schema_id, table, column)
        else:
            app.logger.error("[ERROR] Unable to impute missing data for column {}".format(column))

    def find_and_replace(self, schema_id, table, column, to_be_replaced, replacement, replacement_function):
        """" find and replace """
        try:
            schema_name = 'schema-' + str(schema_id)
            query = ""
            if replacement_function == "substring":
                query = 'UPDATE {0}.{1} SET {2} = REPLACE({2}, {3}, {4})'.format(*_ci(schema_name, table, column),
                                                                                 *_cv(to_be_replaced, replacement))
            elif replacement_function == "full replace":
                query = 'UPDATE {0}.{1} SET {2} = {3} WHERE {2} = {4}'.format(*_ci(schema_name, table, column),
                                                                              *_cv(replacement, to_be_replaced))
            else:
                app.logger.error("[ERROR] Unable to perform find and replace")

            db.engine.execute(query)
            history.log_action(schema_id, table, datetime.now(), 'Used find and replace')
        except Exception as e:
            app.logger.error("[ERROR] Unable to perform find and replace")
            app.logger.exception(e)
            raise e

    def find_and_replace_by_regex(self, schema_id, table, column, regex, replacement):
        """" find and replace """
        try:
            regex = regex.replace('%', '%%')

            schema_name = 'schema-' + str(schema_id)
            query = 'UPDATE {0}.{1} SET {2} = regexp_replace({2}, {3}, {4})'.format(*_ci(schema_name, table, column),
                                                                                    *_cv(regex, replacement))
            db.engine.execute(query)
            history.log_action(schema_id, table, datetime.now(), 'Used find and replace')
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
            data_loader.insert_column(schema_id, table, new_column, "double precision")
            db.engine.execute(
                ' UPDATE {}.{} SET {} = (EXTRACT({} FROM {}::TIMESTAMP));'.format(*_ci(schema_name, table, new_column),
                                                                                  element, _ci(column)))
        except Exception as e:
            app.logger.error("[ERROR] Unable to extract " + element + " from column '{}'".format(column))
            app.logger.exception(e)
            raise e

        # Log action to history
        history.log_action(schema_id, table, datetime.now(), 'Extracted ' + element + ' from column ' + column)

    def extract_date_or_time(self, schema_id, table, column, element):
        """extract date or time from datetime type"""
        try:
            schema_name = 'schema-' + str(schema_id)
            new_column = column + ' (' + element + ')'
            data_loader = DataLoader()
            data_loader.insert_column(schema_id, table, new_column, "varchar(255)")
            db.engine.execute(
                ' UPDATE {0}.{1} SET {2} = {3}::{4};'.format(*_ci(schema_name, table, new_column, column), element))
        except Exception as e:
            app.logger.error("[ERROR] Unable to extract " + element + " from column '{}'".format(column))
            app.logger.exception(e)
            raise e

        # Log action to history
        history.log_action(schema_id, table, datetime.now(), 'Extracted ' + element + ' from column ' + column)

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
            df = pd.read_sql_query('SELECT * FROM "{}"."{}"'.format(schema_name, table_name), db.engine)
            new_column_name = column_name + '_norm'

            df[new_column_name] = df[column_name]
            if df[column_name].std(ddof=0):
                df[new_column_name] = (df[column_name] - df[column_name].mean()) / df[column_name].std(ddof=0)

            db.engine.execute('DROP TABLE "{0}"."{1}"'.format(schema_name, table_name))
            df.to_sql(name=table_name, con=db.engine, schema=schema_name, if_exists='fail', index=False)
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
            df = pd.read_sql_query('SELECT * FROM "{}"."{}"'.format(schema_name, table_name), db.engine)
            new_column_name = column_name + '_intervals_eq_w_' + str(num_intervals)
            df[new_column_name] = pd.cut(df[column_name], num_intervals, precision=9).apply(str)
            db.engine.execute('DROP TABLE "{0}"."{1}"'.format(schema_name, table_name))
            df.to_sql(name=table_name, con=db.engine, schema=schema_name, if_exists='fail', index=False)
            transaction.commit()
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
            df = pd.read_sql_query('SELECT * FROM "{}"."{}"'.format(schema_name, table_name), db.engine)
            new_column_name = column_name + '_intervals_eq_f_' + str(num_intervals)

            sorted_data = list(df[column_name].sort_values())
            data_length = len(df[column_name])
            interval_size = data_length // num_intervals
            intervals_list = []
            for i in range(0, data_length, interval_size):
                intervals_list.append(sorted_data[i] - (sorted_data[i] / 1000))  #
            df[new_column_name] = pd.cut(df[column_name], intervals_list, precision=9).apply(str)

            db.engine.execute('DROP TABLE "{0}"."{1}"'.format(schema_name, table_name))
            df.to_sql(name=table_name, con=db.engine, schema=schema_name, if_exists='fail', index=False)
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
            df = pd.read_sql_query('SELECT * FROM "{}"."{}"'.format(schema_name, table_name), db.engine)
            new_column_name = column_name + '_intervals_custom'

            df[new_column_name] = pd.cut(df[column_name], intervals).apply(str)

            db.engine.execute('DROP TABLE "{0}"."{1}"'.format(schema_name, table_name))
            df.to_sql(name=table_name, con=db.engine, schema=schema_name, if_exists='fail', index=False)

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
                db.engine.execute(
                    'DELETE FROM "{}"."{}" WHERE "{}" < {}'.format(schema_name, table_name, column_name, value))
            else:
                db.engine.execute(
                    'DELETE FROM "{}"."{}" WHERE "{}" > {}'.format(schema_name, table_name, column_name, value))
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            app.logger.error("[ERROR] Couldn't remove outliers from " + column_name)
            app.logger.exception(e)
            raise e

    def chart_data_numerical(self, schema_id, table_name, column_name):
        schema_name = 'schema-' + str(schema_id)
        df = pd.read_sql_query('SELECT * FROM "{}"."{}"'.format(schema_name, table_name), db.engine)

        intervals = pd.cut(df[column_name], 10).value_counts()
        data = {
            'labels': list(intervals.index.astype(str)),
            'data': list(intervals.astype(int)),
            'label': '# Items Per Interval',
            'chart': 'bar'
        }
        return data

    def chart_data_categorical(self, schema_id, table_name, column_name):
        schema_name = 'schema-' + str(schema_id)
        df = pd.read_sql_query('SELECT * FROM "{}"."{}"'.format(schema_name, table_name), db.engine)

        intervals = df[column_name].value_counts()
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
                # SELECT id, 'column' FROM "schema_name"."table";
                data_query = 'SELECT * FROM {}.{}'.format(*_ci(schema_name, table_name))

                df = pd.read_sql(data_query, con=db.engine)
                ohe = pd.get_dummies(df[column_name])
                df = df.join(ohe)
                df.to_sql(table_name, con=db.engine, schema=schema_name, if_exists='replace', index=False)

            except Exception as e:
                transaction.rollback()
                app.logger.error("[ERROR] Couldn't one_hot_encode  '" + column_name + "' in '." + table_name + "',")
                app.logger.exception(e)
                raise e


class DataDeduplicator:
    def __init__(self, dataloader):
        self.dataloader = dataloader

    def remove_identical_rows(self, schema_id, table_name, column_names):
        """ remove identical rows from table for given columns, only row with smallest 'id' remains """

        schema_name = 'schema-' + str(schema_id)

        try:
            # Retrieve id's for identical rows
            identical_rows_query = "SELECT t1.id FROM {0}.{1} as t1, {0}.{1} as t2 WHERE t1.id > t2.id".format(
                *_ci(schema_name, table_name))

            for column_name in column_names:
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

    def collect_identical_rows_on_distance(self, schema_id, table_name, column_names, selected_column, distance):
        """ create view of identical rows from table based on given distance of words in column"""

        schema_name = 'schema-' + str(schema_id)
        dedup_table_name = '_dedup_' + table_name
        try:
            # Retrieve id's and 'column_name' value for identical rows
            '''
            identical_rows_query = "SELECT t1.id, t1.{2}, t2.id, t2.{2} FROM {0}.{1} as t1, {0}.{1} as t2 WHERE t1.id > t2.id".format(
                *_ci(schema_name, table_name, selected_column))
            '''

            identical_rows_query = "SELECT t1.id FROM {0}.{1} as t1, {0}.{1} as t2 WHERE t1.id <> t2.id".format(
                *_ci(schema_name, table_name))

            for column_name in column_names:
                # Skip 'id' and 'column_name'
                if column_name == 'id' or column_name == selected_column:
                    continue
                identical_rows_query += " AND t1.{0} = t2.{0}".format(_ci(column_name))

            # Create a view if 'identical' rows
            create_view_query = "CREATE OR REPLACE VIEW {}.{} AS ".format(*_ci(schema_name, dedup_table_name))
            create_view_query += "SELECT * FROM {}.{} WHERE id IN ({});".format(*_ci(schema_name, table_name),
                                                                                identical_rows_query)

            db.engine.execute(create_view_query)
        except Exception as e:
            app.logger.error("[ERROR] Could not create view for \'identical\' rows for table '{}'".format(table_name))
            app.logger.exception(e)
            raise e

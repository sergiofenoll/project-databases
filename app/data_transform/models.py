from datetime import datetime
from statistics import median
from numpy import array, argmax
from sklearn.preprocessing import LabelEncoder, OneHotEncoder

import pandas as pd

from app import app, database as db
from app.data_service.models import DataLoader
from app.history.models import History

def _ci(*args: str):
    if len(args) == 1:
        return '"{}"'.format(args[0].replace('"', '""'))
    return ['"{}"'.format(arg.replace('"', '""')) for arg in args]


def _cv(*args: str):
    if len(args) == 1:
        return "'{}'".format(args[0].replace("'", "''"))
    return ["'{}'".format(arg.replace("'", "''")) for arg in args]


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
        except Exception as e:
            app.logger.error("[ERROR] Unable to perform find and replace")
            app.logger.exception(e)
            raise e

    def find_and_replace_by_regex(self, schema_id, table, column, regex, replacement):
        """" find and replace """
        try:
            schema_name = 'schema-' + str(schema_id)
            query = 'UPDATE {0}.{1} SET {2} = regexp_replace({2}, {3}, {4})'.format(*_ci(schema_name, table, column),
                                                                                    *_cv(regex, replacement))
            db.engine.execute(query)
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

    def extract_time_or_date(self, schema_id, table, column, element):
        """extract date or time from datetime type"""
        try:
            schema_name = 'schema-' + str(schema_id)
            new_column = column + ' (' + element + ')'
            data_loader = DataLoader()
            data_loader.insert_column(schema_id, table, new_column, element)
            db.engine.execute(
                ' UPDATE {}.{} SET {} = {}::{};'.format(*_ci(schema_name, table, new_column), element))
        except Exception as e:
            app.logger.error("[ERROR] Unable to extract " + element + " from column '{}'".format(column))
            app.logger.exception(e)
            raise e

        # Log action to history
        history.log_action(schema_id, table, datetime.now(), 'Extracted ' + element + ' from column ' + column)

    def get_transformations(self):
        trans = ["extract day of week", "extract month", "extract year", "parse date", "extract time", "extract date"]
        return trans

    def transform(self, schema_id, table, column, operation):
        if operation == "extract day of week":
            return self.extract_element_from_date(schema_id, table, column, "DOW")
        elif operation == "extract month":
            return self.extract_element_from_date(schema_id, table, column, "MONTH")
        elif operation == "extract year":
            return self.extract_element_from_date(schema_id, table, column, "YEAR")
        elif operation == "extract date":
            return self.extract_time_or_date(schema_id, table, column, "DATE")
        elif operation == "extract time":
            return self.extract_time_or_date(schema_id, table, column, "TIME")


class NumericalTransformations:
    def __init__(self):
        pass

    def normalize(self, schema_id, table_name, column_name):
        schema_name = 'schema-' + str(schema_id)
        df = pd.read_sql_query('SELECT * FROM "{}"."{}"'.format(schema_name, table_name), db.engine)
        new_column_name = column_name + '_norm'

        df[new_column_name] = df[column_name]
        if df[column_name].std(ddof=0):
            df[new_column_name] = (df[column_name] - df[column_name].mean()) / df[column_name].std(ddof=0)

        db.engine.execute('DROP TABLE "{0}"."{1}"'.format(schema_name, table_name))
        df.to_sql(name=table_name, con=db.engine, schema=schema_name, if_exists='fail', index=False)

    def equal_width_interval(self, schema_id, table_name, column_name, num_intervals):
        schema_name = 'schema-' + str(schema_id)
        df = pd.read_sql_query('SELECT * FROM "{}"."{}"'.format(schema_name, table_name), db.engine)
        new_column_name = column_name + '_intervals_eq_w_' + str(num_intervals)

        df[new_column_name] = pd.cut(df[column_name], num_intervals).apply(str)

        db.engine.execute('DROP TABLE "{0}"."{1}"'.format(schema_name, table_name))
        df.to_sql(name=table_name, con=db.engine, schema=schema_name, if_exists='fail', index=False)

    def equal_freq_interval(self, schema_id, table_name, column_name, num_intervals):
        schema_name = 'schema-' + str(schema_id)
        df = pd.read_sql_query('SELECT * FROM "{}"."{}"'.format(schema_name, table_name), db.engine)
        new_column_name = column_name + '_intervals_eq_f_' + str(num_intervals)

        sorted_data = list(df[column_name].sort_values())
        data_length = len(df[column_name])
        interval_size = data_length // num_intervals
        intervals_list = []
        for i in range(0, data_length, interval_size):
            intervals_list.append(sorted_data[i])
        df[new_column_name] = pd.cut(df[column_name], intervals_list).apply(str)

        db.engine.execute('DROP TABLE "{0}"."{1}"'.format(schema_name, table_name))
        df.to_sql(name=table_name, con=db.engine, schema=schema_name, if_exists='fail', index=False)

    def manual_interval(self, schema_id, table_name, column_name, intervals):
        schema_name = 'schema-' + str(schema_id)
        df = pd.read_sql_query('SELECT * FROM "{}"."{}"'.format(schema_name, table_name), db.engine)
        new_column_name = column_name + '_intervals_custom'

        df[new_column_name] = pd.cut(df[column_name], intervals).apply(str)

        db.engine.execute('DROP TABLE "{0}"."{1}"'.format(schema_name, table_name))
        df.to_sql(name=table_name, con=db.engine, schema=schema_name, if_exists='fail', index=False)

    def remove_outlier(self, schema_id, table_name, column_name, value, less_than=False):
        schema_name = 'schema-' + str(schema_id)
        if less_than:
            db.engine.execute(
                'DELETE FROM "{}"."{}" WHERE "{}" < {}'.format(schema_name, table_name, column_name, value))
        else:
            db.engine.execute(
                'DELETE FROM "{}"."{}" WHERE "{}" > {}'.format(schema_name, table_name, column_name, value))

    def chart_data_numerical(self, schema_id, table_name, column_name):
        schema_name = 'schema-' + str(schema_id)
        df = pd.read_sql_query('SELECT * FROM "{}"."{}"'.format(schema_name, table_name), db.engine)

        intervals = pd.cut(df[column_name], 10).value_counts()

        data = {
            'labels': list(intervals.index.astype(str)),
            'data': list(intervals.astype(int)),
            'chart': 'bar',
            'label': '# Items Per Interval'
        }

        return data

    def chart_data_categorical(self, schema_id, table_name, column_name):
        schema_name = 'schema-' + str(schema_id)
        df = pd.read_sql_query('SELECT * FROM "{}"."{}"'.format(schema_name, table_name), db.engine)

        data = {'labels': [], 'data': []}
        data['labels'] = list(df[column_name].unique())
        for label in data['labels']:
            data['data'].append(int(df[df[column_name] == label][column_name].count()))
        data['chart'] = 'pie'
        data['label'] = '# Items Per Slice'

        return data

class OneHotEncode:
    def __init__(self, dataloader):
        self.dataloader = dataloader

    def one_hot_encode(self, integer_encoded):
        one_hot_encoder = OneHotEncoder(sparse=False)

        # Reshape 1D array into 2D suitable for one_hot_encoder
        integer_encoded = integer_encoded.reshape(len(integer_encoded), 1)

        one_hot_encoded = one_hot_encoder.fit_transform(integer_encoded)

        return one_hot_encoded



    def encode(self, schema_id, table_name, column_name):
        schema_name = 'schema-' + str(schema_id)
        ohe_table_name = 'ohe_' + table_name + '_' + column_name

        #TODO Check column 'categorical' (string or integer)
        is_categorical = False
        column_types = self.dataloader.get_column_names_and_types(schema_id, table_name)
        for column in column_types:
            if column.name == column_name and column.type == 'text':
                is_categorical = True
                break
        # Exception
        if not is_categorical:
            return

        # SELECT id, 'column' FROM "schema_name"."table";
        data_query = 'SELECT id, {} FROM {}.{}'.format(*_ci(column_name, schema_name, table_name))

        try:
            result = db.engine.execute(data_query)

            id_s = list()
            data = list()
            for row in result:
                id = row[0]
                value = row[1]

                id_s.append(id)
                data.append(value)
        except Exception as e:
            app.logger.error("[ERROR] Couldn't one_hot_encode  '" + column_name + "' in '." + table_name + "',")
            app.logger.exception(e)
            raise e

        # Extract 'column' values into array
        values = array(data)

        # Pass trough LabelEncoder
        # integer encode
        label_encoder = LabelEncoder()
        integer_encoded = label_encoder.fit_transform(values)

        # Pass through OneHotEncoder
        one_hot_encoded = self.one_hot_encode(integer_encoded)

        # Get labels from LabelEncoder
        labels = label_encoder.classes_
        
        # If table already exists, remove it
        if self.dataloader.table_exists(ohe_table_name, schema_name):
            self.dataloader.delete_table(ohe_table_name, schema_name)

        # Create OHE_table
        self.dataloader.create_table(ohe_table_name, schema_id, labels)
        # For each id, insert encoded row into table
        for _row in range(len(id_s)):
            ohe_row_values = dict()
            ohe_row_values['id'] = str(id_s[_row])

            for _label in range(len(labels)):
                ohe_row_values[labels[_label]] = str(int(one_hot_encoded[_row][_label]))

            self.dataloader.insert_row(ohe_table_name, schema_id, ohe_row_values, ohe_row_values)
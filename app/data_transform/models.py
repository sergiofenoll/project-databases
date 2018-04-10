from app import app
from psycopg2 import sql
from app.data_service.models import DataLoader


class DateTimeTransformer:
    def __init__(self, dbconnection):
        self.dbconnect = dbconnection

    def extract_element_from_date(self, schema_id, table, column, element):
        """" Return element of date out of timestamp"""
        try:
            schema_name = 'schema-' + str(schema_id)
            new_column = column + ' (' + element + ')'
            cursor = self.dbconnect.get_cursor()
            data_loader = DataLoader(self.dbconnect)
            data_loader.insert_column(schema_id, table, new_column, "double precision")
            query = cursor.mogrify(
                sql.SQL(' UPDATE {}.{} SET {} = (EXTRACT(' + element + ' FROM {}::timestamp));').format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table),
                    sql.Identifier(new_column),
                    sql.Identifier(column)))
            cursor.execute(query)
        except Exception as e:
            app.logger.error("[ERROR] Unable to extract " + element +" from column '{}'".format(column))
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e


    def extract_time_or_date(self, schema_id, table, column, element):
        """extract date or time from datetime type"""
        try:

            schema_name = 'schema-' + str(schema_id)
            new_column = column + ' (' + element + ')'
            cursor = self.dbconnect.get_cursor()
            data_loader = DataLoader(self.dbconnect)
            data_loader.insert_column(schema_id, table, new_column, element)
            query = cursor.mogrify(
                sql.SQL('UPDATE {}.{} SET {} = {}::' + element.lower() + ';').format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table),
                    sql.Identifier(new_column),
                    sql.Identifier(column)))
            cursor.execute(query)


        except Exception as e:
            app.logger.error("[ERROR] Unable to extract " + element +" from column '{}'".format(column))
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

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


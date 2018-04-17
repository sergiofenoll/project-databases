from app import app


class History:
    def __init__(self, dbconnection):
        self.dbconnect = dbconnection

    def log_action(self, dataset_id, table_name, date, desc):
        cursor = self.dbconnect.get_cursor()
        dataset_name = 'schema-' + str(dataset_id)
        try:
            query = cursor.mogrify('INSERT INTO HISTORY VALUES (%s, %s, %s, %s)',
                                   (dataset_name, table_name, date, desc))
            cursor.execute(query)
            self.dbconnect.commit()
        except Exception as e:
            app.logger.error(
                "[ERROR] Failed to save action with description {} to history of {}.{}".format(desc, dataset_name,
                                                                                               table_name))
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

    def get_actions(self, dataset_id, table_name, offset=0, limit='ALL', ordering=None, search=None):
        cursor = self.dbconnect.get_cursor()
        dataset_name = 'schema-' + str(dataset_id)
        try:
            ordering_query = ''
            if ordering is not None:
                # ordering tuple is of the form (columns, asc|desc)
                ordering_query = 'ORDER BY "{}" {}'.format(*ordering)

            search_query = ''
            if search is not None:
                search_query = "WHERE (id_dataset='{0}' AND id_table='{1}' ) AND (action_desc LIKE '%{2}%')".format(
                    dataset_name, table_name, search)
            else:
                search_query = "WHERE id_dataset='{0}' AND id_table='{1}'".format(dataset_name, table_name)

            query = cursor.mogrify(
                'SELECT DATE, ACTION_DESC FROM HISTORY {} {} LIMIT {} OFFSET {};'.format(search_query, ordering_query,
                                                                                         limit, offset))
            cursor.execute(query)
            self.dbconnect.commit()

            history = [row for row in cursor]
            return history
        except Exception as e:
            app.logger.error(
                "[ERROR] Failed to get actions from history of {}.{}".format(dataset_name, table_name))
            app.logger.exception(e)
            self.dbconnect.rollback()
            raise e

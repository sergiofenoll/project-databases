from app import app, database as db

def _ci(*args: str):
    if len(args) == 1:
        return '"{}"'.format(args[0].replace('"', '""'))
    return ['"{}"'.format(arg.replace('"', '""')) for arg in args]


def _cv(*args: str):
    if len(args) == 1:
        return "'{}'".format(args[0].replace("'", "''"))
    return ["'{}'".format(arg.replace("'", "''")) for arg in args]

class History:
    def __init__(self):
        pass

    def log_action(self, dataset_id, table_name, date, desc):
        dataset_name = 'schema-' + str(dataset_id)
        try:
            db.engine.execute('INSERT INTO HISTORY VALUES (%s, %s, %s, %s)',
                                   (dataset_name, table_name, date, desc))
        except Exception as e:
            app.logger.error(
                "[ERROR] Failed to save action with description {} to history of {}.{}".format(desc, dataset_name,
                                                                                               table_name))
            app.logger.exception(e)
            raise e

    def get_actions(self, dataset_id, table_name, offset=0, limit='ALL', ordering=None, search=None):
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

            rows = db.engine.execute(
                'SELECT DATE, ACTION_DESC FROM HISTORY {} {} LIMIT {} OFFSET {};'.format(search_query, ordering_query,
                                                                                         limit, offset))

            history = [list(row) for row in rows]
            return history
        except Exception as e:
            app.logger.error(
                "[ERROR] Failed to get actions from history of {}.{}".format(dataset_name, table_name))
            app.logger.exception(e)
            raise e

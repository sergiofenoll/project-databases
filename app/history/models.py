from app import app, database as db


def _ci(*args: str):
    if len(args) == 1:
        return '"{}"'.format(str(args[0]).replace('"', '""'))
    return ['"{}"'.format(str(arg).replace('"', '""')) for arg in args]


def _cv(*args: str):
    if len(args) == 1:
        return "'{}'".format(str(args[0]).replace("'", "''"))
    return ["'{}'".format(str(arg).replace("'", "''")) for arg in args]


class History:
    def __init__(self):
        pass

    def log_action(self, dataset_id, table_name, date, desc, inverse_query):
        dataset_name = 'schema-' + str(dataset_id)
        try:
            db.engine.execute(
                    "INSERT INTO HISTORY (id_dataset, id_table, date, action_desc, inv_query, undone) VALUES ({}, {}, '{}', {}, {}, FALSE)".format(*_cv(dataset_name, table_name), date, *_cv(desc, inverse_query)))
            if app.config['HISTORY_LIMIT']:
                db.engine.execute(
                        'UPDATE HISTORY SET UNDONE=TRUE, INV_QUERY=NULL ' +
                        'WHERE ACTION_ID=(SELECT MIN(ACTION_ID) FROM HISTORY WHERE ID_DATASET={} AND ID_TABLE={} AND UNDONE=FALSE) '.format(*_cv(dataset_name, table_name)) +
                        'AND (SELECT COUNT(ACTION_ID) FROM HISTORY WHERE ID_DATASET={} AND ID_TABLE={} AND UNDONE=FALSE)>{};'.format(
                            *_cv(dataset_name, table_name, app.config['HISTORY_LIMIT'])))
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
                search_query = "WHERE (id_dataset='{}' AND id_table='{}' ) AND (action_desc LIKE '%%{}%%')".format(
                    dataset_name, table_name, search)
            else:
                search_query = "WHERE id_dataset='{}' AND id_table='{}'".format(dataset_name, table_name)

            rows = db.engine.execute(
                'SELECT DATE, ACTION_DESC, ACTION_ID, UNDONE FROM HISTORY {} {} LIMIT {} OFFSET {};'.format(search_query, ordering_query,
                                                                                         limit, offset))

            id_undoable_action = db.engine.execute(
                    'SELECT MAX(ACTION_ID) FROM HISTORY WHERE ID_DATASET={} AND ID_TABLE={} AND UNDONE=FALSE;'.format(*_cv(dataset_name, table_name))).fetchone()[0]

            history = [
                    [row['date'], row['action_desc'], [row['action_id'], row['undone'], row['action_id'] == id_undoable_action]] for row in rows]
            return history
        except Exception as e:
            app.logger.error(
                "[ERROR] Failed to get actions from history of {}.{}".format(dataset_name, table_name))
            app.logger.exception(e)
            raise e

    def undo_action(self, dataset_id, table_name, action_id):
        dataset_name = 'schema-' + str(dataset_id)
        try:
            inverse_query = db.engine.execute('SELECT INV_QUERY FROM HISTORY WHERE ACTION_ID={} AND UNDONE=FALSE'.format(action_id)).fetchone()[0]
        except Exception as e:
            app.logger.error('[ERROR] Failed to get inverse query from action with id {}'.format(action_id))
            app.logger.exception(e)
            raise e
        try:
            db.engine.execute(inverse_query)
        except Exception as e:
            app.logger.error('[ERROR] Failed to undo action with id {}'.format(action_id))
            app.logger.exception(e)
            raise e
        try:
            db.engine.execute('UPDATE HISTORY SET UNDONE=TRUE WHERE ACTION_ID={}'.format(action_id))
        except Exception as e:
            app.logger.error('[ERROR] Failed to set action with id {} as undone'.format(action_id))
            app.logger.exception(e)
            raise e


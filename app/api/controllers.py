from flask import Blueprint, jsonify, request

from app import data_loader, date_time_transformer, history

api = Blueprint('api', __name__)


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>', methods=['GET'])
def get_table(dataset_id, table_name):
    start = request.args.get('start')
    length = request.args.get('length')
    order_column = int(request.args.get('order[0][column]'))
    order_direction = request.args.get('order[0][dir]')

    ordering = (data_loader.get_column_names(dataset_id, table_name)[order_column], order_direction)
    table = data_loader.get_table(dataset_id, table_name, offset=start, limit=length, ordering=ordering)
    _table = data_loader.get_table(dataset_id, table_name)  # TODO: This shit is dirty
    return jsonify(draw=int(request.args.get('draw')),
                   recordsTotal=len(_table.rows),
                   recordsFiltered=len(_table.rows),
                   data=table.rows)


@api.route('/api/datasets/<int:dataset_id>/share', methods=['GET'])
def get_access_table(dataset_id):
    start = request.args.get('start')
    length = request.args.get('length')
    access_table_columns = ['id_user', 'role']
    ordering = (access_table_columns[int(request.args.get('order[0][column]'))], request.args.get('order[0][dir]'))
    search = request.args.get('search[value]')

    table = data_loader.get_dataset_access(dataset_id, offset=start, limit=length, ordering=ordering, search=search)
    _table = data_loader.get_dataset_access(dataset_id)

    return jsonify(draw=int(request.args.get('draw')),
                   recordsTotal=len(_table.rows),
                   recordsFiltered=len(_table.rows),
                   data=table.rows)


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/history', methods=['GET'])
def get_history(dataset_id, table_name):
    start = request.args.get('start')
    length = request.args.get('length')
    search = request.args.get('search[value]')
    order_column = int(request.args.get('order[0][column]'))
    order_direction = request.args.get('order[0][dir]')
    ordering = (['date', 'action_desc'][order_column], order_direction)

    rows = history.get_actions(dataset_id, table_name, offset=start, limit=length, ordering=ordering, search=search)
    _rows = history.get_actions(dataset_id, table_name)
    print(len(rows))
    print(len(_rows))

    return jsonify(draw=int(request.args.get('draw')),
                   recordsTotal=len(_rows),
                   recordsFiltered=len(_rows),
                   data=rows)


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/rows', methods=['POST'])
def add_row(dataset_id, table_name):
    values = list()
    for key in request.args:
        values.append(request.args.get(key))
    data_loader.insert_row(table_name, dataset_id, data_loader.get_column_names(dataset_id, table_name)[1:], values)
    return jsonify({'success': True}), 200


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/rows', methods=['DELETE'])
def delete_row(dataset_id, table_name):
    row_ids = [key.split('-')[1] for key in request.args]
    data_loader.delete_row(dataset_id, table_name, row_ids)
    return jsonify({'success': True}), 200


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/columns', methods=['POST'])
def add_column(dataset_id, table_name):
    column_name = request.args.get('col-name')
    column_type = request.args.get('col-type')
    data_loader.insert_column(dataset_id, table_name, column_name, column_type)
    return jsonify({'success': True}), 200


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/columns', methods=['PUT'])
def update_column(dataset_id, table_name):
    column_name = request.args.get('col-name')
    column_type = request.args.get('col-type')
    data_loader.update_column_type(dataset_id, table_name, column_name, column_type)
    return jsonify({'success': True}), 200


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/columns', methods=['DELETE'])
def delete_column(dataset_id, table_name):
    column_name = request.args.get('col-name')
    data_loader.delete_column(dataset_id, table_name, column_name)
    return jsonify({'success': True}), 200


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/date-time-transformations', methods=['PUT'])
def transform_date_or_time(dataset_id, table_name):
    column_name = request.args.get('col-name')
    operation_name = request.args.get('operation-name')
    date_time_transformer.transform(dataset_id, table_name, column_name, operation_name)
    return jsonify({'success': True}), 200

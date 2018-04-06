from flask import Blueprint, jsonify, request, render_template, redirect, url_for
from flask_login import login_required, current_user

from app import connection, data_loader, ALLOWED_EXTENSIONS, UPLOAD_FOLDER

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
    print(column_name)
    data_loader.delete_column(dataset_id, table_name, column_name)
    return jsonify({'success': True}), 200

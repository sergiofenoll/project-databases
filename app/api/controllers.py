from flask import Blueprint, jsonify, request, send_from_directory

from app import data_loader, date_time_transformer, data_transformer, numerical_transformer, UPLOAD_FOLDER
from app.history.models import History

api = Blueprint('api', __name__)

_history = History()


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>', methods=['GET'])
def get_table(dataset_id, table_name):
    start = request.args.get('start')
    length = request.args.get('length')
    order_column = int(request.args.get('order[0][column]'))
    order_direction = request.args.get('order[0][dir]')
    ordering = (data_loader.get_column_names(dataset_id, table_name)[order_column], order_direction)
    search = request.args.get('search[value]')

    table = data_loader.get_table(dataset_id, table_name, offset=start, limit=length, ordering=ordering, search=search)
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

    rows = _history.get_actions(dataset_id, table_name, offset=start, limit=length, ordering=ordering, search=search)
    _rows = _history.get_actions(dataset_id, table_name)

    return jsonify(draw=int(request.args.get('draw')),
                   recordsTotal=len(_rows),
                   recordsFiltered=len(_rows),
                   data=rows)


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/rows', methods=['POST'])
def add_row(dataset_id, table_name):
    values = dict()
    columns = list()
    for key in request.args:
        if key.startswith('value-col'):
            col_name = key.split('-')[2]  # Key is of the form "value-col-[name]"
            values[col_name] = request.args.get(key)
            columns.append(col_name)
    data_loader.insert_row(table_name, dataset_id, columns, values)
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


@api.route('/api/datasets/update-dataset-metadata', methods=['PUT'])
def update_dataset_metadata():
    dataset_id = request.args.get('ds-id')
    new_name = request.args.get('ds-name')
    new_desc = request.args.get('ds-desc')
    data_loader.update_dataset_metadata(dataset_id, new_name, new_desc)
    return jsonify({'success': True}), 200


@api.route('/api/datasets/<int:dataset_id>/update-metadata', methods=['PUT'])
def update_table_metadata(dataset_id):
    old_table_name = request.args.get('t-old-name')
    new_table_name = request.args.get('t-name')
    new_desc = request.args.get('t-desc')
    data_loader.update_table_metadata(dataset_id, old_table_name, new_table_name, new_desc)
    return jsonify({'success': True}), 200


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/impute-missing-data', methods=['PUT'])
def impute_missing_data(dataset_id, table_name):
    column_name = request.args.get('col-name')
    function = request.args.get('function')
    data_transformer.impute_missing_data(dataset_id, table_name, column_name, function)
    return jsonify({'success': True}), 200


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/export', methods=['PUT'])
def export_table(dataset_id, table_name):
    # Maybe later we might add other types, but for now this is hardcoded to export as CSV
    filename = table_name + ".csv"
    path = UPLOAD_FOLDER + "/" + filename

    separator = request.args.get('separator')
    quote_char = request.args.get('quote_char')
    empty_char = request.args.get('empty_char')

    data_loader.export_table(path, dataset_id, table_name, separator=separator, quote_char=quote_char,
                             empty_char=empty_char)
    return send_from_directory(UPLOAD_FOLDER, filename, as_attachment=True)


@api.route('/api/download/<string:filename>', methods=['GET'])
def download_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename, as_attachment=True)


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/show-raw-data', methods=['GET'])
def show_raw_data(dataset_id, table_name):
    start = request.args.get('start')
    length = request.args.get('length')
    order_column = int(request.args.get('order[0][column]'))
    order_direction = request.args.get('order[0][dir]')
    raw_table_name = "_raw_" + table_name
    ordering = (data_loader.get_column_names(dataset_id, raw_table_name)[order_column], order_direction)
    table = data_loader.get_table(dataset_id, raw_table_name, offset=start, limit=length, ordering=ordering)
    _table = data_loader.get_table(dataset_id, raw_table_name)
    return jsonify(draw=int(request.args.get('draw')),
                   recordsTotal=len(_table.rows),
                   recordsFiltered=len(_table.rows),
                   data=table.rows)


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/find-and-replace', methods=['PUT'])
def find_and_replace(dataset_id, table_name):
    colomn = request.args.get('col-name')
    replacement_function = request.args.get('replacement-function')

    replacement_value = request.args.get('replacement-value')
    if replacement_function == "regex":
        regex = request.args.get('replacement-regex')
        data_transformer.find_and_replace_by_regex(dataset_id, table_name, colomn, regex, replacement_value)
    else:

        value_to_be_replaced = request.args.get('value-to-be-replaced')
        data_transformer.find_and_replace(dataset_id, table_name, colomn, value_to_be_replaced, replacement_value,
                                          replacement_function)
    return jsonify({'success': True}), 200


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/normalize', methods=['PUT'])
def normalize(dataset_id, table_name):
    column_name = request.args.get('col-name')
    numerical_transformer.normalize(dataset_id, table_name, column_name)
    return jsonify({'success': True}), 200


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/discretize', methods=['PUT'])
def discretize(dataset_id, table_name):
    column_name = request.args.get('col-name')
    discretization = request.args.get('discretization')
    try:
        if discretization == 'eq-width':
            print('porque zo dom iedereen?')
            num_intervals = int(request.args.get('num-intervals'))
            numerical_transformer.equal_width_interval(dataset_id, table_name, column_name, num_intervals)
        elif discretization == 'eq-freq':
            num_intervals = int(request.args.get('num-intervals'))
            numerical_transformer.equal_freq_interval(dataset_id, table_name, column_name, num_intervals)
        elif discretization == 'manual':
            intervals = [int(n) for n in request.args.get('intervals').strip().split(',')]
            numerical_transformer.manual_interval(dataset_id, table_name, column_name, intervals)
        else:
            return jsonify({'success': False, 'message': 'Unknown discretization type.'}), 400
    except ValueError as e:
        return jsonify({'success': False,
                        'message': 'Unable to convert intervals into numerical types. Did you send numbers?'}), 400
    return jsonify({'success': True}), 200


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/outliers', methods=['PUT'])
def outliers(dataset_id, table_name):
    try:
        column_name = request.args.get('col-name')
        option = request.args.get('option') == 'less-than'
        value = float(request.args.get('value'))
        numerical_transformer.remove_outlier(dataset_id, table_name, column_name, value, option)
    except ValueError as e:
        return jsonify({'success': False,
                        'message': 'Unable to convert value into a numerical type. Did you send a number?'}), 400
    return jsonify({'success': True}), 200


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/rename-column', methods=['PUT'])
def rename_column(dataset_id, table_name):
    to_rename = request.args.get('col-name')
    new_name = request.args.get('new-name')
    data_loader.rename_column(dataset_id, table_name, to_rename, new_name)
    return jsonify({'success': True}), 200


@api.route('/api/datasets/<int:dataset_id>/tables/<string:table_name>/chart', methods=['GET'])
def chart(dataset_id, table_name):
    column_name = request.args.get('col-name')
    column_type = request.args.get('col-type')

    # data = numerical_transformer.chart_data_numerical(dataset_id, table_name, column_name)

    if column_type not in ['real', 'double', 'integer', 'timestamp']:
        return jsonify(numerical_transformer.chart_data_categorical(dataset_id, table_name, column_name))
        # data['chart'] = 'bar'
    else:
        return jsonify(numerical_transformer.chart_data_numerical(dataset_id, table_name, column_name))
        # data['chart'] = 'pie'
    # return jsonify(data)

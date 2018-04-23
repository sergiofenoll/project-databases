import os

from flask import Blueprint, request, render_template, redirect, url_for, abort, jsonify
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from app import app, data_loader, table_joiner, date_time_transformer, ALLOWED_EXTENSIONS, UPLOAD_FOLDER
from app.data_service.models import TableJoinPair

data_service = Blueprint('data_service', __name__)


# TODO: Find a better way to do this, e.g. wrap it in models, this is ugly
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@data_service.route('/datasets', methods=['GET'])
@login_required
def get_datasets():
    return render_template('data_service/datasets.html',
                           datasets=data_loader.get_user_datasets(current_user.username))


@data_service.route('/datasets', methods=['POST'])
@login_required
def add_dataset():
    name = request.form.get('ds-name')
    meta = request.form.get('ds-meta')
    owner_id = current_user.username
    data_loader.create_dataset(name, owner_id, meta)
    return render_template('data_service/datasets.html',
                           datasets=data_loader.get_user_datasets(current_user.username))


@data_service.route('/datasets/<int:dataset_id>', methods=['GET'])
def get_dataset(dataset_id):
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)

    dataset = data_loader.get_dataset(dataset_id)
    tables = data_loader.get_tables(dataset_id)

    users_with_access = data_loader.get_dataset_access(dataset_id).rows
    access_permission = current_user.username in dataset.moderators

    current_user.active_schema = dataset_id

    columns = list()
    if len(tables) != 0:
        columns = data_loader.get_column_names(dataset_id, tables[0].name)
        columns.remove('id')

    return render_template('data_service/dataset-view.html', ds=dataset, tables=tables, columns=columns,
                           access_permission=access_permission, users_with_access=users_with_access)


@data_service.route('/datasets/<int:dataset_id>/delete', methods=['POST'])
def delete_dataset(dataset_id):
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)
    schema_name = "schema-" + str(dataset_id)
    data_loader.delete_dataset(schema_name)
    return redirect(url_for('data_service.get_datasets'), code=303)


@data_service.route('/datasets/<int:dataset_id>', methods=['POST'])
def add_table(dataset_id):
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)
    if 'file' not in request.files:
        return get_dataset(dataset_id)
    file = request.files['file']
    # If the user doesn't select a file, the browser
    # submits an empty part without filename
    if file.filename == '':
        return get_dataset(dataset_id)

    # TEMP solution: create UPLOAD_FOLDER if it doesn't exists to prevent 'file not found' error.
    # This should probably be done in some setup function and not every time this method is called
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER)

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        path = os.path.join(UPLOAD_FOLDER, filename)
        try:
            file.save(path)
        except Exception as e:
            app.logger.error("[ERROR] Failed to upload file '" + file.filename + "'")
            app.logger.exception(e)
            file.close()
            os.remove(path)
            return get_dataset(dataset_id)

        current_user.active_schema = dataset_id

        try:
            if filename[-3:] == "zip":
                data_loader.process_zip(path, dataset_id)
            elif filename[-3:] == "csv":
                tablename = filename.split('.csv')[0]
                create_new = not data_loader.table_exists(tablename, dataset_id)
                if create_new:
                    data_loader.process_csv(path, dataset_id, tablename)
                else:
                    data_loader.process_csv(path, dataset_id, True)
            else:
                data_loader.process_dump(path, dataset_id)
        except Exception as e:
            app.logger.error("[ERROR] Failed to process file '" + filename + "'")
            app.logger.exception(e)
            return get_dataset(dataset_id)

        file.close()
        os.remove(path)
    return get_dataset(dataset_id)


@data_service.route('/datasets/<int:dataset_id>/tables/<string:table_name>', methods=['GET'])
def get_table(dataset_id, table_name):
    # TODO: Why is the method called get_table() if it returns a list of rows (a list of pseudo dictionaries/lists)?
    # TODO: In fact, why does get_table() return a list of rows instead of a Table object containing the data?
    # TODO: Why *doesn't* a Table object contain any data?
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)
    table = data_loader.get_table(dataset_id, table_name)
    statistics = data_loader.get_statistics_for_all_columns(dataset_id, table_name, table.columns)
    time_date_transformations = date_time_transformer.get_transformations()

    raw_table_name = "_raw_" + table_name
    raw_table_exists = data_loader.table_exists(raw_table_name, "schema-" + str(dataset_id))
    current_user.active_schema = dataset_id
    return render_template('data_service/table-view.html', table=table,
                           time_date_transformations=time_date_transformations,
                           statistics=statistics, raw_table_exists=raw_table_exists)
  

@data_service.route('/datasets/<int:dataset_id>/tables/<string:table_name>/delete', methods=['POST'])
def delete_table(dataset_id, table_name):
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)
    schema_name = "schema-" + str(dataset_id)
    data_loader.delete_table(table_name, schema_name)
    return redirect(url_for('data_service.get_dataset', dataset_id=dataset_id), code=303)


@data_service.route('/datasets/<int:dataset_id>/share', methods=['POST'])
def grant_dataset_access(dataset_id):
    # TEMP
    username = request.form.get('ds-share-name')
    role = request.form.get('ds-share-role')

    data_loader.grant_access(username, dataset_id, role)

    return redirect(url_for('data_service.get_dataset', dataset_id=dataset_id))


@data_service.route('/datasets/<int:dataset_id>/share/delete', methods=['POST'])
def delete_dataset_access(dataset_id):
    username = request.form.get('ds-delete-user-select')
    data_loader.remove_access(username, dataset_id)

    if username == current_user.username:
        return redirect(url_for('data_service.get_datasets'))
    return redirect(url_for('data_service.get_dataset', dataset_id=dataset_id))


@data_service.route('/datasets/<int:dataset_id>/tables/<string:table_name>/revert-to-raw-data', methods=['PUT'])
def revert_to_raw_data(dataset_id, table_name):
    data_loader.revert_back_to_raw_data(dataset_id, table_name)
    return redirect(url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name), code=303)


@data_service.route('/datasets/<int:dataset_id>/tables/<string:table_name>/show-raw-data', methods=['GET'])
def show_raw_data(dataset_id, table_name):
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)
    raw_table_name = "_raw_" + table_name
    raw_table_exists = data_loader.table_exists(raw_table_name, "schema-" + str(dataset_id))
    if not raw_table_exists:
        return redirect(url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name))

    table = data_loader.get_table(dataset_id, raw_table_name)
    title="Raw data for " + table_name
    return render_template('data_service/raw-table-view.html', table=table, title=title)

@data_service.route('/datasets/<int:dataset_id>/tables/<string:table_name>/remove-rows', methods=['POST'])
def remove_rows_predicate(dataset_id, table_name):

    predicates = list()
    for entry in request.form.keys():
        if entry.startswith('join'):
            p = request.form.getlist(entry)
            predicates.append(p)

    data_loader.delete_row_predicate(dataset_id, table_name, predicates)
  
    return redirect(url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name))

@data_service.route('/datasets/<int:dataset_id>/join-tables/<string:table_name>', methods=['GET'])
@login_required
def get_join_column_names(dataset_id, table_name):
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)

    column_names = data_loader.get_column_names(dataset_id, table_name)
    column_names.remove('id')

    return jsonify(column_names)

@data_service.route('/datasets/<int:dataset_id>/join-tables', methods=['POST'])
@login_required
def join_tables(dataset_id):
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)
    # Extract data from form
    join_pairs = list()

    try:
        name = request.form.get('table-name')
        meta = request.form.get('table-meta')

        f = request.form

        # Iterate over all keys in form starting with join, each join<number> represents a join_pair
        for key in f.keys():
            if key.startswith("join"):
                join_pair_row =  f.getlist(key)
                t1 = join_pair_row[0]
                t2 = join_pair_row[1]
                t1_column = join_pair_row[2]
                t2_column = join_pair_row[4]
                relation_operator = join_pair_row[3]

                join_pair = TableJoinPair(table1_name=t1, table2_name=t2, table1_column=t1_column, table2_column=t2_column, relation_operator=relation_operator)
                join_pairs.append(join_pair)
            else:
                continue

        table_joiner.join_multiple_tables(dataset_id, name, meta, join_pairs)

    except Exception as e:
        return redirect(url_for('data_service.get_dataset', dataset_id=dataset_id))

    return redirect(url_for('data_service.get_dataset', dataset_id=dataset_id))

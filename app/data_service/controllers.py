import os

from flask import Blueprint, request, render_template
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from app import connection, data_loader, ALLOWED_EXTENSIONS, UPLOAD_FOLDER

data_service = Blueprint('data_service', __name__)


# TODO: Find a better way to do this, e.g. wrap it in models, this is ugly
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@data_service.route('/datasets', methods=['GET', 'POST'])
@login_required
def datasets():
    if request.method == 'GET':
        return render_template('data_service/datasets.html', datasets=data_loader.get_user_datasets(current_user.username))
    else:
        name = request.form.get('ds-name')
        meta = request.form.get('ds-meta')
        owner_id = current_user.username
        data_loader.create_dataset(name, meta, owner_id)
        return render_template('data_service/datasets.html', datasets=data_loader.get_user_datasets(current_user.username))


@data_service.route('/datasets/<int:dataset_id>', methods=['GET'])
def get_dataset(dataset_id):
    dataset = data_loader.get_dataset(dataset_id)
    tables = data_loader.get_tables(dataset_id)
    return render_template('data_service/dataset-view.html', ds=dataset, tables=tables)


@data_service.route('/datasets/<int:dataset_id>', methods=['POST'])
def add_dataset(dataset_id):
    if 'file' not in request.files:
        return get_dataset(dataset_id)
    file = request.files['file']
    # If the user doesn't select a file, the browser
    # submits an empty part without filename
    if file.filename == '':
        return get_dataset(dataset_id)
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        path = os.path.join(UPLOAD_FOLDER, filename)
        try:
            file.save(path)
        except Exception as e:
            print("[ERROR] Failed to upload file '" + file.filename + "'")
            print(e)
            file.close()
            os.remove(path)
            return get_dataset(dataset_id)

        current_user.active_schema = "schema-" + str(dataset_id)

        try:
            if filename[-3:] == "zip":
                data_loader.process_zip(path, current_user.active_schema)
            elif filename[-3:] == "csv":
                tablename = filename.split('.csv')[0]
                create_new = not data_loader.table_exists(tablename, dataset_id)
                if create_new:
                    data_loader.process_csv(path, current_user.active_schema, tablename)
                else:
                    data_loader.process_csv(path, current_user.active_schema, True)
            else:
                data_loader.process_dump(path, current_user.active_schema)
        except Exception as e:
            print("[ERROR] Failed to process file '" + filename + "'")
            print(e)
            connection.rollback()
            return get_dataset(dataset_id)

        connection.commit()
        file.close()
        os.remove(path)
    return get_dataset(dataset_id)


@data_service.route('/datasets/<int:dataset_id>', methods=['PUT'])
def update_dataset(dataset_id):
    pass  # TODO: Edit name/descriptions of tables in dataset


@data_service.route('/datasets/<int:dataset_id>', methods=['DELETE'])
def delete_dataset(dataset_id):
    schema_name = "schema-" + str(dataset_id)
    data_loader.delete_dataset(schema_name)
    return datasets()


@data_service.route('/datasets/<int:dataset_id>/<string:table_name>', methods=['GET'])
def get_table(dataset_id, table_name):
    table = data_loader.get_table(dataset_id, table_name)
    columns = data_loader.get_column_names(dataset_id, table_name)

    return render_template('data_service/table-view.html', columns=columns, table=table)


@data_service.route('/datasets/<int:dataset_id>/<string:table_name>', methods=['POST'])
def add_table(dataset_id, table_name):
    pass


@data_service.route('/datasets/<int:dataset_id>/<string:table_name>', methods=['PUT'])
def update_table(dataset_id, table_name):
    pass


@data_service.route('/datasets/<int:dataset_id>/<string:table_name>', methods=['DELETE'])
def delete_table(dataset_id, table_name):
    schema_name = "schema-" + str(dataset_id)
    data_loader.delete_table(schema_name, table_name)
    return datasets()

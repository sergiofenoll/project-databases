from flask import Blueprint, request, render_template, url_for, redirect, abort
from flask_login import login_required, current_user

from app import login, user_data_access, data_loader, ALLOWED_EXTENSIONS
from app.user_service.models import User

data_service = Blueprint('data_service', __name__)


# TODO: Find a better way to do this, e.g. wrap it in models, this is ugly
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@data_service.route('/data_service')
@login_required
def data_service():
    return render_template('data-overview.html', datasets=data_loader.get_user_datasets(current_user.username))


@data_service.route('/data_service/new', methods=['POST'])
@login_required
def create_new_dataset():
    name = request.form.get('ds-name')
    meta = request.form.get('ds-meta')
    owner_id = current_user.username

    data_loader.create_dataset(name, meta, owner_id)

    return render_template('data-overview.html', datasets=data_loader.get_user_datasets(current_user.username))


@data_service.route('/data_service/<int:dataset_id>', methods=['POST'])
def upload_file(dataset_id):
    if 'file' not in request.files:
        return show_dataset(dataset_id)
    file = request.files['file']
    # If the user doesn't select file, the browser
    # submits an empty part without filename
    if file.filename == '':
        return show_dataset(dataset_id)

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        try:
            file.save(path)
        except Exception as e:
            print("[ERROR] Failed to upload file '" + file.filename + "'")
            print(e)
            file.close()
            os.remove(path)
            return show_dataset(dataset_id)

        current_user.active_schema = "schema-" + str(dataset_id)

        try:
            if filename[-3:] == "zip":
                dataloader.process_zip(path, current_user.active_schema)
            elif filename[-3:] == "csv":
                tablename = filename.split('.csv')[0]
                create_new = not dataloader.table_exists(tablename, dataset_id)
                if create_new:
                    dataloader.process_csv(path, current_user.active_schema, tablename)
                else:
                    dataloader.process_csv(path, current_user.active_schema, True)
            else:
                dataloader.process_dump(path, current_user.active_schema)
        except Exception as e:
            print("[ERROR] Failed to process file '" + filename + "'")
            print(e)
            connection.rollback()

        connection.commit()
        file.close()
        os.remove(path)

    return show_dataset(dataset_id)


@data_service.route('/data-service/delete/<int:id>')
@login_required
def delete_dataset(id):
    """
     TEMP: this method is called when the button for removing a dataset is clicked.
           It's probably very insecure but since I don't know what I'm doing, this is my solution.
           Please fix this if you actually know how to make buttons work and stuff.
    """

    schema_id = "schema-" + str(id)
    dataloader.delete_dataset(schema_id)

    return redirect(url_for('data_overview'))


@data_service.route('/data-service/<int:dataset_id>')
def show_dataset(dataset_id):
    dataset = dataloader.get_dataset(dataset_id)
    tables = dataloader.get_tables(dataset_id)

    return render_template('dataset-view.html', ds=dataset, tables=tables)


@data_service.route('/data-service/<int:dataset_id>/<string:table_name>')
def show_table(dataset_id, table_name):
    table = dataloader.get_table(dataset_id, table_name)
    columns = dataloader.get_column_names(dataset_id, table_name)

    return render_template('table-view.html', columns=columns, table=table)

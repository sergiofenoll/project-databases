import os

from flask import Blueprint, request, render_template, redirect, url_for, abort, jsonify, flash
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from app import app, data_loader, table_joiner, date_time_transformer,active_user_handler, data_deduplicator, ALLOWED_EXTENSIONS, UPLOAD_FOLDER

from app.data_service.models import TableJoinPair

data_service = Blueprint('data_service', __name__)


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
    try:
        data_loader.create_dataset(name, owner_id, meta)
    except Exception:
        flash(u"Something went wrong while creating your dataset.", 'danger')
    return render_template('data_service/datasets.html',
                           datasets=data_loader.get_user_datasets(current_user.username))


@data_service.route('/datasets/<int:dataset_id>', methods=['GET'])
def get_dataset(dataset_id):
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)

    dataset = data_loader.get_dataset(dataset_id, current_user.username)
    tables = data_loader.get_tables(dataset_id, current_user.username)

    users_with_access = data_loader.get_dataset_access(dataset_id).rows
    access_permission = current_user.username in dataset.moderators

    current_user.active_schema = dataset_id

    columns = list()

    if len(tables) != 0:
        columns = data_loader.get_column_names(dataset_id, tables[0].name)
    active_user_handler.make_user_active_in_dataset(dataset_id, current_user.username)
    return render_template('data_service/dataset-view.html', ds=dataset, tables=tables, columns=columns,
                           access_permission=access_permission, users_with_access=users_with_access)


@data_service.route('/datasets/<int:dataset_id>/delete', methods=['POST'])
def delete_dataset(dataset_id):
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)
    try:
        data_loader.delete_dataset(dataset_id)
        flash(u"Dataset has been deleted.", 'success')
    except Exception:
        flash(u"Something went wrong while deleting your dataset.", 'danger')
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
            flash(u"Failed to upload file.", 'danger')
            return get_dataset(dataset_id)

        current_user.active_schema = dataset_id

        try:
            type_deduction = (request.form.get('ds-type-deduction') is not None) # Unchecked returns None
            table_name = request.form.get('ds-table-name') or filename.rsplit('.')[0]
            table_desc = request.form.get('ds-table-desc') or 'Default description'
            if table_name.isspace():
                table_name = filename.rsplit('.')[0]
            if filename[-3:] == "zip":
                data_loader.process_zip(path, dataset_id, type_deduction=type_deduction)
            elif filename[-3:] == "csv":
                create_new = not data_loader.table_exists(table_name, dataset_id)
                if create_new:
                    data_loader.process_csv(path, dataset_id, table_name, table_description=table_desc, type_deduction=type_deduction)
                else:
                    data_loader.process_csv(path, dataset_id, table_name, table_description=table_desc, append=True, type_deduction=type_deduction)
            else:
                data_loader.process_dump(path, dataset_id, table_name=table_name, table_description=table_desc)
            flash(u"Data has been imported.", 'success')
        except Exception as e:
            app.logger.error("[ERROR] Failed to process file '" + filename + "'")
            app.logger.exception(e)
            flash(u"Data couldn't be imported.", 'danger')
            return get_dataset(dataset_id)

        file.close()
        os.remove(path)
    return get_dataset(dataset_id)


@data_service.route('/datasets/<int:dataset_id>/tables/<string:table_name>', methods=['GET'])
def get_table(dataset_id, table_name):
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)
    try:
        table = data_loader.get_table(dataset_id, table_name)
        statistics = data_loader.get_statistics_for_all_columns(dataset_id, table_name, table.columns)
        time_date_transformations = date_time_transformer.get_transformations()
        backups = data_loader.get_backups(dataset_id, table_name)

        raw_table_name = "_raw_" + table_name
        raw_table_exists = data_loader.table_exists(raw_table_name,dataset_id)
        current_user.active_schema = dataset_id
        active_user_handler.make_user_active_in_table(dataset_id, table_name, current_user.username)
        return render_template('data_service/table-view.html', table=table,
                               time_date_transformations=time_date_transformations,
                               statistics=statistics, raw_table_exists=raw_table_exists, backups=backups)
    except Exception:
        flash(u"Table couldn't be shown.", 'danger')
        return redirect(url_for('data_service.get_dataset', dataset_id=dataset_id), code=303)


@data_service.route('/datasets/<int:dataset_id>/tables/<string:table_name>/delete', methods=['POST'])
def delete_table(dataset_id, table_name):
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)
    try:
        data_loader.delete_table(table_name, dataset_id)
        active_user_handler.make_user_active_in_dataset(dataset_id, current_user.username)
        flash(u"Table has been removed.", 'success')
    except Exception:
        flash(u"Table couldn't be removed.", 'danger')
    return redirect(url_for('data_service.get_dataset', dataset_id=dataset_id), code=303)


@data_service.route('/datasets/<int:dataset_id>/share', methods=['POST'])
def grant_dataset_access(dataset_id):
    username = request.form.get('ds-share-name')
    role = request.form.get('ds-share-role')
    try:
        data_loader.grant_access(username, dataset_id, role)
        active_user_handler.make_user_active_in_dataset(dataset_id, current_user.username)
        flash(u"Access has been granted.", 'success')
    except Exception:
        flash(u"Access couldn't be granted.", 'danger')

    return redirect(url_for('data_service.get_dataset', dataset_id=dataset_id))


@data_service.route('/datasets/<int:dataset_id>/share/delete', methods=['POST'])
def delete_dataset_access(dataset_id):
    username = request.form.get('ds-delete-user-select')
    try:
        data_loader.remove_access(username, dataset_id)
        active_user_handler.make_user_active_in_dataset(dataset_id, current_user.username)
        flash(u"Access has been revoked.", 'success')
    except Exception:
        flash(u"Access couldn't be revoked.", 'danger')

    if username == current_user.username:
        return redirect(url_for('data_service.get_datasets'))
    return redirect(url_for('data_service.get_dataset', dataset_id=dataset_id))


@data_service.route('/datasets/<int:dataset_id>/tables/<string:table_name>/revert-to-raw-data', methods=['PUT'])
def revert_to_raw_data(dataset_id, table_name):
    try:
        data_loader.revert_back_to_raw_data(dataset_id, table_name)
        active_user_handler.make_user_active_in_table(dataset_id, table_name, current_user.username)
        flash(u"Your data has been reverted to its raw state.", 'success')
    except Exception:
        flash(u"Your data couldn't be reverted to its raw state.", 'danger')
    return redirect(url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name), code=303)


@data_service.route('/datasets/<int:dataset_id>/tables/<string:table_name>/show-raw-data', methods=['GET'])
def show_raw_data(dataset_id, table_name):
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)

    raw_table_name = "_raw_" + table_name
    raw_table_exists = data_loader.table_exists(raw_table_name, dataset_id)
    if not raw_table_exists:
        flash(u"Raw data does not exist.", 'warning')
        return redirect(url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name))
    try:
        active_user_handler.make_user_active_in_table(dataset_id, table_name, current_user.username)
        table = data_loader.get_table(dataset_id, raw_table_name)
        title = "Raw data for " + table_name
        return render_template('data_service/raw-table-view.html', table=table, title=title)
    except Exception:
        flash(u"Raw data couldn't be shown.", 'danger')
        return redirect(url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name), code=303)

@data_service.route('/datasets/<int:dataset_id>/tables/<string:table_name>/remove-rows', methods=['POST'])
def remove_rows_predicate(dataset_id, table_name):
    predicates = list()
    for entry in request.form.keys():
        if entry.startswith('join'):
            p = request.form.getlist(entry)
            predicates.append(p)
    try:
        active_user_handler.make_user_active_in_table(dataset_id, table_name, current_user.username)
        data_loader.delete_row_predicate(dataset_id, table_name, predicates)
        flash(u"Removal of rows by predicate was successful.", 'success')
    except Exception:
        flash(u"Removal of rows by predicate was unsuccessful.", 'warning')

    return redirect(url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name))


@data_service.route('/datasets/<int:dataset_id>/tables/<string:table_name>/show-dedup-data-alg', methods=['GET'])
def show_dedup_data_alg(dataset_id, table_name):
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)
    dedup_table_name = "_dedup_" + table_name + "_grouped"
    dedup_table_exists = data_loader.table_exists(dedup_table_name, dataset_id)
    if not dedup_table_exists:
        flash(u"Duplicate data does not exist.", 'warning')
        return redirect(url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name))
    try:
        group_id = data_deduplicator.get_next_group_id(dataset_id, table_name)
        table = data_deduplicator.get_cluster(dataset_id, table_name, group_id)
        title = "Duplicate data for " + table_name + ": Group " + str(group_id)
        return render_template('data_service/dedup-cluster-view.html', table=table, title=title)
    except Exception:
        flash(u"Duplicate data couldn't be shown.", 'danger')
        return redirect(url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name), code=303)


@data_service.route('/datasets/<int:dataset_id>/tables/<string:table_name>/show-dedup-data-alg/sty', methods=['POST'])
def remove_or_mark_identical_rows_alg_sty(dataset_id, table_name):
    try:
        row_ids = [key.split('-')[1] for key in request.args]

        # Mark given id's as 'to_delete' and remove associated cluster from dedup_table_grouped
        data_deduplicator.add_rows_to_delete(dataset_id, table_name, row_ids)

        if data_deduplicator.get_amount_of_cluster(dataset_id, table_name) == 0:
            # Clean dedup tables from db and remove the selected rows
            data_deduplicator.remove_rows_from_table(dataset_id, table_name)
            data_deduplicator.delete_dedup_table(dataset_id, table_name)
            flash(u"Marked rows have been deleted.", 'success')
            return jsonify({'success': True, 'reload': False, 'redirect': True, 'url': url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name)}), 200
        else:
            flash(u"Rows have been marked for deletion'.", 'success')
            return jsonify({'success': True, 'reload': True, 'redirect': False}), 200

        flash(u"Rows have been marked for deletion'.", 'success')
        return jsonify({'success': True, 'reload': True, 'redirect': False}), 200

    except Exception:
        # Clean dedup tables from db
        data_deduplicator.delete_dedup_table(dataset_id, table_name)
        flash(u"Rows couldn't be marked for deletion.", 'warning')

        return jsonify({'error': True, 'reload': False, 'redirect': True,
                        'url': url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name)}), 400


@data_service.route('/datasets/<int:dataset_id>/tables/<string:table_name>/show-dedup-data-alg/ctu', methods=['POST'])
def remove_or_mark_identical_rows_alg_ctu(dataset_id, table_name):
    try:
        row_ids = [key.split('-')[1] for key in request.args]

        # Mark given id's as 'to_delete' and remove associated cluster from dedup_table_grouped
        data_deduplicator.add_rows_to_delete(dataset_id, table_name, row_ids)
        data_deduplicator.remove_cluster(dataset_id, table_name, data_deduplicator.get_next_group_id(dataset_id, table_name))

        if data_deduplicator.get_amount_of_cluster(dataset_id, table_name) == 0:
            # Clean dedup tables from db and remove the selected rows
            data_deduplicator.remove_rows_from_table(dataset_id, table_name)
            data_deduplicator.delete_dedup_table(dataset_id, table_name)
            flash(u"Marked rows have been deleted.", 'success')
            return jsonify({'success': True, 'reload': False, 'redirect': True, 'url': url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name)}), 200
        else:
            flash(u"Rows have been marked for deletion'.", 'success')
            return jsonify({'success': True, 'reload': True, 'redirect': False}), 200

    except Exception:
        # Clean dedup tables from db
        data_deduplicator.delete_dedup_table(dataset_id, table_name)
        flash(u"Rows couldn't be marked for deletion.", 'warning')

        return jsonify({'error': True, 'reload': False, 'redirect': True,
                        'url': url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name)}), 400


@data_service.route('/datasets/<int:dataset_id>/tables/<string:table_name>/show-dedup-data-alg/exit', methods=['POST'])
def remove_or_mark_identical_rows_alg_exit(dataset_id, table_name):
    try:
        row_ids = [key.split('-')[1] for key in request.args]

        # Mark given id's as 'to_delete' and remove associated cluster from dedup_table_grouped
        data_deduplicator.add_rows_to_delete(dataset_id, table_name, row_ids)
        data_deduplicator.remove_cluster(dataset_id, table_name, data_deduplicator.get_next_group_id(dataset_id, table_name))

        # Clean dedup tables from db and remove the selected rows
        data_deduplicator.remove_rows_from_table(dataset_id, table_name)
        data_deduplicator.delete_dedup_table(dataset_id, table_name)
        flash(u"Marked rows have been deleted.", 'success')
        return jsonify({'success': True, 'reload': False, 'redirect': True, 'url': url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name)}), 200

    except Exception:
        # Clean dedup tables from db
        data_deduplicator.delete_dedup_table(dataset_id, table_name)
        flash(u"Rows couldn't be deleted.", 'warning')

        return jsonify({'error': True, 'reload': False, 'redirect': True,
                        'url': url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name)}), 400


@data_service.route('/datasets/<int:dataset_id>/tables/<string:table_name>/show-dedup-data-alg/exp', methods=['POST'])
def remove_or_mark_identical_rows_alg_exp(dataset_id, table_name):
    try:
        row_ids = [key.split('-')[1] for key in request.args]

        if len(row_ids) != 0:
            # Mark given id's as 'to_delete' and remove associated cluster from dedup_table_grouped
            data_deduplicator.add_rows_to_delete(dataset_id, table_name, row_ids)
            data_deduplicator.remove_cluster(dataset_id, table_name, data_deduplicator.get_next_group_id(dataset_id, table_name))

        data_deduplicator.process_remaining_duplicates(dataset_id, table_name)

        # Clean dedup tables from db and remove the selected rows
        data_deduplicator.remove_rows_from_table(dataset_id, table_name)
        data_deduplicator.delete_dedup_table(dataset_id, table_name)
        flash(u"Marked rows have been deleted.", 'success')
        return jsonify({'success': True, 'reload': False, 'redirect': True, 'url': url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name)}), 200

    except Exception:
        # Clean dedup tables from db
        data_deduplicator.delete_dedup_table(dataset_id, table_name)
        flash(u"Rows couldn't be deleted.", 'warning')

        return jsonify({'error': True, 'reload': False, 'redirect': True,
                        'url': url_for('data_service.get_table', dataset_id=dataset_id, table_name=table_name)}), 400


@data_service.route('/datasets/<int:dataset_id>/join-tables/<string:table_name>', methods=['GET'])
@login_required
def get_join_column_names(dataset_id, table_name):
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)

    column_names = data_loader.get_column_names(dataset_id, table_name)

    return jsonify(column_names)


@data_service.route('/datasets/<int:dataset_id>/join-tables', methods=['POST'])
@login_required
def join_tables(dataset_id):
    if (data_loader.has_access(current_user.username, dataset_id)) is False:
        return abort(403)
    # Extract data from form
    join_pairs = list()

    try:
        active_user_handler.make_user_active_in_dataset(dataset_id, current_user.username)

        name = request.form.get('table-name')
        meta = request.form.get('table-meta')

        f = request.form
        # Iterate over all keys in form starting with join, each join<number> represents a join_pair
        for key in f.keys():
            if key.startswith("join"):
                join_pair_row = f.getlist(key)
                t1 = join_pair_row[0]
                t1_column = join_pair_row[1]
                relation_operator = join_pair_row[2]
                t2 = join_pair_row[3]
                t2_column = join_pair_row[4]

                join_pair = TableJoinPair(table1_name=t1, table2_name=t2, table1_column=t1_column,
                                          table2_column=t2_column, relation_operator=relation_operator)
                join_pairs.append(join_pair)
            else:
                continue
        table_joiner.join_multiple_tables(dataset_id, name, meta, join_pairs)
        flash(u"Join of tables was successful.", 'success')
    except Exception as e:
        flash(u"Join of tables was unsuccessful.", 'danger')
    return redirect(url_for('data_service.get_dataset', dataset_id=dataset_id))

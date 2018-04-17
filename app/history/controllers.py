from flask import Blueprint, request, render_template, redirect, url_for, abort
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from app import app, connection, data_loader, date_time_transformer, ALLOWED_EXTENSIONS, UPLOAD_FOLDER

_history = Blueprint('_history', __name__)


@_history.route('/datasets/<int:dataset_id>/tables/<string:table_name>/history', methods=['GET'])
def get_history(dataset_id, table_name):
    table = data_loader.get_table(dataset_id, table_name)
    return render_template('history/history.html', table=table)

from flask import Blueprint, render_template, redirect, url_for, flash

from app import data_loader
from app.data_service.controllers import data_service

_history = Blueprint('_history', __name__)


@_history.route('/datasets/<int:dataset_id>/tables/<string:table_name>/history', methods=['GET'])
def get_history(dataset_id, table_name):
    try:
        table = data_loader.get_table(dataset_id, table_name)
        return render_template('history/history.html', table=table)
    except Exception:
        return redirect(url_for('data_service.get_dataset', dataset_id=dataset_id), code=303)

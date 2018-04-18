from flask import Blueprint, render_template

from app import data_loader

_history = Blueprint('_history', __name__)


@_history.route('/datasets/<int:dataset_id>/tables/<string:table_name>/history', methods=['GET'])
def get_history(dataset_id, table_name):
    table = data_loader.get_table(dataset_id, table_name)
    return render_template('history/history.html', table=table)

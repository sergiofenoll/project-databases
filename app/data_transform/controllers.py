from flask import Blueprint, jsonify, request, render_template, redirect, url_for
from flask_login import login_required, current_user
from werkzeug.utils import secure_filename

from app import data_loader

data_transform = Blueprint('data_transform', __name__)


@data_transform.route('/datasets/<int:dataset_id>/tables/<string:table_name>/rows', methods=['POST'])
def add_row(dataset_id, table_name):
    pass


@data_transform.route('/datasets/<int:dataset_id>/tables/<string:table_name>/rows/<int:row_id>/delete',
                      methods=['POST'])
def delete_row(dataset_id, table_name, row_id):
    pass


@data_transform.route('/datasets/<int:dataset_id>/tables/<string:table_name>/columns', methods=['POST'])
def add_column(dataset_id, table_name):
    pass


@data_transform.route('/datasets/<int:dataset_id>/tables/<string:table_name>/columns/<string:column_name>/update',
                      methods=['POST'])
def change_column(dataset_id, table_name, column_name):
    pass


@data_transform.route('/datasets/<int:dataset_id>/tables/<string:table_name>/columns/<string:column_name>/delete',
                      methods=['POST'])
def delete_column(dataset_id, table_name, column_name):
    pass

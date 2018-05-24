# For more information on how this project's modules are structured, see:
# https://www.digitalocean.com/community/tutorials/how-to-structure-large-flask-applications#structuring-the-application-directory
#
# If after reading the above and looking at the existing files
# you're still not sure how/where to add new functionality, send Sergio a message

import sys
from flask import Flask
from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy
from config import *

app = Flask(__name__)
'''
if len(sys.argv == 2):
    if sys.argv[1] == 'test':
        app.config.from_object('test_config')  # See: http://flask.pocoo.org/docs/0.12/config/
else:
    app.config.from_object('config')  # See: http://flask.pocoo.org/docs/0.12/config/
'''
app.config.from_object('config')  # See: http://flask.pocoo.org/docs/0.12/config/
database = SQLAlchemy(app)
login = LoginManager(app)
login.init_app(app)

from app.data_service.models import DataLoader, TableJoiner, ActiveUserHandler

from app.user_service.models import UserDataAccess
from app.data_transform.models import DateTimeTransformer, DataTransformer, NumericalTransformations, OneHotEncode, DataDeduplicator

user_data_access = UserDataAccess()
data_loader = DataLoader()
date_time_transformer = DateTimeTransformer()
data_transformer = DataTransformer()
numerical_transformer = NumericalTransformations()
active_user_handler = ActiveUserHandler()

table_joiner = TableJoiner(data_loader)
one_hot_encoder = OneHotEncode(data_loader)
data_deduplicator = DataDeduplicator(data_loader)


@login.user_loader
def load_user(user_id):
    user = None
    try:
        user = user_data_access.get_user(user_id)
    except Exception as e:
        app.logger.error("[ERROR] User is none")
        app.logger.exception(e)
    return user


from app.main.controllers import main
from app.user_service.controllers import user_service
from app.data_service.controllers import data_service
from app.data_transform.controllers import data_transform
from app.history.controllers import _history
from app.api.controllers import api

app.register_blueprint(main)
app.register_blueprint(user_service)
app.register_blueprint(data_service)
app.register_blueprint(data_transform)
app.register_blueprint(_history)
app.register_blueprint(api)

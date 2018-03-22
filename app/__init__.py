# For more information on how this project's modules are structured, see:
# https://www.digitalocean.com/community/tutorials/how-to-structure-large-flask-applications#structuring-the-application-directory
#
# If after reading the above and looking at the existing files
# you're still not sure how/where to add new functionality, send Sergio a message

from flask import Flask
from flask_login import LoginManager

from config import *

app = Flask(__name__)
app.config.from_object('config')  # See: http://flask.pocoo.org/docs/0.12/config/

from app.data_service.models import DataLoader
from app.database_connection.models import DBConnection
from app.user_service.models import UserDataAccess

try:
    connection = DBConnection(dbname=config_data['dbname'], dbuser=config_data['dbuser'], dbpass=config_data['dbpass'],
                              dbhost=config_data['dbhost'])
    user_data_access = UserDataAccess(connection)
    data_loader = DataLoader(connection)
except Exception as e:
    app.logger.error("[ERROR] Failed to establish user connection.")
    app.logger.exception(e)

login = LoginManager(app)
login.init_app(app)


@login.user_loader
def load_user(user_id):
    return user_data_access.get_user(user_id)


from app.main.controllers import main
from app.user_service.controllers import user_service
from app.data_service.controllers import data_service
from app.api.controllers import api

app.register_blueprint(main)
app.register_blueprint(user_service)
app.register_blueprint(data_service)
app.register_blueprint(api)

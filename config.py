import os

config_data = dict()
config_data['app_name'] = 'Project Databases'
config_data['dbname'] = 'userdb'
config_data['dbuser'] = 'dbadmin'
config_data['dbpass'] = 'dbadmin'
config_data['dbhost'] = 'localhost'

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
SECRET_KEY = '*^*(*&)(*)(*afafafaSDD47j\3yX R~X@H!jmM]Lwf/,?KT'
ALLOWED_EXTENSIONS = ['zip', 'csv', 'dump']
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'input')

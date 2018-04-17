import os

config_data = dict()
config_data['app_name'] = 'Project Databases'
config_data['dbname'] = 'userdb'
config_data['dbuser'] = 'dbadmin'
config_data['dbpass'] = 'dbadmin'
config_data['dbhost'] = 'localhost'

# Define production database
# SQLAlchemy URI uses following format:
# dialect+driver://username:password@host:port/database
# Many of the parts in the string are optional.
# If no driver is specified the default one is selected
# (make sure to not include the + in that case)
SQLALCHEMY_DATABASE_URI = 'postgresql://dbadmin:dbadmin@localhost:5432/userdb'
SQLALCHEMY_TRACK_MODIFICATIONS = False
SQLALCHEMY_ECHO = False

# Enable protection agains *Cross-site Request Forgery (CSRF)*
CSRF_ENABLED = True
CSRF_SESSION_KEY = 'NotSoSecret'

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
SECRET_KEY = '*^*(*&)(*)(*afafafaSDD47j\3yX R~X@H!jmM]Lwf/,?KT'
ALLOWED_EXTENSIONS = ['zip', 'csv', 'dump']
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'input')

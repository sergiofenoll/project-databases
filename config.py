import os
import sys

# Define production database
# SQLAlchemy URI uses following format:
# dialect+driver://username:password@host:port/database
# Many of the parts in the string are optional.
# If no driver is specified the default one is selected
# (make sure to not include the + in that case)

if len(sys.argv[1:]) > 0:
    SQLALCHEMY_DATABASE_URI = 'postgresql://dbadmin:dbadmin@localhost:5432/test_userdb'
else:
    SQLALCHEMY_DATABASE_URI = 'postgresql://dbadmin:dbadmin@localhost:5432/userdb'

SQLALCHEMY_TRACK_MODIFICATIONS = False
SQLALCHEMY_ECHO = False

# Enable protection agains *Cross-site Request Forgery (CSRF)*
CSRF_ENABLED = True
CSRF_SESSION_KEY = 'NotSoSecret'

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
SECRET_KEY = '*^*(*&)(*)(*afafafaSDD47j\3yX R~X@H!jmM]Lwf/,?KT'
ALLOWED_EXTENSIONS = ['zip', 'csv', 'dump', 'sql']
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'input')
ACTIVE_USER_TIME_SECONDS = 300
BACKUP_LIMIT = 10

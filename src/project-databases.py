from flask import Flask, render_template, request, session, jsonify
from passlib.hash import sha256_crypt

app = Flask(__name__)

from user_data_access import User, DBConnection, UserDataAccess
from config import config_data

# Mock users
mock_users = {'sff': sha256_crypt.encrypt('password')}

### INITIALIZE SINGLETON SERVICES ###
app = Flask('UserTest')
#app.secret_key = '*^*(*&)(*)(*afafafaSDD47j\3yX R~X@H!jmM]Lwf/,?KT'
app_data = {}
app_data['app_name'] = config_data['app_name']
connection_failed = False

try:
	connection = DBConnection(dbname=config_data['dbname'], dbuser=config_data['dbuser'] ,dbpass=config_data['dbpass'], dbhost=config_data['dbhost'])
	user_data_access = UserDataAccess(connection)
except Exception as e:
	print("[ERROR] Failed to establish user connection.")
	print(e)
	connection_failed = True

# API
@app.route('/login', methods=['POST'])
def send_login_request():
    username = request.form.get('lg-username')
    password = request.form.get('lg-password')

    print('Validating data for user "{0}"'.format(username))

    try:
        retrieved_pass = user_data_access.login_user(username)
        if sha256_crypt.verify(password, retrieved_pass):
            return 'Logged in!'
        else:
            return render_template('login-form.html', failed_login=True)
    except Exception as e:
        print(e)
        return render_template('login-form.html', failed_login=True)


@app.route('/register', methods=['POST'])
def register_user():
    username = request.form.get('lg-username')
    password = sha256_crypt.encrypt(request.form.get('lg-password'))
    fname = request.form.get('lg-fname')
    lname = request.form.get('lg-lname')
    email = request.form.get('lg-email')
    status = request.form.get('lg-status')
    active = True

    user_obj = User(username, password, fname, lname, email, status, active)

    if user_data_access.add_user(user_obj):
        return "Registered"
    return "Not Registered"


# Views
@app.route('/')
def index():
    return "Placeholder landing page!"


@app.route('/login')
def login():
    return render_template('login-form.html', failed_login=False)


@app.route('/register')
def register():
    return render_template('register-form.html')


@app.route('/users', methods=['GET'])
def get_users():
    user_objects = user_data_access.get_users()
    return jsonify([obj.to_dct() for obj in user_objects])


if __name__ == "__main__":
    if not connection_failed:
        app.run()

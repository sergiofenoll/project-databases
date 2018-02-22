from flask import Flask, render_template, request, session, jsonify, redirect, url_for
from flask_login import LoginManager, login_user, login_required, logout_user, current_user
from passlib.hash import sha256_crypt
from user_data_access import User, DBConnection, UserDataAccess
from config import config_data

app = Flask(__name__)

# INITIALIZE SINGLETON SERVICES
app = Flask('UserTest')
app.secret_key = '*^*(*&)(*)(*afafafaSDD47j\3yX R~X@H!jmM]Lwf/,?KT'
app_data = dict()
app_data['app_name'] = config_data['app_name']
login = LoginManager(app)
login.login_view = 'login'
connection_failed = False

try:
    connection = DBConnection(dbname=config_data['dbname'], dbuser=config_data['dbuser'], dbpass=config_data['dbpass'],
                              dbhost=config_data['dbhost'])
    user_data_access = UserDataAccess(connection)
except Exception as e:
    print("[ERROR] Failed to establish user connection.")
    print(e)
    connection_failed = True


# API
@login.user_loader
def load_user(id):
    return user_data_access.get_user(id)


@app.route('/login', methods=['POST'])
def send_login_request():
    username = request.form.get('lg-username')
    password = request.form.get('lg-password')

    print('Validating data for user "{0}"'.format(username))

    try:
        retrieved_pass = user_data_access.login_user(username)
        if sha256_crypt.verify(password, retrieved_pass):
            # Login and validate the user.
            # user should be an instance of your `User` class
            login_user(user_data_access.get_user(username))

            """
            next = flask.request.args.get('next')
            # is_safe_url should check if the url is safe for redirects.
            # See http://flask.pocoo.org/snippets/62/ for an example.
            if not is_safe_url(next):
                return flask.abort(400)
            """
            return redirect(url_for('index'))
        else:
            print("Wrong password.")
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
        # Login and validate the user.
        # user should be an instance of your `User` class
        login_user(user_data_access.get_user(username))
        return redirect(url_for('index'))
    return render_template('register-form.html', failed_login=True)


# Views
@app.route('/')
def landing_page():
    return render_template('landing_page.html')


@app.route('/login')
def login():
    return render_template('login-form.html', failed_login=False)


@app.route('/register')
def register():
    return render_template('register-form.html')


@app.route('/index')
@login_required
def index():
    return render_template('main_page.html')


@app.route('/users', methods=['GET'])
@login_required
def get_users():
    user_objects = user_data_access.get_users()
    return jsonify([obj.to_dct() for obj in user_objects])


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('landing_page'))

@app.route('/user-data')
@login_required
def user_data():
    return render_template('user-data.html')

@app.route('/user-data',  methods=['POST'])
@login_required
def change_user_data():
    fname = request.form.get('lg-fname')
    lname = request.form.get('lg-lname')
    email = request.form.get('lg-email')
    password = request.form.get('lg-password')

    if(password == ''):
        password = current_user.password
    else:
        password = sha256_crypt.encrypt(password)
    print(password)
    user_obj = User(current_user.username, password, fname, lname, email, current_user.status, current_user.active)
    user_data_access.alter_user(user_obj)
    return redirect(url_for('user_data'))

if __name__ == "__main__":
    if not connection_failed:
        app.run()

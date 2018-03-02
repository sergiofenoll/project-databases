from flask import Flask, render_template, request, jsonify, redirect, url_for, abort
from flask_login import LoginManager, login_user, login_required, logout_user, current_user
from passlib.hash import sha256_crypt
from user_data_access import User, DBConnection, UserDataAccess
from config import config_data
from data_loader import DataLoader


# INITIALIZE SINGLETON SERVICES
app = Flask(__name__)
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

    try:
        retrieved_pass = user_data_access.login_user(username)
        if sha256_crypt.verify(password, retrieved_pass):

            # Check if user is inactive
            user = user_data_access.get_user(username)
            if not user.active:
                print("Inactive account")
                return render_template('login-form.html', user_inactive=True)

            # Login and validate the user.
            # user should be an instance of your `User` class
            login_user(user)

            return redirect(url_for('main_page'))
        else:
            return render_template('login-form.html', wrong_password=True)
    except Exception as e:
        print(e)
        return render_template('login-form.html', wrong_password=True)


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
        return redirect(url_for('main_page'))
    return render_template('register-form.html', wrong_password=True)


@app.route('/user-data', methods=['POST'])
@login_required
def change_user_data():
    if not sha256_crypt.verify(request.form.get('lg-current-password'), current_user.password):
        return render_template('user-data.html', wrong_password=True)

    fname = request.form.get('lg-fname')
    lname = request.form.get('lg-lname')
    email = request.form.get('lg-email')
    password = request.form.get('lg-new-password')

    if (password == ''):
        password = current_user.password
    else:
        password = sha256_crypt.encrypt(password)
    user_obj = User(current_user.username, password, fname, lname, email, current_user.status, current_user.active)
    user_data_access.alter_user(user_obj)
    return render_template('user-data.html', data_updated=True)


@app.route('/admin-page', methods=['POST'])
@login_required
def admin_activity_change():
    for user in user_data_access.get_users():
        # Checkbox uses username as it's identifier for Flask
        if request.form.get(user.username) is None:  # If the checkbox is unchecked the response is None
            user.is_active = False
            user.active = False
        else:  # If the checkbox is checked the response is 'on'
            user.is_active = True
            user.active = True
        # I have no clue why they don't just return True or False
        user_data_access.alter_user(user)
    return render_template('admin-page.html', users=user_data_access.get_users(), data_updated=True)


# Views
@app.route('/')
@app.route('/index')
def main_page():
    return render_template('main-page.html')


@app.route('/login')
def login():
    return render_template('login-form.html')


@app.route('/register')
def register():
    return render_template('register-form.html')


@app.route('/users', methods=['GET'])
@login_required
def get_users():
    user_objects = user_data_access.get_users()
    return jsonify([obj.to_dct() for obj in user_objects])


@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('main_page'))


@app.route('/user-data')
@login_required
def user_data():
    return render_template('user-data.html')


@app.route('/admin-page')
@login_required
def admin_page():
    if current_user.status != 'admin':
        return abort(403)
    return render_template('admin-page.html', users=user_data_access.get_users())


@app.route('/data-service')
@login_required
def data_overview():
    return render_template('data-overview.html')

if __name__ == "__main__":
    if not connection_failed:
        app.run()
        '''
        try:
            dl = DataLoader(connection)

            tablename = "tijgers"
            schema = "public"

            if dl.table_exists(tablename, schema):
                dl.delete_table(tablename, schema)
            
            dl.process_csv("../input/tijgers.csv", schema, tablename)
            dl.process_csv("../input/tijgers2.csv", schema, tablename, True)

        except Exception as e:
            print("[ERROR] An error occured during execution.")
            print(e)
        '''

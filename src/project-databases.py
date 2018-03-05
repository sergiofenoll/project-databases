from flask import Flask, render_template, request, jsonify, redirect, url_for, abort
from flask_login import LoginManager, login_user, login_required, logout_user, current_user
from werkzeug.utils import secure_filename
from passlib.hash import sha256_crypt
from user_data_access import User, DBConnection, UserDataAccess
from config import config_data
from data_loader import DataLoader

from Lib import os

# INITIALIZE SINGLETON SERVICES
app = Flask(__name__)
app.secret_key = '*^*(*&)(*)(*afafafaSDD47j\3yX R~X@H!jmM]Lwf/,?KT'
app_data = dict()
app_data['app_name'] = config_data['app_name']
login = LoginManager(app)
login.login_view = 'login'
connection_failed = False

UPLOAD_FOLDER = '../input'
ALLOWED_EXTENSIONS = ['csv', 'zip']
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

try:
    connection = DBConnection(dbname=config_data['dbname'], dbuser=config_data['dbuser'], dbpass=config_data['dbpass'],
                              dbhost=config_data['dbhost'])
    user_data_access = UserDataAccess(connection)
    dataloader = DataLoader(connection)
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
        # I have no clue why they don't just return True or False - Edit by Jona: Fucking webdevelopment
        user_data_access.alter_user(user)
    return render_template('admin-page.html', users=user_data_access.get_users(), data_updated=True)


@app.route('/data-service/new', methods=['POST'])
@login_required
def create_new_dataset():
    name = request.form.get('ds-name')
    meta = request.form.get('ds-meta')
    owner_id = current_user.username

    dataloader.create_dataset(name, meta, owner_id)

    return render_template('data-overview.html', datasets=dataloader.get_user_datasets(current_user.username))

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/data-service/<int:dataset_id>', methods=['POST'])
def upload_file(dataset_id):

    if 'file' not in request.files:
        return show_dataset(dataset_id)
    file = request.files['file']
    # if user does not select file, browser also
    # submit a empty part without filename
    if file.filename == '':
        return show_dataset(dataset_id)

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(path)

        dl = DataLoader(connection)
        current_user.active_schema = "schema-" + str(dataset_id)

        if filename[-3:] == "zip":
            dl.process_zip(path, current_user.active_schema)

        else:
            tablename = filename.split('.csv')[0]
            create_new = not dl.table_exists(tablename, dataset_id)

            if create_new:
                dl.process_csv(path, current_user.active_schema, tablename)
            else:
                dl.process_csv(path, current_user.active_schema, True)

        file.close()
        os.remove(path)




    return show_dataset(dataset_id)


@app.route('/data-service/delete/<int:id>')
@login_required
def delete_dataset(id):
    '''
     TEMP: this method is called when the button for removing a dataset is clicked.
           It's probably very insecure but since I don't know what I'm doing, this is my solution.
           Please fix this if you actually know how to make buttons work and stuff.
    '''

    schema_id = "schema-" + str(id)
    dataloader.delete_dataset(schema_id)

    return redirect(url_for('data_overview'))


@app.route('/data-service/<int:dataset_id>')
def show_dataset(dataset_id):
    dataset = dataloader.get_dataset(dataset_id)
    tables = dataloader.get_tables(dataset_id)

    return render_template('dataset-view.html', ds=dataset, tables=tables)


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
    return render_template('data-overview.html', datasets=dataloader.get_user_datasets(current_user.username))


if __name__ == "__main__":
    if not connection_failed:
        app.run()

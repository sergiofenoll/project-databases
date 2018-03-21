from flask import Blueprint, request, render_template, url_for, redirect, abort
from flask_login import login_required, current_user, login_user, logout_user
from passlib.hash import sha256_crypt

from app import app, login, user_data_access
from app.user_service.models import User

user_service = Blueprint('user_service', __name__)


@user_service.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        return render_template('user_service/login-form.html')
    else:
        username = request.form.get('lg-username')
        password = request.form.get('lg-password')

        try:
            retrieved_pass = user_data_access.login_user(username)
            if sha256_crypt.verify(password, retrieved_pass):

                # Check if user is inactive
                user = user_data_access.get_user(username)
                if not user.is_active:
                    return render_template('user_service/login-form.html', user_inactive=True)

                # Login and validate the user.
                # user should be an instance of your `User` class
                login_user(user)

                return redirect(url_for('main.index'))
            else:
                return render_template('user_service/login-form.html', wrong_password=True)
        except Exception as e:
            app.logger.exception(e)
            return render_template('user_service/login-form.html', wrong_password=True)


@user_service.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'GET':
        return render_template('user_service/register-form.html', wrong_password=False)
    else:
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
            return redirect(url_for('main.index'))
        return render_template('user_service/register-form.html', wrong_password=True)


@user_service.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('main.index'))


@user_service.route('/user_data', methods=['GET', 'POST'])
@login_required
def user_data():
    if request.method == 'GET':
        return render_template('user_service/user-data.html', data_updated=False)
    else:
        if not sha256_crypt.verify(request.form.get('lg-current-password'), current_user.password):
            return render_template('user_service/user-data.html', wrong_password=True)

        fname = request.form.get('lg-fname')
        lname = request.form.get('lg-lname')
        email = request.form.get('lg-email')
        password = request.form.get('lg-new-password')

        if password == '':
            password = login.current_user.password
        else:
            password = sha256_crypt.encrypt(password)
        user_obj = User(login.current_user.username, password, fname, lname, email, login.current_user.status,
                        login.current_user.active)
        user_data_access.alter_user(user_obj)
        return render_template('user_service/user-data.html', data_updated=True)


@user_service.route('/admin_page', methods=['GET', 'POST'])
@login_required
def admin_page():
    if current_user.status != 'admin':
        return abort(403)
    if request.method == 'GET':
        return render_template('user_service/admin-page.html', users=user_data_access.get_users())
    else:
        for user in user_data_access.get_users():
            # Checkbox uses username as it's identifier for Flask
            if request.form.get(user.username) is None:  # If the checkbox is unchecked the response is None
                user.is_active = False
            else:  # If the checkbox is checked the response is 'on'
                user.is_active = True
            # I have no clue why they don't just return True or False
            user_data_access.alter_user(user)
        return render_template('user_service/admin-page.html', users=user_data_access.get_users(), data_updated=True)

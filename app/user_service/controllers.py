from flask import Blueprint, request, render_template, url_for, redirect, abort, flash
from flask_login import login_required, current_user, login_user, logout_user
from passlib.hash import sha256_crypt

from app import app, data_loader, login, user_data_access, active_user_handler
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
                    flash(u"This user is inactive and can't log in.", 'warning')
                    return render_template('user_service/login-form.html')
                # Login and validate the user.
                # user should be an instance of your `User` class
                login_user(user)

                return redirect(url_for('main.index'))
            else:
                flash(u"Wrong password.", 'danger')
                return render_template('user_service/login-form.html')
        except Exception as e:
            flash(u"Username doesn't exist.", 'danger')
            app.logger.exception(e)
            return render_template('user_service/login-form.html')


@user_service.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'GET':
        return render_template('user_service/register-form.html')
    else:
        username = request.form.get('lg-username')
        password = sha256_crypt.encrypt(request.form.get('lg-password'))
        fname = request.form.get('lg-fname')
        lname = request.form.get('lg-lname')
        email = request.form.get('lg-email')
        status = "user" # New users are never admin
        active = True

        user_obj = User(username, password, fname, lname, email, status, active)

        if user_data_access.add_user(user_obj):
            # Login and validate the user.
            # user should be an instance of your `User` class
            login_user(user_data_access.get_user(username))
            return redirect(url_for('main.index'))

        flash(u'That username is already in use.', 'danger')
        return render_template('user_service/register-form.html')


@user_service.route('/logout')
@login_required
def logout():
    active_user_handler.remove_active_states_of_user(current_user.username)
    logout_user()
    flash(u"Successfully logged out!", 'success')
    return redirect(url_for('main.index'))


@user_service.route('/user-data', methods=['GET', 'POST'])
@login_required
def user_data():
    if request.method == 'GET':
        return render_template('user_service/user-data.html')
    else:
        if not sha256_crypt.verify(request.form.get('lg-current-password'), current_user.password):
            flash(u"Wrong password!", 'danger')
            return render_template('user_service/user-data.html')

        fname = request.form.get('lg-fname')
        lname = request.form.get('lg-lname')
        email = request.form.get('lg-email')
        password = request.form.get('lg-new-password')

        if password == '':
            password = current_user.password
        else:
            password = sha256_crypt.encrypt(password)
        try:
            user_obj = User(current_user.username, password, fname, lname, email, current_user.status,
                            current_user.is_active)
            user_data_access.alter_user(user_obj)
            flash(u"User data has been updated!", 'succes')
        except:
            flash(u"User data couldn't be updated!", 'danger')
        return render_template('user_service/user-data.html')


@user_service.route('/admin-page', methods=['GET', 'POST'])
@login_required
def admin_page():
    if current_user.status != 'admin':
        return abort(403)
    admins = user_data_access.get_admins()
    print("Admins: ", admins)
    if request.method == 'GET':
        return render_template('user_service/admin-page.html', users=user_data_access.get_users(),
                                                               admins=admins)
    else:
        try:
            for user in user_data_access.get_users():
                # Checkbox uses username as it's identifier for Flask
                if request.form.get(user.username) is None:  # If the checkbox is unchecked the response is None
                    user.is_active = False
                else:  # If the checkbox is checked the response is 'on'
                    user.is_active = True
                user_data_access.alter_user(user)
            flash(u"User data has been updated!", 'success')
        except Exception:
            flash(u"User data couldn't be updated!", 'danger')
        return render_template('user_service/admin-page.html', users=user_data_access.get_users(), admins=admins)


@user_service.route('/admin-page/<string:username>/delete', methods=['DELETE'])
@login_required
def delete_user_as_admin(username):
    if current_user.status != 'admin':
        return abort(403)
    try:
        user_data_access.delete_user(data_loader, username)
        flash(u"User has been removed!", 'success')
    except Exception as e:
        flash(u"User couldn't be removed!", 'danger')

    if current_user.username == username:
        return redirect(url_for('user_service.logout'), code=303)

    return redirect(url_for('user_service.admin_page'), code=303)


@user_service.route('/user-data/<string:username>/delete', methods=['POST'])
@login_required
def delete_own_account(username):
    try:
        user_data_access.delete_user(data_loader, username)
        flash(u"your account has been removed!", 'success')
    except Exception:
        flash(u"your account couldn't be removed!", 'danger')
    if current_user.username == username:
        return logout()

    return user_data()

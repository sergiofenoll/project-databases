from flask import Flask, render_template, request, session, jsonify
from passlib.hash import sha256_crypt
app = Flask(__name__)


# Mock users
mock_users = {'sff': sha256_crypt.encrypt('password')}


# API
@app.route('/login', methods=['POST'])
def send_login_request():
    username = request.form.get('lg-username')
    password = request.form.get('lg-password')

    print('Validating data for user "{0}"'.format(username))

    if username not in mock_users or sha256_crypt.verify(password, mock_users[username]):
        return render_template('login-form.html', failed_login=True)
    return 'Logged in!'


# Views
@app.route('/')
def index():
    return "Hello, World!"


@app.route('/login')
def login():
    return render_template('login-form.html', failed_login=False)


@app.route('/register')
def register():
    return render_template('register-form.html')


if __name__ == "__main__":
    app.run()

#!/bin/bash

# If you're encountering this error: psql: FATAL: Peer authentication failed for user "postgres"
# See this answer: https://stackoverflow.com/a/21166595 and change authentication method to trust for postgres and *all* users
# If you're still not sure about what to do, send Sergio a message

# If you don't use virtualenv the modules will be installed systemwide!
# Please consider installing virtualenv (sudo apt install python3-virtualenv in Ubuntu)

psql -U postgres -c "CREATE ROLE dbadmin WITH LOGIN PASSWORD 'dbadmin';"
psql -U postgres -c "ALTER ROLE dbadmin CREATEDB;"

psql -U postgres -c "CREATE DATABASE userdb OWNER dbadmin;"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE userdb TO dbadmin;"

psql -U dbadmin -d userdb -f sql/tables.sql

virtualenv venv/
source venv/bin/activate
pip3 install -r requirements.txt
deactivate

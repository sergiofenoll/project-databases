CREATE ROLE stoereboi WITH LOGIN
  PASSWORD 'stoereboi';
ALTER ROLE stoereboi
CREATEDB;
CREATE DATABASE userdb OWNER stoereboi;

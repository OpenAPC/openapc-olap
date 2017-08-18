-- Having installed postgres, execute this script via
--     sudo -u postgres psql -f setup.sql -v pw="'secret'"
-- to set up the openapc database. Replace the pw parameter by something more
-- advanced and copy it into the db_settings.ini 
CREATE USER table_creator WITH PASSWORD :pw;
CREATE USER cubes_user WITH PASSWORD 'no_password';
CREATE DATABASE openapc_db;
\c openapc_db;
CREATE SCHEMA openapc_schema;
GRANT ALL PRIVILEGES ON SCHEMA openapc_schema TO table_creator WITH GRANT OPTION;
GRANT USAGE ON SCHEMA openapc_schema TO cubes_user;

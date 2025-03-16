/*
Create database, schemas for bronze, silver and gold layers and also an user `dwh_user`.

This bootstrap script gets automatically executed as part of postgres docker container. If mounted postgres data volume already has a database, the initialisation script won't run.

Create database sql_dwh_db if it doesn't exist and create schemas for bronze, silver, gold layers. Also create a user dwh_user with custom password with restricted access. 

*/

SELECT 'CREATE DATABASE sql_dwh_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'sql_dwh_db')\gexec

\c sql_dwh_db;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dwh_user') THEN
        CREATE USER dwh_user WITH 
            PASSWORD 'dwh_password'
            VALID UNTIL 'infinity'
            CONNECTION LIMIT -1; -- Replace with a strong password
    END IF;
END $$;

GRANT USAGE, CREATE ON SCHEMA bronze TO dwh_user;
GRANT USAGE, CREATE ON SCHEMA silver TO dwh_user;
GRANT USAGE, CREATE ON SCHEMA gold TO dwh_user;

GRANT CONNECT ON DATABASE sql_dwh_db to dwh_user;



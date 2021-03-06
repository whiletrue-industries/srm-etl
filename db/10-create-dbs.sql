CREATE DATABASE airflow;
CREATE DATABASE auth;
CREATE DATABASE etls;
CREATE DATABASE datasets;

GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;
GRANT ALL PRIVILEGES ON DATABASE auth TO postgres;
GRANT ALL PRIVILEGES ON DATABASE etls TO postgres;
GRANT ALL PRIVILEGES ON DATABASE datasets TO postgres;
#!/bin/bash
set -e

POSTGRES="psql --username ${POSTGRES_USER}"

echo "Creating database: connect"

$POSTGRES <<-EOSQL
CREATE DATABASE connect;
EOSQL

echo "Creating database user: connect"

$POSTGRES -d connect <<-EOSQL
CREATE USER kafkaconnect WITH ENCRYPTED PASSWORD '6UCa59tBPbGI';
GRANT ALL PRIVILEGES ON DATABASE connect TO kafkaconnect;
GRANT USAGE ON SCHEMA "public" TO kafkaconnect;
ALTER DEFAULT PRIVILEGES IN SCHEMA "public" GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO kafkaconnect;
EOSQL

echo "Creating database user: tester"

$POSTGRES -d connect <<-EOSQL
CREATE USER tester WITH ENCRYPTED PASSWORD '7vLlfbecvQGy';
GRANT ALL PRIVILEGES ON DATABASE connect TO tester;
GRANT USAGE ON SCHEMA "public" TO tester;
ALTER DEFAULT PRIVILEGES IN SCHEMA "public" GRANT SELECT ON TABLES TO tester;
EOSQL


#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE hdb_pipeline;
    GRANT ALL PRIVILEGES ON DATABASE hdb_pipeline TO $POSTGRES_USER;
EOSQL
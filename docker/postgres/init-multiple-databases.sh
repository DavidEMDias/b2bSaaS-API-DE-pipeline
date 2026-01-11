#!/bin/bash

set -e
set -u

function create_user_and_database() {
    local database=$1
    local username=$2
    local password=$3
    local alter_schema_owner=$4  # se "true", muda o dono do schema public
    echo "Creating user '$username' and database '$database'"

    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE USER $username WITH PASSWORD '$password';
        CREATE DATABASE $database;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $username;
EOSQL
    # AIRFLOW precisa de autorizaçao no schema public para criar as suas tabelas de metadados
    if [ "$alter_schema_owner" = "true" ]; then
        echo "  Changing owner of schema public to '$username' for database '$database'"
        psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
            \c $database
            ALTER SCHEMA public OWNER TO $username; 
EOSQL
    fi

    echo "  User '$username' and database '$database' created successfully"
}

# Metadata database (Airflow needs to create tables here)
create_user_and_database $METADATA_DATABASE_NAME $METADATA_DATABASE_USERNAME $METADATA_DATABASE_PASSWORD true

# ELT database (Airflow DAGs use it, mas não precisa ser dono do schema)
create_user_and_database $ELT_DATABASE_NAME $ELT_DATABASE_USERNAME $ELT_DATABASE_PASSWORD false

echo "All databases and users created successfully"

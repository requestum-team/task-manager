# DEPRECATED

#echo "--- starts entrypoint"
##!/bin/sh
#
## create db and role if not exists
#psql -U $POSTGRES_USER -h $POSTGRES_HOST -p $POSTGRES_PORT -tc "SELECT 1 FROM pg_database WHERE datname = '$POSTGRES_NAME'" |
#  grep -q 1 ||
#  psql -U $POSTGRES_USER -h $POSTGRES_HOST -p $POSTGRES_PORT -c "CREATE USER $POSTGRES_USER WITH ENCRYPTED PASSWORD '$POSTGRES_PASS';"
#  psql -U $POSTGRES_USER -h $POSTGRES_HOST -p $POSTGRES_PORT -c "CREATE DATABASE $POSTGRES_NAME;"
#  psql -U $POSTGRES_USER -h $POSTGRES_HOST -p $POSTGRES_PORT -c "GRANT ALL PRIVILEGES ON DATABASE $POSTGRES_NAME TO $POSTGRES_USER;"
#
## run migrations
#echo "--- starts migrations"
#psql "user=$POSTGRES_USER host=$POSTGRES_HOST port=$POSTGRES_PORT password=$POSTGRES_PASS dbname=$POSTGRES_NAME" -f ./sqlmigrations/001_create_schema.sql
#echo "--- ends entrypoint"
#exec "$@"

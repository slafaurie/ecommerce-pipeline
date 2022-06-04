#!/usr/bin/env bash


# dont load examples
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# Initiliase the metastore
airflow db init

# Run the scheduler in background
airflow scheduler &> /dev/null &

# Create user
airflow users create -u admin -p admin -r Admin -e admin@admin.com -f admin -l admin

# Run the web server in foreground (for docker logs)
exec airflow webserver
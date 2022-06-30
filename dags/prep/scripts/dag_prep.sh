#!/bin/bash

## BUILD DIRECTORIES
FOLDER_DATA="/opt/airflow/dags/data"
subdirs=("raw" "transient" "curated")

for subdir in "${subdirs[@]}"; do
    if [ ! -d "$FOLDER_DATA/$subdir" ]; then
            mkdir "$FOLDER_DATA/$subdir"
    fi
done

## ADD POSTGRES CONNECTION
POSTGRES_CONN="postgres_ecommerce"
airflow connections delete $POSTGRES_CONN
airflow connections add $POSTGRES_CONN \
        --conn-host postgres \
        --conn-login postgres \
        --conn-password postgres \
        --conn-port 5432 \
        --conn-schema curated \
        --conn-type postgres

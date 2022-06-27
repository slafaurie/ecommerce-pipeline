#!/bin/bash



airflow connections add postgres_ecommerce \
        --conn-host postgres \
        --conn-login postgres \
        --conn-password postgres \
        --conn-port 5432 \
        --conn-schema curated \
        --conn-type postgres

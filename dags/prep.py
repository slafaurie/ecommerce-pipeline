
from airflow import DAG 
from airflow.operators.python import PythonOperator

from datetime import datetime

from prep.build_directories import build_directories
from prep.preprocessing import prep_olist_files

default_args = {
    "start_date": datetime(2022,1,1),
    "owner": "Airflow" 
}

with DAG(dag_id="prep", schedule_interval=None, default_args=default_args) as dag:

    build_directories_task = PythonOperator(
        task_id = "build-directories",
        python_callable=build_directories
    )

    prep_olist_files_task = PythonOperator(
        task_id = "prep-olist-files",
        python_callable=prep_olist_files
    )

    build_directories_task >> prep_olist_files_task

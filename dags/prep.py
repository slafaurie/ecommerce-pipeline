
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime


from prep.build_directories import build_directories
from prep.preprocessing import prep_olist_files

default_args = {
    "start_date": datetime(2022,1,1),
    "owner": "Airflow" 
}

init_query = "SELECT 'HELLO BITCHES'"

def print_cwd():
    import os
    print(os.getcwd())


with DAG(dag_id="prep", schedule_interval=None, default_args=default_args) as dag:


    dag_prep = BashOperator(
        task_id = "prep_dag_script",
        bash_command = "/opt/airflow/dags/prep/scripts/entrypoint_dag_prep.sh "
    )

    postgres_hello = PostgresOperator(
        task_id = "prep-postgres",
        postgres_conn_id = "postgres_ecommerce",
        sql=init_query
    )

    prep_olist_files_task = PythonOperator(
        task_id = "prep-olist-files",
        python_callable=prep_olist_files
    )

    dag_prep >> postgres_hello >> prep_olist_files_task



from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime
from prep.preprocessing import prep_olist_files

default_args = {
    "start_date": datetime(2022,1,1),
    "owner": "Airflow" 
}


prep_file = "/opt/airflow/dags/prep/scripts/dag_prep.sh"


init_query = "SELECT 'HELLO POSTGRES'"

schema_query = open("/opt/airflow/dags/prep/scripts/create_schema.sql","r").read()
orders_schema_query = open("/opt/airflow/dags/prep/scripts/create_tables.sql", "r").read()

with DAG(dag_id="prep", schedule_interval=None, default_args=default_args) as dag:


    dag_prep = BashOperator(
        task_id = "prep_dag_script",
        bash_command = f"chmod +x {prep_file} && bash {prep_file} "
    )

    postgres_hello = PostgresOperator(
        task_id = "hello_postgres",
        postgres_conn_id = "postgres_ecommerce",
        sql=init_query
    )

    postgres_schema = PostgresOperator(
        task_id = "schema_postgres",
        postgres_conn_id = "postgres_ecommerce",
        sql=schema_query
    )

    postgres_tables = PostgresOperator(
        task_id = "tables_postgres",
        postgres_conn_id = "postgres_ecommerce",
        sql=orders_schema_query
    )


    # prep_olist_files_task = PythonOperator(
    #     task_id = "prep-olist-files",
    #     python_callable=prep_olist_files
    # )

    dag_prep >> postgres_hello >> postgres_schema >> postgres_tables


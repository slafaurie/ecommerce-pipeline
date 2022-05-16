from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

from curated.orders_lean.run import run_orders_lean
from curated.orders_ranking.run import run_order_ranking

default_args = {
    "start_date": datetime(2016,9,5),
    "end_date": datetime(2018,10,17),
    "owner": "Airflow" ,
    "wait_for_downstream":True
}

with DAG(dag_id="curated-dags", schedule_interval= "@daily", default_args=default_args, catchup=True) as dag:

    dummy_start = DummyOperator(
        task_id = "start-curated-dags"
    )

    curated_orders_lean = PythonOperator(
        task_id = "transient-orders-lean",
        python_callable=run_orders_lean,
        op_kwargs = {"ds_date": "{{ ds }}" }
    )

    curated_order_ranking = PythonOperator(
        task_id = "transient-order-rankings",
        python_callable=run_order_ranking,
        op_kwargs = {
            "start_date": "2016-09-04", 
            "end_date":"{{ ds }}"
            }
    )

    dummy_start >> curated_orders_lean >> curated_order_ranking
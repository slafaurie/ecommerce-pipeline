from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import airflow.macros

from datetime import datetime

from curated.orders_lean.run import run_orders_lean
from curated.orders_ranking.run import run_order_ranking
from curated.orders.run import run_orders



default_args = {
    "start_date": datetime(2016,9,5),
    "end_date": datetime(2018,10,17),
    "owner": "Airflow" ,
    "wait_for_downstream":True
}



# print(partitions_dates)

with DAG(dag_id="curated-dags", schedule_interval= "@daily", default_args=default_args, catchup=True) as dag:

    # start_window =None

    end_window = "{{ ds }}"
    start_window = '{{macros.ds_add(ds, -7)}}'
    partitions_dates = [start_window, end_window]

    dummy_start = DummyOperator(
        task_id = "start-curated-dags"
    )

    curated_orders_lean = PythonOperator(
        task_id = "transient-orders-lean",
        python_callable=run_orders_lean,
        op_kwargs = { "partition_dates": partitions_dates }
    )

    curated_order_ranking = PythonOperator(
        task_id = "transient-order-rankings",
        python_callable=run_order_ranking,
        op_kwargs = {
            "start_date": "2016-09-04", 
            "end_date": end_window
            }
    )

    curated_orders = PythonOperator(
        task_id = "curated-orders",
        python_callable=run_orders,
        op_kwargs = { "partition_dates": partitions_dates }
    )


    dummy_end = DummyOperator(
        task_id = "end-curated-dags"
    )

    dummy_start >> curated_orders_lean >> curated_order_ranking >> curated_orders >> dummy_end
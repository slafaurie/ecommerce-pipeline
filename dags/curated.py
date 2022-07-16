from airflow import DAG, macros
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator


from datetime import datetime, timedelta
import time


from common.datalake import Datalake
from common.operators.etl_operator import ETLOperator
from common.operators.postgres import DataLake2PostgresOperator, Staging2CuratedOperator

from curated.orders_lean.models.transformer import OrderLeanTransformer
from curated.orders_ranking.models.transformer import OrderRankingTransformer
from curated.orders.models.transformer import OrdersTransformer
from airflow.operators.trigger_dagrun import TriggerDagRunOperator




default_args = {
    "start_date": datetime(2016,9,6),
    "end_date": datetime(2018,10,20),
    "owner": "Airflow" ,
    "wait_for_downstream":True
}

def check_order_data(start, end):
    tmp = Datalake.read_partitioned_dataframe(zone="curated", dataset="orders", partition_dates=[start,end])
    if tmp is None:
        print("No data today, no need to load data to postgres")
        return ["no-order"]
    else:
        return ["postgres-staging-orders", "postgres-curated-orders"]

def sleep_dagrun():
    """
    Introduce a lag between dags runs to mimic time betweens two days
    """
    time.sleep(30*1)


with DAG(
    dag_id="curated-dags", 
    schedule_interval= timedelta(days=4),
     default_args=default_args, 
     catchup=True,
     max_active_runs=1,
     user_defined_macros={
        "earliest_date":datetime(2016,9,1).strftime("%Y-%m-%d")
     }
     ) as dag:

    dummy_start = DummyOperator(
        task_id = "start-curated-dags"
    )


    curated_orders_lean = ETLOperator(
        task_id = "transient-orders-lean",
        sources_dict =  {
                            "partitioned": [ 
                                ["raw", "olist_orders_dataset"],
                                ["raw", "olist_order_payments_dataset"],
                                ["raw", "olist_order_items_dataset"],
                            ],
                            "full_tables": [
                                ["raw", "olist_sellers_dataset.parquet"],
                                ["raw", "olist_customers_dataset.parquet"],
                            ]
                        },
        output_params = ["transient", "orders_lean", "purchase_date"], 
        transformer_callable = OrderLeanTransformer,
        start =  "{{macros.ds_add(ds, -7)}}" ,
        end = "{{ ds }}"
    )


    curated_order_rankings = ETLOperator(
        task_id = "transient-orders-ranking",
        sources_dict =  {
                            "partitioned": [ 
                                ["transient", "orders_lean"]
                            ]
                        },
        output_params = ["transient", "orders_ranking", "purchase_date"], 
        transformer_callable = OrderRankingTransformer,
        start =  "{{earliest_date}}" ,
        end = "{{ ds }}"
    )

    curated_orders = ETLOperator(
        task_id = "curated-orders",
        sources_dict =  {
                            "partitioned": [ 
                                ["transient", "orders_lean"],
                                ["transient", "orders_ranking"]
                            ]
                        },
        output_params = ["curated", "orders", "purchase_date"], 
        transformer_callable = OrdersTransformer,
        start =  "{{macros.ds_add(ds, -7)}}" ,
        end = "{{ ds }}"
    )

    staging_postgres_orders = DataLake2PostgresOperator(
        task_id = "postgres-staging-orders",
        postgres_conn_id = "postgres_ecommerce",
        name = "orders",
        zone = "curated",
        start =  "{{macros.ds_add(ds, -7)}}" ,
        end = "{{ ds }}"
    )

    curated_postgres_orders = Staging2CuratedOperator(
        task_id = "postgres-curated-orders",
        postgres_conn_id = "postgres_ecommerce",
        table = "orders",
        key = "order_id"
    )

    skip_postgres_load = DummyOperator(
        task_id = "no-order"
    )

    branch_task = BranchPythonOperator(
        task_id="check_order_data",
        python_callable= check_order_data,
        op_kwargs = {
            "start": "{{macros.ds_add(ds, -7)}}",
             "end":"{{ ds }}"
             }
        
    )

    end_branch = DummyOperator(task_id="end_branch", trigger_rule="none_failed")


    dummy_end = DummyOperator(
        task_id = "end-curated-dags"
    )

    trigger_datamart = TriggerDagRunOperator(
        task_id="trigger-datamart",
        trigger_dag_id="datamart-dags"

    )

    sleep_dag = PythonOperator(
        task_id="sleep_dagrun",
        python_callable=sleep_dagrun
    )

    # dummy_start >> sleep_dag >> curated_orders_lean >> curated_order_rankings >> curated_orders
    dummy_start >> curated_orders_lean >> curated_order_rankings >> curated_orders
    curated_orders >> branch_task
    branch_task >> staging_postgres_orders >> curated_postgres_orders >> end_branch
    branch_task >> skip_postgres_load >> end_branch
    end_branch >> dummy_end >> trigger_datamart >> sleep_dag



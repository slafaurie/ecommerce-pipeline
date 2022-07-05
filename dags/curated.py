from airflow import DAG, macros
from airflow.operators.dummy import DummyOperator

from datetime import datetime


from common.operators.etl_operator import ETLOperator
from common.operators.postgres import DataLake2PostgresOperator, Staging2CuratedOperator

from curated.orders_lean.models.transformer import OrderLeanTransformer
from curated.orders_ranking.models.transformer import OrderRankingTransformer
from curated.orders.models.transformer import OrdersTransformer




default_args = {
    "start_date": datetime(2016,9,5),
    "end_date": datetime(2018,10,20),
    "owner": "Airflow" ,
    "wait_for_downstream":True
}


with DAG(
    dag_id="curated-dags", 
    schedule_interval= "@daily",
     default_args=default_args, 
     catchup=True,
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


    dummy_end = DummyOperator(
        task_id = "end-curated-dags"
    )

    dummy_start >> curated_orders_lean >> curated_order_rankings >> curated_orders
    curated_orders >> staging_postgres_orders >> curated_postgres_orders >> dummy_end


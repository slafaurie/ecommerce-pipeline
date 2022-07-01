from airflow import DAG, macros
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
# from airflow.macros import ds_add

from datetime import datetime

# from curated.orders_lean.run import run_orders_lean
# from curated.orders_ranking.run import run_order_ranking
# from curated.orders.run import run_orders

from curated.orders_lean.models.transformer import OrderLeanTransformer
from common.operators.pandas_operator import ETLOperator



default_args = {
    "start_date": datetime(2016,9,5),
    "end_date": datetime(2018,10,20),

    "owner": "Airflow" ,
    "wait_for_downstream":True
}



# print(partitions_dates)

with DAG(dag_id="curated-dags", schedule_interval= "@daily", default_args=default_args, catchup=True) as dag:

    dummy_start = DummyOperator(
        task_id = "start-curated-dags"
    )

    # curated_orders_lean = PythonOperator(
    #     task_id = "transient-orders-lean",
    #     python_callable=run_orders_lean,
    #     op_kwargs = { "partition_dates": partitions_datespy }
    # )

    # start_date = "{{ ds }}"
    # end_date = "{{prev_ds}}"

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
        # end_date = end_date
    )



    # curated_order_ranking = PythonOperator(
    #     task_id = "transient-order-rankings",
    #     python_callable=run_order_ranking,
    #     op_kwargs = {
    #         "start_date": "2016-09-04", 
    #         "end_date": end_window
    #         }
    # )

    # curated_orders = PythonOperator(
    #     task_id = "curated-orders",
    #     python_callable=run_orders,
    #     op_kwargs = { "partition_dates": partitions_dates }
    # )


    dummy_end = DummyOperator(
        task_id = "end-curated-dags"
    )

    dummy_start >> curated_orders_lean >> dummy_end

    # >> curated_order_ranking >> curated_orders >>
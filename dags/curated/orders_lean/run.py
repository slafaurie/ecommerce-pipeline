# TODO - Complete orchestrator
# TODO - Add logger

import logging

from common.data_model import DataModel
from curated.orders_lean.models.transformer import OrderLeanTransformer
from airflow.macros import ds_add


def run_orders_lean(ds_date):


    partition_dates = [ds_add(ds_date, -7), ds_date]


    # Args
    ZONE = "raw"
    logging.basicConfig(
        level= logging.INFO,
        format= " Orders Transformer - %(asctime)s - %(levelname)s - %(message)s"
    )

    logger = logging.getLogger(__name__)
    logger.info("Start loading process...")

    logger.info(f"Window to process {partition_dates}")

    # load
    DataModel.set_mode(local=True)
    orders = DataModel.read_partitioned_dataframe(zone=ZONE, dataset="olist_orders_dataset", partition_dates = partition_dates)
    items = DataModel.read_partitioned_dataframe(zone=ZONE, dataset="olist_order_items_dataset", partition_dates = partition_dates)
    payments = DataModel.read_partitioned_dataframe(zone=ZONE, dataset="olist_order_payments_dataset", partition_dates = partition_dates)
    seller = DataModel.read_dataframe(zone=ZONE, dataset="olist_sellers_dataset.parquet")
    customer = DataModel.read_dataframe(zone=ZONE, dataset="olist_customers_dataset.parquet")

    # transform
    logger.info("Start transformation process...")
    order_lean = OrderLeanTransformer.curate_orders_lean(orders, payments, items, seller, customer)

    # save
    DataModel.write_partitioned_dataframe(order_lean, zone="transient", dataset="orders_lean", partition_column="purchase_date")
    logger.info("Curation done")


# if __name__ == "__main__":

    # run("2018-01-01")
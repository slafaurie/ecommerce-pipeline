# TODO - Complete orchestrator
# TODO - Add logger

import logging

from common.data_model import DataModel
from curated.orders_lean.models.transformer import OrderLeanTransformer


def run():

    # Args
    ZONE = "raw"
    logging.basicConfig(
        level= logging.INFO,
        format= " Orders Transformer - %(asctime)s - %(levelname)s - %(message)s"
    )

    logger = logging.getLogger(__name__)
    logger.info("Start loading process...")

    # load
    DataModel.print_mode()
    orders = DataModel.read_parquet_to_dataframe(zone=ZONE, dataset="olist_orders_dataset.parquet")
    items = DataModel.read_parquet_to_dataframe(zone=ZONE, dataset="olist_order_items_dataset.parquet")
    payments = DataModel.read_parquet_to_dataframe(zone=ZONE, dataset="olist_order_payments_dataset.parquet")
    seller = DataModel.read_parquet_to_dataframe(zone=ZONE, dataset="olist_sellers_dataset.parquet")
    customer = DataModel.read_parquet_to_dataframe(zone=ZONE, dataset="olist_customers_dataset.parquet")

    # transform
    logger.info("Start transformation process...")
    order_lean = OrderLeanTransformer.curate_orders_transient(orders, payments, items, seller, customer)

    # save
    DataModel.write_df_to_s3_as_parquet(order_lean, zone="transient", dataset="orders_lean.parquet")
    logger.info("Curation done")


if __name__ == "__main__":
    run()
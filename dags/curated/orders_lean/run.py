# TODO - Complete orchestrator
# TODO - Add logger

import logging

from common.data_model import DataModel
from curated.orders_lean.models.transformer import OrderLeanTransformer


def run_orders_lean(partition_dates):

    sources_dict = {
        "partitioned": [ 
            ["raw", "olist_orders_dataset"],
            ["raw", "olist_order_payments_dataset"],
            ["raw", "olist_order_items_dataset"],
        ],
        "full_tables": [
            ["raw", "olist_sellers_dataset.parquet"],
            ["raw", "olist_customers_dataset.parquet"],
        ]
    }

    output_params = ["transient", "orders_lean", "purchase_date"] 


    # Args
    logging.basicConfig(
        level= logging.INFO,
        format= " Orders Transformer - %(asctime)s - %(levelname)s - %(message)s"
    )

    logger = logging.getLogger(__name__)
    logger.info("Start loading process...")

    logger.info(f"Window to process {partition_dates}")

    # load


    load_partitioned_sources = [DataModel.read_partitioned_dataframe(*inputs) for inputs in sources_dict.get("partitioned")]
    load_non_partitioned_sources = [DataModel.read_dataframe(*inputs) for inputs in sources_dict.get("full_tables")]
    all_sources = load_partitioned_sources + load_non_partitioned_sources

    
    # orders = DataModel.read_partitioned_dataframe(zone=ZONE, dataset="olist_orders_dataset", partition_dates = partition_dates)
    # if orders is None:
    #     logger.info("No order data to process...")
    #     return None
    # items = DataModel.read_partitioned_dataframe(zone=ZONE, dataset="olist_order_items_dataset", partition_dates = partition_dates)
    # payments = DataModel.read_partitioned_dataframe(zone=ZONE, dataset="olist_order_payments_dataset", partition_dates = partition_dates)
    # seller = DataModel.read_dataframe(zone=ZONE, dataset="olist_sellers_dataset.parquet")
    # customer = DataModel.read_dataframe(zone=ZONE, dataset="olist_customers_dataset.parquet")


    # transform
    logger.info("Start transformation process...")
    output = OrderLeanTransformer.curate_orders_lean(*all_sources)

    # save
    DataModel.write_partitioned_dataframe(output, *output_params)
    logger.info("Curation done")


# if __name__ == "__main__":

    # run("2018-01-01")
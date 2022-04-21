import logging

from common.data_model import DataModel
from curated.orders.models.transformer import OrdersTransformer


def run():

    # Args
    ZONE = "transient"

    logging.basicConfig(
        level= logging.INFO,
        format= " Orders Transformer - %(asctime)s - %(levelname)s - %(message)s"
    )

    logger = logging.getLogger(__name__)
    logger.info("Start loading process...")

    # load
    DataModel.print_mode()
    orders_lean = DataModel.read_parquet_to_dataframe(zone=ZONE, dataset="orders_lean.parquet")
    orders_ranking = DataModel.read_parquet_to_dataframe(zone=ZONE, dataset="orders_ranking.parquet")
  

    # transform
    logger.info("Start transformation process...")
    orders = OrdersTransformer.curate_orders(orders_lean, orders_ranking)

    # save
    DataModel.write_df_to_s3_as_parquet(orders, zone="curated", dataset="orders.parquet")
    logger.info("Curation done")


if __name__ == "__main__":
    run()
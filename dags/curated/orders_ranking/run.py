import logging

from common.data_model import DataModel
from curated.orders_ranking.models.transformer import OrderRankingTransformer


def run():

    # Args
    ZONE = "transient"

    logging.basicConfig(
        level= logging.INFO,
        format= " Orders Ranking Transformer - %(asctime)s - %(levelname)s - %(message)s"
    )

    logger = logging.getLogger(__name__)
    logger.info("Start loading process...")

    # load
    DataModel.print_mode()
    orders = DataModel.read_parquet_to_dataframe(zone=ZONE, dataset="orders_lean.parquet")
  

    # transform
    logger.info("Start transformation process...")
    order_ranking = OrderRankingTransformer.curate_order_rankings(orders)

    # save
    DataModel.write_df_to_s3_as_parquet(order_ranking, zone="transient", dataset="orders_ranking.parquet")
    logger.info("Curation done")


if __name__ == "__main__":
    run()
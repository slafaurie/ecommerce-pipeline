import logging

from common.data_model import DataModel
from curated.orders_ranking.models.transformer import OrderRankingTransformer


def run(partition_date):

    # Args
    ZONE = "transient"

    logging.basicConfig(
        level= logging.INFO,
        format= " Orders Ranking Transformer - %(asctime)s - %(levelname)s - %(message)s"
    )

    logger = logging.getLogger(__name__)
    logger.info("Start loading process...")

    # load
    DataModel.set_mode(local=True)
    orders = DataModel.read_partitioned_dataframe(zone=ZONE, dataset="olist_orders_dataset", partition_date = partition_date, direction="lt")
  

    # transform
    logger.info("Start transformation process...")
    order_ranking = OrderRankingTransformer.curate_order_rankings(orders)

    # save
    DataModel.write_df(order_ranking, zone="transient", dataset="orders_ranking.parquet")
    logger.info("Curation done")


if __name__ == "__main__":
    run()
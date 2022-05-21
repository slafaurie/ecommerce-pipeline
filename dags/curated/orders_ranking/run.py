import logging

from common.data_model import DataModel
from curated.orders_ranking.models.transformer import OrderRankingTransformer
# from airflow.macros import ds_add



def run_order_ranking(start_date, end_date):

    partition_dates = [start_date, end_date]

    # Args
    ZONE = "transient"

    logging.basicConfig(
        level= logging.INFO,
        format= " Orders Ranking Transformer - %(asctime)s - %(levelname)s - %(message)s"
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Start loading process with dates equal to {partition_dates}")

    # load
    DataModel.set_mode(local=True)
    orders = DataModel.read_partitioned_dataframe(zone=ZONE, dataset="orders_lean", partition_dates = partition_dates)
  

    # transform
    logger.info("Start transformation process...")
    order_ranking = OrderRankingTransformer.curate_order_rankings(orders)

    # save
    DataModel.write_partitioned_dataframe(order_ranking, zone="transient", dataset="orders_ranking", partition_column="purchase_date")
    # DataModel.write_df(order_ranking, zone="transient", dataset="orders_ranking.parquet")

    
    logger.info("Curation done")


# if __name__ == "__main__":
    # run()
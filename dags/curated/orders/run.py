import logging

from common.data_model import DataModel
from curated.orders.models.transformer import OrdersTransformer



def run_orders(partition_dates):


    # Args
    ZONE = "transient"

    logging.basicConfig(
        level= logging.INFO,
        format= " Orders Transformer - %(asctime)s - %(levelname)s - %(message)s"
    )

    logger = logging.getLogger(__name__)
    logger.info("Start loading process...")

    # load
    DataModel.set_mode(local=True)
    orders_lean = DataModel.read_partitioned_dataframe(zone=ZONE, dataset="orders_lean", partition_dates=partition_dates)
    if orders_lean is None:
        logger.info("No order data to process...")
        return None
        
    orders_ranking = DataModel.read_partitioned_dataframe(zone=ZONE, dataset="orders_ranking", partition_dates=partition_dates)
  

    # transform
    logger.info("Start transformation process...")
    orders = OrdersTransformer.curate_orders(orders_lean, orders_ranking)

    # save
    DataModel.write_partitioned_dataframe(orders, zone="curated", dataset="orders", partition_column="purchase_date")
    logger.info("Curation done")


# if __name__ == "__main__":
    # run()
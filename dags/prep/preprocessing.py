import pandas as pd
import os
import logging

from common.datalake import Datalake 

from prep.prep_utils.constants import FilePath
from prep.prep_utils.preprocess import get_orders_with_multiple_seller, remove_order_with_multiple_seller
from prep.prep_utils.chunk_data import chunk_dataframe, filter_days_without_items_and_payments

from typing import List

# logger
logging.basicConfig(
    level= logging.INFO,
    format= "ETL Prep  - %(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)



def save_to_parquet(df_, filename):
    """
    Save file to parquet
    """

    Datalake.write_dataframe(df_, FilePath.SAVE_ZONE, filename.split(".")[0] + ".parquet")

   
def prep_orders_datasets(df: pd.DataFrame, dataset:str, orders_with_multiple_seller:pd.DataFrame, dates: List[str], orders_with_dates):
    logging.info(f"Preparing {dataset}...")
    (
        df
        .pipe(remove_order_with_multiple_seller, orders_with_multiple_seller)
        .pipe(chunk_dataframe, dataset, dates, orders_with_dates)
    )


def prep_non_orders_dataset(dataset:str):
    logging.info(f"Preparing {dataset}...")
    (
        Datalake.read_csv_as_dataframe(FilePath.ZONE, dataset)
        .pipe(save_to_parquet, dataset)
    )


def prep_olist_files():
    # load required data for prep in cs
    items = Datalake.read_csv_as_dataframe(FilePath.ZONE, FilePath.ITEMS)
    orders = Datalake.read_csv_as_dataframe(FilePath.ZONE, FilePath.ORDERS)
    payments = Datalake.read_csv_as_dataframe(FilePath.ZONE, FilePath.PAYMENTS)


    # prep - prep
    orders_with_multiple_seller = get_orders_with_multiple_seller(items)
    dates, orders_with_dates = filter_days_without_items_and_payments(orders, items, payments)

    # process already loaded datasets
    for df, dataset in zip([items, orders, payments], FilePath.return_datasets_to_chunk()):
        prep_orders_datasets(df, dataset, orders_with_multiple_seller, dates, orders_with_dates)

    # process rest

    for dataset in FilePath.return_other_datasets():
        prep_non_orders_dataset(dataset)

# if __name__ == "__main__":
    # set_dir_to_parent()
    # print(os.getcwd())





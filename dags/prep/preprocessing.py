import pandas as pd
import os
import logging

from common.data_model import DataModel 

from prep.prep_utils.constants import FilePath
from prep.prep_utils.preprocess import get_orders_with_multiple_seller, remove_order_with_multiple_seller
from prep.prep_utils.chunk_data import chunk_dataframe, filter_days_without_items_and_payments


# logger
logging.basicConfig(
    level= logging.INFO,
    format= "ETL Prep  - %(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# def set_dir_to_parent():
#     """
#     Set the working directory the root of the project.
    
#     """
#     currentdir = os.path.dirname(os.path.realpath(__file__))
#     parentdir = os.path.dirname(os.path.dirname(currentdir))
#     os.chdir(parentdir)


def save_to_parquet(df_, filename):
    """
    Save file to parquet
    """

    DataModel.write_df(df_, FilePath.SAVE_ZONE, filename.split(".")[0] + ".parquet")

   
def prep_orders_datasets(df: pd.DataFrame, dataset:str):
    logging.info(f"Preparing {dataset}...")
    (
        df
        .pipe(remove_order_with_multiple_seller, orders_with_multiple_seller)
        .pipe(chunk_dataframe, dataset, dates, orders_with_dates)
    )


def prep_non_orders_dataset(dataset:str):
    logging.info(f"Preparing {dataset}...")
    (
        pd.read_csv(os.path.join(DataModel.return_zone_path(FilePath.ZONE), dataset))
        .pipe(save_to_parquet, dataset)
    )



if __name__ == "__main__":
    # set_dir_to_parent()
    # print(os.getcwd())

    DataModel.set_mode(local=True)


    # load required data for prep in csv
    items = pd.read_csv(os.path.join(DataModel.return_zone_path(FilePath.ZONE), FilePath.ITEMS))
    orders = pd.read_csv(os.path.join(DataModel.return_zone_path(FilePath.ZONE), FilePath.ORDERS))
    payments = pd.read_csv(os.path.join(DataModel.return_zone_path(FilePath.ZONE), FilePath.PAYMENTS))


    # prep - prep
    orders_with_multiple_seller = get_orders_with_multiple_seller(items)
    dates, orders_with_dates = filter_days_without_items_and_payments(orders, items, payments)

    # process already loaded datasets
    for df, dataset in zip([items, orders, payments], FilePath.return_datasets_to_chunk()):
        prep_orders_datasets(df, dataset)

    # process rest
    others_datasets = [x for x in os.listdir(DataModel.return_zone_path(FilePath.ZONE)) if x not in FilePath.return_datasets_to_chunk()]
    for dataset in others_datasets:
        prep_non_orders_dataset(dataset)



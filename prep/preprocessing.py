import pandas as pd
import os
import logging

from prep_utils.constants import DataRoot
from prep_utils.preprocess import get_orders_with_multiple_seller, remove_order_with_multiple_seller
from prep_utils.chunk_data import chunk_dataframe, filter_days_without_items_and_payments


# logger
logging.basicConfig(
    level= logging.INFO,
    format= "ETL Prep  - %(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def set_dir_to_parent():
    """
    Set the working directory the root of the project.
    
    """
    currentdir = os.path.dirname(os.path.realpath(__file__))
    parentdir = os.path.dirname(currentdir)
    os.chdir(parentdir)


def save_to_parquet(df_, path, filename):
    """
    Save file to parquet
    """
    return (
        df_.to_parquet(
            os.path.join(
                path,
                filename.split(".")[0] + ".parquet"
            )
            , index=False
        )

    )

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
        pd.read_csv(DataRoot.return_dataset_path(dataset))
        .pipe(save_to_parquet, DataRoot.return_save_root(), dataset)
    )



if __name__ == "__main__":
    set_dir_to_parent()
    # print(os.getcwd())


    # load required data for prep in csv
    items = pd.read_csv(DataRoot.return_items_path())
    orders = pd.read_csv(DataRoot.return_orders_path())
    payments = pd.read_csv(DataRoot.return_payments_path())


    # prep - prep
    orders_with_multiple_seller = get_orders_with_multiple_seller(items)
    dates, orders_with_dates = filter_days_without_items_and_payments(orders, items, payments)

    # process already loaded datasets
    for df, dataset in zip([items, orders, payments], DataRoot.return_datasets_to_chunk()):
        prep_orders_datasets(df, dataset)

    # process rest
    others_datasets = [x for x in os.listdir(DataRoot.return_root()) if x not in DataRoot.return_datasets_to_chunk()]
    for dataset in others_datasets:
        prep_non_orders_dataset(dataset)



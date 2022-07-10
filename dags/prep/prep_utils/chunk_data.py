import pandas as pd
import os
from typing import List, Tuple
from datetime import datetime
from prep.prep_utils.constants import FilePath, DateColumns
from common.datalake import Datalake


def _get_orders_with_dates(df: pd.DataFrame) -> Tuple[List[datetime], pd.DataFrame]:
    """
    Create the partition date column and return both the dataframes and the unique list of dates
    preset in the orders dataset
    """
    df.loc[:, DateColumns.DATE_COLUMN] = pd.to_datetime(df[DateColumns.TIMESTAMP_COLUMN]).dt.date
    all_dates = df[DateColumns.DATE_COLUMN].sort_values().unique().tolist()
    return all_dates, df[["order_id", DateColumns.DATE_COLUMN]]

def _add_date_column(df: pd.DataFrame, orders_with_date: pd.DataFrame) -> pd.DataFrame:
    """
    Add the partition date column to the dataframe (df) if the column is not present
    """
    if not DateColumns.DATE_COLUMN in df.columns:
        # print("Adding date columns to Dataframe...")
        df = df.merge(orders_with_date, how="inner", on="order_id")
    return df


def filter_days_without_items_and_payments(orders, items, payments):

    """
    Returns a filtered date of list where all three main datasets: orders, items and payments contain data
    to be processed. It also return the dataframe with partition dates at the order_id level to join
    to dataframes that doesn't have this column.
    """

    dates, orders_with_date = _get_orders_with_dates(orders)

    items = (
        items
        .pipe(_add_date_column, orders_with_date)
    )


    payments = (
        payments
        .pipe(_add_date_column, orders_with_date)
    )


    days_without_items = set(dates).difference(set(items.purchase_date))
    days_without_payments = set(dates).difference(set(payments.purchase_date))
    days_without_items_or_payments = days_without_items.union(days_without_payments)
    filtered_dates = list(set(dates).difference(days_without_items_or_payments))

    return filtered_dates, orders_with_date

    

def chunk_dataframe(df, dataset, dates, orders_with_date):
    """
    split the entire dataframe by the purchase date and save each chunk of data into a folder
    """
    # print("Getting dates to chunk data")
    # dates, orders_with_date = filter_days_without_items_and_payments(DataRoot.return_orders_path())

    # print(f"Creating chunks for {dataset}")
    folder = os.path.join(Datalake.return_zone_path(FilePath.SAVE_ZONE), dataset).split(".")[0]

    # read_df and create generator:
    df = df.pipe(_add_date_column, orders_with_date)

    df_list = (df[df[DateColumns.DATE_COLUMN] == d] for d in dates)

    # save each chunk file within dataset folder
    for i, df_ in enumerate(df_list):

        if len(df_[DateColumns.DATE_COLUMN]) == 0:
            print(dates[i])
            raise Exception("No rows for date")

        date_str = df_[DateColumns.DATE_COLUMN].iloc[0].strftime("%Y-%m-%d")
        if not os.path.exists(folder):
            os.makedirs(folder)
        filename = dataset.split(".")[0] + "_"  + date_str + ".parquet"
        df_.to_parquet(os.path.join(folder, filename))
    
    # print("Removing origin dataset...")
    # os.remove(complete_path)
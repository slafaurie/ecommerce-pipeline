import pandas as pd
import os
import logging

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
    logging.info(f"Writing {filename} to {path}")
    return (
        df_.to_parquet(
            os.path.join(
                path,
                filename.split(".")[0] + ".parquet"
            )
            , index=False
        )

    )
   

def _get_orders_with_multiple_seller(items_path):
    """
    Return a dataframe with the orders that contains multiple sellers
    """
    return  (
                pd.read_csv(items_path)
                .groupby("order_id", as_index=False)
                .agg(
                    n_seller = ('seller_id', 'nunique')
                )
                .query("n_seller > 1")
            )


def remove_order_with_multiple_seller(df_, orders_with_multiple_sellers):
    """
    Return the orders datasets without orders with multiple sellers
    """      
    logger.info("Removing orders with multiple sellers...")
    return (
        df_
        .merge(orders_with_multiple_sellers, on='order_id', how='left', indicator=True)
        .query("_merge == 'left_only'")
        .drop(columns=['n_seller', '_merge'])
    )


def main(path, save_path):
    """
    Main function used to prep the csv files for the project pipeline. This implies:
        - Remove orders with multiple sellers
        - Split data into daily chunks
        - Save data into parquet to the raw folder
    """
    list_of_files_to_modify = ["olist_orders_dataset.csv", "olist_order_payments_dataset.csv", "olist_order_items_dataset.csv", "olist_order_reviews_dataset.csv"]
    df_orders_multiple_seller = _get_orders_with_multiple_seller(os.path.join(path, "olist_order_items_dataset.csv"))
    for f in os.listdir(path):
        logging.info(f"Preprocessing file {f}")
        df = pd.read_csv(os.path.join(path,f))
        if f in list_of_files_to_modify:
            df = remove_order_with_multiple_seller(df, df_orders_multiple_seller)
        save_to_parquet(df, save_path, f)



if __name__ == "__main__":
    path = "data\\kaggle"
    save_path = "data\\raw"
    set_dir_to_parent()
    main(path, save_path)
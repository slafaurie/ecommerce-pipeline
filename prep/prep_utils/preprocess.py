import pandas as pd


def get_orders_with_multiple_seller(items):
    """
    Return a dataframe with the orders that contains multiple sellers
    """
    return  (
                items
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
    return (
        df_
        .merge(orders_with_multiple_sellers, on='order_id', how='left', indicator=True)
        .query("_merge == 'left_only'")
        .drop(columns=['n_seller', '_merge'])
    )


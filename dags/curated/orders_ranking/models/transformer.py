import pandas as pd
import numpy as np
import logging

from common.utils import row_number, lead_window, days_diff, month_diff


class OrderRankingTransformer:
    _logger = logging.getLogger(__name__)


    def _add_ranking_columns(cls, df_):
        customer_col = "customer_unique_id"
        sort_col = "order_purchase_timestamp"

        ranking_dict = {
            "order_ranking": ([customer_col], [sort_col]), 
            "order_ranking_delivered": ([customer_col, "order_status"], [sort_col]),
            "order_at_seller_ranking_delivered": ([customer_col, "order_status", "seller_id"], [sort_col])
        }
        for col, ranking in ranking_dict.items():
            df_.loc[:, col] = row_number(df_, *ranking)
        return 
        
    def _clean_ranking_at_delivered_columns(df_):
        """
        Set to null if order_status != delivered
        """
        mask = df_.order_status == "delivered"
        mask_columns =  df_.columns.str.contains("ranking_delivered")

        clean_ranking = (
            df_
            .loc[:, mask_columns]
            .where(mask)
            .astype("Int64")
        )

        data = (
            pd.concat(
                [df_.loc[:, ~mask_columns]
                , clean_ranking
                ]
                , axis=1
            )
        )

        return data



    def _add_last_order_timestamp(df_):
        """
        Add last_order_timestamp column to the dataframe
        """
        df_.loc[:, "last_order_timestamp"] = lead_window(df_, ["customer_unique_id"], ["order_purchase_timestamp"], "order_purchase_timestamp", 1)
        return df_


    def _add_time_diff_columns(df_):
        """
        Add time differences columns to the dataframe
        """
        return (    df_.assign(
                        days_from_last_order = days_diff(df_.last_order_timestamp, df_.order_purchase_timestamp),
                        month_from_last_order = month_diff(df_.last_order_timestamp, df_.order_purchase_timestamp)
            )
            )

    def _clean_month_diff(df_):
        """
        Set to NA columns that does not have a previous order to calculate the time difference
        """
        df_.loc[:, "month_from_last_order"] = np.where(df_.last_order_timestamp.isnull(), pd.NA, df_.month_from_last_order)
        return df_

    @classmethod
    def curate_order_rankings(cls, df_):

        return (
            df_
            .pipe(cls._add_ranking_columns)
            .pipe(cls._clean_ranking_at_delivered_columns)
            .pipe(cls._add_last_order_timestamp)
            .pipe(cls._add_time_diff_columns)
            .pipe(cls._clean_month_diff)
        )



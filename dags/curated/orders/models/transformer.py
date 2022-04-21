import pandas as pd
import logging

class OrdersTransformer:
    _logger = logging.getLogger(__name__)

    def curate_orders(orders_lean: pd.DataFrame, orders_ranking: pd.DataFrame) -> pd.DataFrame:
        """
        Return the curated dataframe. At this point, only a join is required between the sources
        """
        return (
            orders_lean
            .merge(orders_ranking, how="left")
        )
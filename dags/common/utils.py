

"""
The functions listed here are part of the building bocks in the ETL but they're not coupled with any logic of the ETL, they 
can be reused in any other ETL. 

"""
import pandas as pd
from typing import List



def lead_window(
    df_: pd.DataFrame, 
    partition_cols: List[str], 
    sort_col: str,
    value_col:str, 
    shifts:int = 1
    ) -> pd.Series:
    """
    SQL-like lead function
    """
    return (
        df_
        .sort_values(by= sort_col, ascending=True)
        .groupby(partition_cols)[value_col]
        .shift(shifts)
    )


def row_number(
    data:pd.DataFrame, 
    partition_cols: List[str], 
    sort_col: str
    ) -> pd.Series:

    """
    SQL-like row_number function
    """
    return (
        data
        .sort_values(by= sort_col, ascending=True)
        .groupby(partition_cols)
        .cumcount() + 1
    )

def days_diff(
    start: pd.Series,
    end:pd.Series
    ) -> pd.Series:
    """
    Return the timedelta between two datetime columns in days
    """
    return (end - start).dt.days


def month_diff(
    start:pd.Series,
    end: pd.Series
    ) -> pd.Series:
    """
    Return the timedelta between two datetime columns in months
    """
    start = start.dt.to_period("M").view("float64")
    end = end.dt.to_period("M").view("float64")
    return (end - start).round(0).astype("Int64")


def flatten_columns(df_:pd.DataFrame) -> pd.DataFrame:
    """
    Flatten hierarchical columns of a dataframe by joining them with "_"
    """
    new_columns = ["_".join(cs) for cs in df_.columns.to_flat_index()]
    df_.columns = new_columns
    return df_
import os
from io import BytesIO

import boto3
import pandas as pd


class DataModel:
    """
    Class to handle all load/export within the ETL
    """
    # TODO - abstract bucket to config files
    session = boto3.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"], region_name="us-east-1")
    _s3 = session.resource("s3")
    _bucket = _s3.Bucket("slafaurie-airflow")
    prefix = "olist/one-run"

    @classmethod
    def read_parquet_to_dataframe(cls, zone: str, dataset: str):
        """
        Read a parquet file stored in S3 and return a dataframe
        """
        key = f"{cls.prefix}/{zone}/{dataset}"
        print(f"Reading file {dataset} from {cls._bucket.name} in {zone} zone")
        obj = cls._bucket.Object(key=key).get().get('Body').read()
        if not obj:
            raise Exception(f"{key} does not exist")
        data = BytesIO(obj)
        df = pd.read_parquet(data)
        return df
        

    @classmethod
    def write_df_to_s3_as_parquet(cls, df: pd.DataFrame, zone: str, dataset: str):
        """
        Write a pandas DF as parquet
        """ 
        key = f"{zone}/{dataset}"
        out_buffer =  BytesIO()
        df.to_parquet(out_buffer, index=False)
        print(f"Writing file {dataset} to {cls._bucket.name} in {zone} zone")
        cls._bucket.put_object(Body=out_buffer.getvalue(), Key=key)

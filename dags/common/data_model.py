import os
from io import BytesIO

import boto3
import pandas as pd

import logging



class DataModel:

    """
    Class to handle all load/export within the ETL
    """
    # TODO - abstract bucket to config files
    _session = boto3.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"], region_name="us-east-1")
    _s3 = _session.resource("s3")
    _bucket = _s3.Bucket("slafaurie-airflow")
    _prefix = "olist/one-run"
    _logger = logging.getLogger(__name__)
    # TODO -> how to go multiple parent folder
    _local_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))), "data") 

    @classmethod
    def set_mode(cls, local: bool = True):
        cls.local = local
        cls.work_dir = cls._local_path if cls.local else cls._prefix

    @classmethod
    def print_mode(cls):
        if cls.local:
            cls._logger.info(f"Data model is set to local. All files will be stored in {cls._local_path}")
        else:
            cls._logger.info(f"Data model is set to cloud. All files will be store in {cls._bucket}/{cls._prefix}")

    @classmethod
    def return_zone_path(cls, zone: str):
        return os.path.join(cls.work_dir, zone)

    @classmethod
    def read_dataframe(cls, zone: str, dataset: str):
        """
        Read a parquet file stored in S3 and return a dataframe
        """
        cls._logger.info(f"Reading file {dataset} in {zone} zone")
        if cls.local:
            path = os.path.join(cls.work_dir, zone, dataset)
            if not os.path.exists(path):
                raise Exception(f"{path} is not found")
            df = pd.read_parquet(path)

        else:
            key = f"{cls.work_dir}/{zone}/{dataset}"
            obj = cls._bucket.Object(key=key).get().get('Body').read()
            if not obj:
                raise Exception(f"{key} does not exist")
            data = BytesIO(obj)
            df = pd.read_parquet(data)
        return df
        
    @classmethod
    def write_df(cls, df: pd.DataFrame, zone: str, dataset: str):
        """
        Write a pandas DF as parquet
        """ 

        cls._logger.info(f"Writing file {dataset} in {zone} zone")

        if cls.local:
            path = os.path.join(cls.work_dir, zone, dataset)
            df.to_parquet(path, index=False)

        else:
            key = f"{cls.work_dir}/{zone}/{dataset}"
            out_buffer =  BytesIO()
            df.to_parquet(out_buffer, index=False)
            cls._bucket.put_object(Body=out_buffer.getvalue(), Key=key)

    

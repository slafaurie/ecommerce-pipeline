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
    local = True
    _local_path = 'C:\\Users\\chanl\\Documents\\cursos\\udemy\\airflow-hands-on-guide\\portfolio\\data'


    @classmethod
    def print_mode(cls):
        if cls.local:
            cls._logger.info(f"Data model is set to local. All files will be stored in {cls._local_path}")
        else:
            cls._logger.info(f"Data model is set to cloud. All files will be store in {cls._bucket}/{cls._prefix}")

    @classmethod
    def read_parquet_to_dataframe(cls, zone: str, dataset: str):
        """
        Read a parquet file stored in S3 and return a dataframe
        """
        cls._logger.info(f"Reading file {dataset} in {zone} zone")
        if cls.local:
            path = os.path.join(cls._local_path, zone, dataset)
            if not os.path.exists(path):
                raise Exception(f"{path} is not found")
            df = pd.read_parquet(path)

        else:
            key = f"{cls._prefix}/{zone}/{dataset}"
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

        cls._logger.info(f"Writing file {dataset} in {zone} zone")

        if cls.local:
            path = os.path.join(cls._local_path, zone, dataset)
            df.to_parquet(path, index=False)

        else:
            key = f"{cls._prefix}/{zone}/{dataset}"
            out_buffer =  BytesIO()
            df.to_parquet(out_buffer, index=False)
            cls._bucket.put_object(Body=out_buffer.getvalue(), Key=key)

    

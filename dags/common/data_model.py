import os
from io import BytesIO, StringIO

import boto3
import pandas as pd

import logging

from typing import List

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
    _local_path = os.path.join(
        os.path.dirname(
            os.path.dirname( 
                os.path.realpath(__file__)
                )
            )
        , "data") 

    @classmethod
    def set_mode(cls, local: bool = True):
        cls.local = local
        cls.work_dir = cls._local_path if cls.local else cls._prefix

        if cls.local == True:
            cls._logger.info(f"Data model is set to local. All files will be stored in {cls._local_path}")
        else:
            cls._logger.info(f"Data model is set to cloud. All files will be store in {cls._bucket}/{cls._prefix}")


    @classmethod
    def set_dir_to_parent(cls):
        """
        Set the working directory the root of the project.
        """
        parentdir = os.path.dirname(cls.work_dir)
        os.chdir(parentdir)

    @classmethod
    def return_zone_path(cls, zone: str):
        return os.path.join(cls.work_dir, zone)

    @classmethod
    def read_csv_from_s3(cls, zone:str, dataset:str):
        key = f"{cls._prefix}/{zone}/{dataset}"
        obj = cls._bucket.Object(key=key).get().get('Body').read().decode("utf-8")
        if not obj:
            raise Exception(f"{key} does not exist")
        data = StringIO(obj)
        df = pd.read_csv(data)
        return df


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


    def _compare_filename_to_partition(x, partition_dates):
        date = x.split("_")[-1].split(".")[0]
        return ( date >= partition_dates[0]) &  ( date < partition_dates[1]) 



    @classmethod
    def read_partitioned_dataframe(cls, zone: str, dataset: str, partition_dates: List[str]):
        """
        Read a parquet file stored in S3 and return a dataframe
        """
        cls._logger.info(f"Reading file {dataset} in {zone} zone")
        if not cls.local:
            raise Exception("Method not implemented yet")
    
        path = os.path.join(cls.work_dir, zone, dataset)

        if not os.path.exists(path):
            raise Exception(f"{path} is not found")

        files = [x for x in os.listdir(path) if cls._compare_filename_to_partition(x, partition_dates)]
        df = pd.concat([pd.read_parquet(os.path.join(path, file)) for file in files], ignore_index=True)
        return df


    @classmethod
    def write_partitioned_dataframe(cls, df: pd.DataFrame, zone: str, dataset: str, partition_column: str):
        """
        Read a parquet file stored in S3 and return a dataframe
        """

        cls._logger.info(f"Writing file {dataset} in {zone} zone")
        if not cls.local:
            raise Exception("Method not implemented yet")

        if partition_column not in df.columns:
            raise Exception("Partition is not in columns")

        path = os.path.join(cls.work_dir, zone, dataset)

        if not os.path.exists(path):
            os.makedirs(path)

        dates = df[partition_column].sort_values().unique().tolist()

        df_list = (df[df[partition_column] == d] for d in dates)

        # save each chunk file within dataset folder
        for i, df_ in enumerate(df_list):

            if len(df_[partition_column]) == 0:
                print(dates[i])
                raise Exception("No rows for date")

            date_str = df_[partition_column].iloc[0].strftime("%Y-%m-%d")
            filename = dataset + "_"  + date_str + ".parquet"
            df_.to_parquet(os.path.join(path, filename))

    

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
from common.data_model import DataModel


class DataLake2PostgresOperator(BaseOperator):
    
    template_fields = ("_start","_end")

    @apply_defaults
    def __init__(self, postgres_conn_id, zone, name, start, end, **kwargs) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.zone  = zone
        self.name = name
        self._start = start
        self._end = end

    def execute(self, context):
        hook = PostgresHook(self.postgres_conn_id)
        engine = hook.get_sqlalchemy_engine()

        data_to_save = DataModel.read_partitioned_dataframe(self.zone, self.name, [self._start, self._end])


        data_to_save.to_sql(
            name = self.name, 
            con = engine,
            schema = "staging",
            if_exists = "replace",
            index = False
        )


class Staging2CuratedOperator(BaseOperator):

    upsert_query = """

        WITH data as (
            select
            *
            from staging.{0}
        )

        , del_matching as (
            DELETE FROM public.{0} AS p
            USING data  d
            WHERE p.{1} = d.{1}
        )

        INSERT INTO public.{0} 
        SELECT
            *
            FROM staging.{0}

        """

    @apply_defaults
    def __init__(self, postgres_conn_id, table, key, **kwargs) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = self.upsert_query.format(table, key)
        


    def execute(self, context) -> None:
        hook = PostgresHook(self.postgres_conn_id)
        hook.run(self.sql)


       





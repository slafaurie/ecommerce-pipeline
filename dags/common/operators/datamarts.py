# generate dag dynamically
import os
import re
from airflow.providers.postgres.operators.postgres import PostgresOperator


class DataMartOperator:
    TARGET_REGEX = r'.*?\"(.*)\"'
    DROP_TARGET = """
    DROP TABLE IF EXISTS datamart.{0};
    """

    def __init__(self, dag, folder, conn_id):
        self.dag = dag
        self.folder = folder
        self.conn_id = conn_id
        # self.sensor = sensor

    def list_files_in_folder(self):
        return [ x for x in os.listdir(self.folder) if ".sql" in x]

    def read_sql_file(self, file):
        with open(os.path.join(self.folder, file), "r") as f:
            content = f.read()
        return content
    
    def get_target_table(self, content):
        target_table = re.findall(self.TARGET_REGEX, content)[0]
        return target_table

    def build_task_query(self, content):
        target_table = self.get_target_table(content)
        return self.DROP_TARGET.format(target_table) + content

    def build_task(self, file):
        content = self.read_sql_file(file)
        task_query = self.build_task_query(content)
        task_id = "task__" + file.split(".")[0]
        task_obj = PostgresOperator(
            postgres_conn_id = self.conn_id,
            task_id = task_id,
            sql=task_query, 
            dag = self.dag
        )
        # task_obj.set_upstream(self.sensor)
        
    def build_tasks(self):
        for f in self.list_files_in_folder():
            self.build_task(f)
        # return self.dag
    

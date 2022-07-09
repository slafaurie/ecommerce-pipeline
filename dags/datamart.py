from airflow import DAG
from datetime import datetime


from common.operators.datamarts import DataMartOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor



default_args = {
    "start_date": datetime(2016,9,5),
    "end_date": datetime(2018,10,20),
    "owner": "Airflow" ,
    "wait_for_downstream":True
}


with DAG(
        dag_id="datamart-dags", 
        schedule_interval= "@daily",
        default_args=default_args, 
        catchup=True,
        user_defined_macros={
            "earliest_date":datetime(2016,9,1).strftime("%Y-%m-%d")
        }
    ) as dag:

    external_task = ExternalTaskSensor(
        task_id="curated-sensor",
        external_dag_id="curated-dags",
        external_task_id= "end-curated-dags"
    )

    factory = DataMartOperator(
        conn_id = "postgres_ecommerce",
        dag = dag,
        folder = "dags/datamarts",
        sensor=external_task
    )

    factory.build_tasks()
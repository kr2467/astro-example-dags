

from airflow.decorators import dag
from astro import sql as aql
import pandas as pd
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime



@aql.dataframe(task_id="python_1")
def python_1_func():
    def hello_world():
        print("Hello World")
    
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2024, 3, 2),
        'retries': 1,
    }
    with DAG(
        dag_id='hello_world_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
    ) as dag:
    
        task1 = PythonOperator(
            task_id='hello_world_task',
            python_callable=hello_world,
        )
    
        task1
    

default_args={
    "owner": "kotharao007@gmail.com,Open in Cloud IDE",
}

@dag(
    default_args=default_args,
    schedule="0 0 * * *",
    start_date=pendulum.from_format("2025-02-03", "YYYY-MM-DD").in_tz("UTC"),
    catchup=False,
    owner_links={
        "kotharao007@gmail.com": "mailto:kotharao007@gmail.com",
        "Open in Cloud IDE": "https://cloud.astronomer.io/cm6nrxac710ic01mdl67d3edp/cloud-ide/cm6nt60h410os01mdkmbfebqs/cm6oo2jip157q01mdyenmxm7k",
    },
)
def hello_world():
    python_1 = python_1_func()

dag_obj = hello_world()

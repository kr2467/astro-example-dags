from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

with DAG('dummy_operator',
        start_date=datetime(2025,3,2),
        description='a dummy operator',
        tags=['data'],schedule='@daily',
        catchup=False
        )as dag:
    start_task=DummyOperator(
        task='start',
        dag=dag
        )
    end_operator=DummyOperator(
        task_id='end',
        dag=dag
        )
    start_task>>end_operator

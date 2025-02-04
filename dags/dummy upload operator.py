from airflow import DAG
from datetime import datetime
from airflow.dummy_operator import DummyOperator

with DAG(
        'local_to_s3',
        schedule='@once',
        tags=['uploading'],
        description='Uploading a file from local system to S3',
        start_date=datetime(2025, 4, 2),
        catchup=False  
        ) as dag:
    
    start = DummyOperator(
        task_id='start_task',
    )
    
    upload = DummyOperator(
        task_id='uploading_task',  
    )

    end = DummyOperator(
        task_id='end_task',
    )

    # Defining task dependencies
    start >> upload >> end

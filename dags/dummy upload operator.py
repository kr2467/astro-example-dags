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
    
    upload = DummyOperator(
        task_id='uploading_task',
        file_path=r'C:\Users\kotha.rao\Desktop\Boot camp\IPL_Ball_by_Ball_2008_2022.csv',
        bucket_name='knrbucket',
        object_key='data/IPL_Ball_by_Ball_2008_2022.csv', 
        aws_conn_id='aws_default',
    )

    upload

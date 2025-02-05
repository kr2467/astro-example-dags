from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.transfers.s3 import S3FileOperator

with DAG(
        'local_to_s3',
        schedule='@daily',
        tags=['uploading'],
        description='Uploading a file from local system to S3',
        start_date=datetime(2025, 4, 2),
        catchup=False  
        ) as dag:
    
    upload = S3FileOperator(
        task_id='uploading_task',
        file_path=r'C:\Users\kotha.rao\Desktop\Boot camp\IPL_Ball_by_Ball_2008_2022.csv',
        bucket_name='knrbucket',
        object_key='data/IPL_Ball_by_Ball_2008_2022.csv', 
        aws_conn_id='aws_default',
    )

    upload

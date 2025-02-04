from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFileSystemToS3Operator

with DAG(
        'local_to_s3',
        schedule='@once',
        tags=['uploading'],
        description='uploading the local system to s3',
        start_date=datetime(2025,4,2),
        retries=1
        ) as dag:
    upload=LocalFileSystemToS3Operator(
        task_id='uploading_task',
        filename=r'C:\Users\kotha.rao\Desktop\Boot camp\IPL_Ball_by_Ball_2008_2022.csv',
        bucket_name='knrbucket',
        key='arn:aws:s3:::knrbucket',
        aws_conn_id='aws_default'        
        )
    upload

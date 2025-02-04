from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.transfers.local_to_s3 import S3FileTransferOperator  

with DAG(
        'local_to_s3',
        schedule='@once',
        tags=['uploading'],
        description='Uploading a file from local system to S3',
        start_date=datetime(2025, 4, 2),
        catchup=False  # Avoid backfilling
        ) as dag:
    
    upload = S3FileTransferOperator(
        task_id='uploading_task',
        filename=r'C:\Users\kotha.rao\Desktop\Boot camp\IPL_Ball_by_Ball_2008_2022.csv',
        bucket_name='knrbucket',
        dest_key='data/IPL_Ball_by_Ball_2008_2022.csv',  
        aws_conn_id='aws_default',
    )

    upload

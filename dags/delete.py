from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator

with DAG(
    'delete_file',
    schedule_interval='@once',
    start_date=datetime(2025, 4, 2),
    catchup=False
) as dag:

    delete = S3DeleteObjectsOperator(
        task_id='delete_operator',
        bucket_name='triggerbucketobject',
        keys=['IPL_Matches_2008_2022.csv'],
        aws_conn_id='aws_default'
    )

delete  

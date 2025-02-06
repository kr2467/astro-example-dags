from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3FileSensor
from datetime import datetime

with DAG('s3_glue_dynamodb',
         start_date=datetime(2025,1,30),
         description='Automation of s3 to dynamodb',
         tags=['Air flow'],
         schedule='@daily',catchup=False) as dag:
    
    s3_sensor=S3FileSensor(
        task_id='s3_file_sensor',
        bucket_name='triggerbucketobject',
        aws_conn_id='aws_default',
        poke_interval=60,
        time_out=60,
        dag=dag
        )
    

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime

with DAG('s3_glue_dynamodb',
         start_date=datetime(2025, 1, 30),
         description='Automation of s3 to dynamodb',
         tags=['Air flow'],
         schedule_interval='@daily',
         catchup=False) as dag:
    
    s3_sensor = S3KeySensor(
        task_id='s3_file_sensor',
        bucket_name='triggerbucketobject',
        bucket_key='path/to/your/file.txt',  # Specify the key of the file you're waiting for
        aws_conn_id='aws_default',
        poke_interval=60,
        timeout=60,
        dag=dag
    )

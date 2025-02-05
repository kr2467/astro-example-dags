from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import boto3
import pandas as pd

def read_file(bucket_name, file_key, **kwargs):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read().decode('utf-8')
    print(file_content)

with DAG(
    'testing_a_file',
    schedule_interval='@once',
    start_date=datetime(2025, 5, 2),
    catchup=False
) as dag:
    
    source_bucket = 'knrbucket'
    file_key = "IPL_Ball_by_Ball_2008_2022.csv"

    reading_file = PythonOperator(
        task_id='reading_file_from_s3',
        python_callable=read_file,
        op_args=[source_bucket, file_key],
        provide_context=True
    )

  

    reading_file 

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def read_s3_file(**kwargs):
    s3_hook = S3Hook(aws_conn_id='s3_conn')
    s3_key = 'IPL_Ball_by_Ball_2008_2022.csv'
    bucket_name = 'knrbucket'
    
    file_content = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)
    print(file_content)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 5),
    'retries': 1,
}

dag = DAG(
    'read_s3_file_dag',
    default_args=default_args,
    description='A simple DAG to read a file from S3',
    schedule_interval='@daily',
)

read_file_task = PythonOperator(
    task_id='read_s3_file_task',
    python_callable=read_s3_file,
    dag=dag,
)

read_file_task

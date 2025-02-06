from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def read_s3_file(**kwargs):
    s3 = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'knrbucket'
    key = 'IPL_Ball_by_Ball_2008_2022.csv'
    file_content = s3.read_key(key, bucket_name)
    print(file_content)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('s3_read_dag', default_args=default_args, schedule_interval='@daily') as dag:
    read_file = PythonOperator(
        task_id='read_s3_file',
        python_callable=read_s3_file,
        provide_context=True,
    )

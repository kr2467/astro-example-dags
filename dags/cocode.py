from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from io import StringIO

def read_s3_file(**kwargs):
    s3 = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'knrbucket'
    key = 'IPL_Ball_by_Ball_2008_2022.csv'
    file_content = s3.read_key(key, bucket_name)
    return file_content

def transform_data(**kwargs):
    ti = kwargs['ti']
    file_content = ti.xcom_pull(task_ids='read_s3_file')
    
    df = pd.read_csv(StringIO(file_content))
    
    result = df.groupby(['ID', 'batter'])['total_run'].sum().reset_index().rename(columns={'ID': 'MATCH_ID', 'batter': 'BATSMAN_NAME', 'total_run': 'TOTAL_RUNS'})
    
    csv_buffer = StringIO()
    result.to_csv(csv_buffer, index=False)
    
    ti.xcom_push(key='transformed_data', value=csv_buffer.getvalue())

def store_transformed_data(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    
    s3 = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'knrbucket'
    transformed_key = 'transformed/IPL_Ball_by_Ball_Transformed.csv'
    s3.load_string(transformed_data, transformed_key, bucket_name, replace=True)
    print("Transformation complete and stored in S3")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 2),
}

with DAG('s3_read_transform_store_dag', default_args=default_args, schedule_interval='@daily') as dag:
    
    read_file = PythonOperator(
        task_id='read_s3_file',
        python_callable=read_s3_file,
        provide_context=True,
    )
    
    transform_file = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )
    
    store_file = PythonOperator(
        task_id='store_transformed_data',
        python_callable=store_transformed_data,
        provide_context=True,
    )

    read_file >> transform_file >> store_file

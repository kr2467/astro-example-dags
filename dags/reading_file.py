from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import boto3

def read_file_from_s3(bucket_name, file_key):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read()
    return file_content

def upload_file_to_s3(file_content, bucket_name, file_key):
    s3_client = boto3.client('s3')
    new_file_key = f"processed/{file_key}"  
    s3_client.put_object(Bucket=bucket_name, Key=new_file_key, Body=file_content)
    print(f"File uploaded to {bucket_name}/{new_file_key}")

# Define the DAG
with DAG(
    'retrieving_and_uploading_file_same_bucket',
    schedule='@once',
    start_date=datetime(2025, 5, 2),
    catchup=False,
    tags=['s3', 'file_operations']
) as dag:

    source_bucket = 'knrbucket' 
    file_key = 'IPL_Ball_by_Ball_2008_2022.csv'  

    reading_file_task = PythonOperator(
        task_id='read_file_from_s3',
        python_callable=read_file_from_s3,
        op_args=[source_bucket, file_key],
        provide_context=True  
    )

    upload_file_task = PythonOperator(
        task_id='upload_file_to_s3',
        python_callable=upload_file_to_s3,
        op_args=[
            '{{ task_instance.xcom_pull(task_ids="read_file_from_s3") }}',  
            source_bucket, 
            file_key
        ]
    )


    reading_file_task >> upload_file_task

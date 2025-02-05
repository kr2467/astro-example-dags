from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import boto3
from botocore.exceptions import ClientError

def check_file_exists(bucket_name, file_key, **kwargs):
    s3_client = boto3.client('s3')
    try:
        # Try to fetch metadata of the file using head_object
        s3_client.head_object(Bucket=bucket_name, Key=file_key)
        print("Yes, file exists.")
    except ClientError as e:
        # If the file doesn't exist, it will raise a ClientError
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("No, file does not exist.")
        else:
            # Re-raise the exception if it's another kind of error
            raise e

with DAG(
    'check_file_existence',
    schedule_interval='@once',
    start_date=datetime(2025, 1, 2),
    catchup=False
) as dag:
    
    source_bucket = 'knrbucket'
    file_key = "IPL_Ball_by_Ball_2008_2022.csv"

    check_file = PythonOperator(
        task_id='check_file_exists_in_s3',
        python_callable=check_file_exists,
        op_args=[source_bucket, file_key],
        provide_context=True
    )

    check_file

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from datetime import datetime
from airflow.operators.dummy import DummyOperator

with DAG(
        'copy_an_object',
        start_date=datetime(2025, 2, 2),
        schedule_interval='@once',
        catchup=False
        ) as dag:
    
    copy_object = S3CopyObjectOperator(
        task_id='copy_from_one_bucket_to_another_bucket',
        source_bucket_name='knrbucket',
        source_bucket_key='IPL_Ball_by_Ball_2008_2022.csv',  # Corrected argument
        dest_bucket_name='triggerbucketobject',
        dest_bucket_key='IPL_Ball_by_Ball_2008_2022_copy.csv',  # Corrected argument
        aws_conn_id='aws_default'  # Corrected to aws_conn_id
    )
    
    end = DummyOperator(task_id='end')  # Specify task_id for DummyOperator
    
    copy_object >> end  # Task dependency

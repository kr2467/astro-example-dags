from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from datetime import datetime
from airflow.operators import DummyOperator

with DAG(
        'copy_an_object',
        start_date=datetime(2025,2,2),
        schedule='@once',
        catchup=False
        ) as dag:
    
    copy_object=S3CopyObjectOperator(
        task_id='copy_from_one_bucket_to_another_bucket',
        source_bucket_name='knrbucket',
        key='IPL_Ball_by_Ball_2008_2022.csv',
        dest_bucket='triggerbucketobject',
        aws_conn='aws_default'
    )
    end=DummyOperator('end')
    
    copy_object>>end

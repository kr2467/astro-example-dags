from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG('glue_job_dag', default_args=default_args, schedule_interval='@once') as dag:
    check_s3_file = S3KeySensor(
        task_id='check_s3_file',
        bucket_name='triggerbucketobject',
        bucket_key='IPL_Ball_by_Ball_2008_2022.csv ',
        aws_conn_id='aws_default',
        timeout=20,
        poke_interval=60,  
    )

    glue_task = GlueJobOperator(
        task_id='run_glue_job',
        job_name='new_project',
        aws_conn_id='aws_default',
        region_name='us-east-1',
        iam_role_name='narasimha_glue_role', 
        num_of_dpus=1, 
        wait_for_completion=True,
    )

    check_s3_file >> glue_task

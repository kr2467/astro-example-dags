from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG(
        'test_sensors',
        default_args=default_args,
        schedule_interval='@once',
        description='A DAG that triggers a Glue job after checking for a file in S3',
        catchup=False
    ) as dag:

    # Sensor to check for the presence of the file in S3
    s3_sensor = S3KeySensor(
        task_id='checking_file_existence',
        bucket_name='triggerbucketobject',
        bucket_key='IPL_Ball_by_Ball_2008_2022.csv',
        aws_conn_id='aws_default',
        poke_interval=60,
        timeout=1000
    )

    # Glue job operator to run the Glue job
    glue_task = GlueJobOperator(
        task_id='run_glue_job',
        job_name='new_project',  
        aws_conn_id='aws_default',
        region_name='us-east-1',
        iam_role_name='narasimha_glue_role', 
        num_of_dpus=1,  
        wait_for_completion=True,
    )

    # Dummy operator to mark the end of the DAG
    end_task = DummyOperator(
        task_id='end_task'
    )

    # Set the task dependencies
    s3_sensor >> glue_task >> end_task

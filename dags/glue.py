from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG('glue_job_dag', default_args=default_args, schedule_interval='@once') as dag:
    glue_task = GlueJobOperator(
        task_id='run_glue_job',
        job_name='new_project',  
        aws_conn_id='aws_default',
        region_name='us-east-1',
        iam_role_name='narasimha_glue_role',  
        num_of_dpus=1,  # Mandatory DPUs
        wait_for_completion=True,
    )

    glue_task

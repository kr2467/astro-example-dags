from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.glue import AwsGlueJobHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def check_glue_job_status(job_name, aws_conn_id):
    hook = AwsGlueJobHook(aws_conn_id=aws_conn_id)
    job_runs = hook.get_job_runs(job_name)
    for job_run in job_runs['JobRuns']:
        if job_run['JobRunState'] in ['STARTING', 'RUNNING']:
            return False
    return True

with DAG('glue_job_dag', default_args=default_args, schedule_interval='@daily') as dag:
    glue_task = GlueJobOperator(
        task_id='run_glue_jobbbbb',
        job_name='new_project',
        aws_conn_id='aws_default',
        region_name='us-east-1',
        iam_role_name='narasimha_glue_role',
        num_of_dpus=10,
        wait_for_completion=True,
        execution_timeout=timedelta(hours=2),
        on_failure_callback=lambda context: print("Job failed"),
        on_retry_callback=lambda context: print("Retrying job"),
        on_success_callback=lambda context: print("Job succeeded"),
        pre_execute=lambda context: check_glue_job_status('your_glue_job_name', 'aws_default')
    )

    glue_task

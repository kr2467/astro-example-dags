from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 31),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'simple_dummy_dag',
    default_args=default_args,
    description='A simple DAG with a DummyOperator',
    schedule_interval='@daily',
)

# Define the tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set the task dependencies
start >> end

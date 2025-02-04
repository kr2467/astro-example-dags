from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

with DAG(
        'task_groups',
        schedule='@once',
        start_date=datetime(2025,4,2),
        catchup=False
        ) as dag:
    start = DummyOperator(
        task_id='starting',
    )
    
    # Task Group for extract tasks
    with TaskGroup("extract_task") as extract_tasks:
        extract_data = DummyOperator(task_id='extract_data')
        extract_data1 = DummyOperator(task_id='extract_data1')
    
    # Task Group for transform tasks
    with TaskGroup("transform_tasks") as transform_groups:
        transform_data = DummyOperator(task_id='transform_data')
        transform_data1 = DummyOperator(task_id='transform_data2')  
    
    end = DummyOperator(task_id='end')
    
    # Defining the task dependencies
    start >> extract_tasks >> transform_groups >> end

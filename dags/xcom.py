from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def push_value_to_xcom(**kwargs):
    kwargs['ti'].xcom_push(key='my_number',value=42)
    
def pull_value_from_xcom(**kwargs):
    ti=kwargs['ti']
    value=ti.xcom_pull(task_id='task_push_value',key='my_number')
    print(f"pulled valued from xcom :{value}")
    
with DAG(
        'xcom_execution',
        schedule='@once',
        start=datetime(2025,4,2),
        catchup=False
        ) as dag:
    start=DummyOperator('start')
    
    task_push_value=PythonOperator(
        task_id='push_value',
        python_callable=push_value_to_xcom,
        provide_context=True
        )
    task_pull_value=PythonOperator(
        task_id='pull_value',
        python_callable=pull_value_from_xcom,
        provide_context=True
        )
    end=DummyOperator('end')
    
    start>>task_push_value>>task_pull_value>>end

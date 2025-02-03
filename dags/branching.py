from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import BranchPythonOperator
from airflow.opertors.dummy_operators import DummyOperator


def choose_branch(**kwargs):
    day_of_week=datetime.now().weekday()
    
    if day_of_week<5:
        return 'branch_1'
    else:
        return 'branch_2'

with DAG('simple_dag',
         start_date=datetime(2025,3,2),
         title='a simple dag run',
         schedule='@daily',
         catchup=False
         ) as dag:
    start_task=DummyOperator(
        task_id='start',
        dag=dag
        )
    branching=BranchPythonOperator(
        task_id='branching',
        python_callable=choose_branch,
        provide_context=True
        )
    branch_1=DummyOperator(
        task_id='branch_1',
        dag=dag
        )
    branch_2=DummyOperator(
        task_id='branch_2',
        dag=dag
        )
    end_task=DummyOperator(
        task_id='end',
        dag=dag
        )
    start_task>>branching>>[branch_1,branch_2]
    branch_1>>end_task
    branch_2>>end_task
    
    
    

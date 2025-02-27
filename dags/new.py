from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

def choose_branch(**kwargs):
    day_of_week = datetime.now().weekday()
    
    if day_of_week < 5:
        return 'branch_1'
    else:
        return 'branch_2'

with DAG('simpleee_dag',
         start_date=datetime(2025, 3, 2),
         schedule_interval='@daily',
         catchup=False) as dag:

    start_task = DummyOperator(
        task_id='startt',
        dag=dag
    )
    
    branching = BranchPythonOperator(
        task_id='branchingg',
        python_callable=choose_branch
    )
    
    branch_1 = DummyOperator(
        task_id='branchh_1',
        dag=dag
    )
    
    branch_2 = DummyOperator(
        task_id='branchh_2',
        dag=dag
    )
    
    end_task = DummyOperator(
        task_id='endd',
        dag=dag
    )
    
    start_task >> branching >> [branch_1, branch_2]
    branch_1 >> end_task
    branch_2 >> end_task

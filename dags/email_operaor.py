from airflow import DAG
from airflow.operators.email import EmailOperator
from datetime import datetime

with DAG('send_email_example',
         start_date=datetime(2025, 2, 4),  
         description='Simple Email Sending DAG',
         schedule_interval='@once', 
         catchup=False) as dag:
    send_email = EmailOperator(
        task_id='send_email',
        to='kotharao007@gmail.com',  
        subject='Airflow Task Completed Successfully!',
        html_content='This is an automated email from Airflow.',
        email_on_failure=True 
    )

    send_email

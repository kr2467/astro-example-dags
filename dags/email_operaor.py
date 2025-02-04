from airflow import DAG
from airflow.operators.email import EmailOperator
from datetime import datetime

with DAG('s3_glue_dynamodb',
         start_date=datetime(2025,2,4),
         description='Automation of s3 to dynamodb',
         tags=['Air flow'],
         schedule='@once', catchup=False) as dag:
    
    send_email = EmailOperator(
        task_id='send_email_on_success',
        to='kotharao007@gmail.com',
        subject='Glue job ran successfully and data is stored in the dynamo db',
        html_content='The transformation of the data has been completed and the data is stored in DynamoDB successfully'
    )

    send_email

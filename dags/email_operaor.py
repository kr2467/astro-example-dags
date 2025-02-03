from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3FileSensor
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.dynamodb import DynamoDBOperator
from airflow.operators.email import EmailOperator
from datetime import datetime

with DAG('s3_glue_dynamodb',
         start_date=datetime(2025,1,30),
         description='Automation of s3 to dynamodb',
         tags=['Air flow'],
         schedule='@daily',catchup=False) as dag:
             
             send_email=EmailOperator(
        task_id='send_email_on_success',
        to='kotharao007@gmail.com',
        cc='projectmanager@gmail.com',
        subject='Glue job ran successfully and data is stored in the dynamo db',
        html_content='The tranformation of the data has been completed and the data is stored in the dynamodb successfully',
        dag=dag
        )
        
        send_email

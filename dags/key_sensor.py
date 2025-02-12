from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.operators import DummyOperator
from datetime import datetime
from airflow import DAG


with DAG(
        's3_key_sensors',
        start_date=datetime(2025,2,10),
        schedule='@once',
        poke_interval=60,
        description='Atrail of filesensor'
        ) as dag:
    s3_sensor=S3KeySensor(
        task_id='checking_file_existence',
        bucket_name='triggerbucketobject',
        bucket_key='IPL_Ball_by_Ball_2008_2022.csv',
        poke_interval=60
        
        )
    end_task=DummyOperator(
        'end_task'
        )
    s3_sensor>>end_task

import pandas as pd
import endpoints
from datetime import datetime, timedelta
from config import MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, SITE_EMAIL, SITE_PASSWORD
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import boto3
from sqlalchemy import create_engine
import pandas as pd
from io import BytesIO
import time

def main():
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    today_str = today.strftime('%Y-%m-%d')
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    BUCKET_NAME  = "airflow-bucket"
    KEY_LOAD = f"data/info_subjects.csv"

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        region_name="us-east-1"
    )

    session = endpoints.get_session()
    cookie = endpoints.get_cookies()

    data = endpoints.get_info_subjects(yesterday_str, today_str, session, cookie)
    df = pd.json_normalize(data['items'])

    # Сохранение DataFrame в CSV в памяти
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)  # Переместить указатель в начало буфера

    # Загрузка файла в MinIO
    try:
            s3.upload_fileobj(
                Fileobj=csv_buffer,
                Bucket=BUCKET_NAME,
                Key=KEY_LOAD,
                ExtraArgs={'ContentType': 'text/csv'}
            )
            print(f"File successfully uploaded to MinIO: {BUCKET_NAME}/{KEY_LOAD}")
    except Exception as e:
            print(f"Error uploading to MinIO: {e}")
            raise

args = {
    "email": [],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "params": {"priority": "P0"},
}

@dag(
    dag_id="get_info_subjects_days_dag",
    schedule=None,
    start_date=datetime(year=2025, month=8, day=26),
    catchup=False,
    dagrun_timeout=timedelta(minutes=59),
    default_args=args,
    tags=["TEST"],
)
def my_dag():
    @task
    def process():
        main()
    process()
my_dag()
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

def main():

    today = datetime.today()
    yesterday = today - timedelta(days=1)
    today_str = today.strftime('%Y-%m-%d')
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    BUCKET_NAME  = "airflow-bucket"
    KEY = f"data/l1_l2_data.csv"

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        region_name="us-east-1"
    )

    cookies_dict = endpoints.get_cookies(SITE_EMAIL, SITE_PASSWORD)
    session = endpoints.get_session()
    data = endpoints.get_l1_l2(yesterday_str, today_str, session, cookies_dict)

    df = pd.json_normalize(data['data'])
    df.rename({'id': 'l1_id', 'value': 'l1_name'}, axis=1, inplace=True)
    df = df[['l1_id', 'l1_name', 'data']]\
        .explode('data')\
        .reset_index(drop=True)
    df = df.drop(0)\
        .reset_index(drop=True)
    df_expanded = pd.json_normalize(df['data'])
    df = pd.concat([df[['l1_id', 'l1_name']], df_expanded], axis=1)\
        .rename({'id': 'l2_id', 'value': 'l2_name'}, axis=1)
    df = df[['l1_id', 'l1_name', 'l2_id', 'l2_name']]

    # Сохранение DataFrame в CSV в памяти
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)  # Переместить указатель в начало буфера

    # Загрузка файла в MinIO
    try:
        s3.upload_fileobj(
            Fileobj=csv_buffer,
            Bucket=BUCKET_NAME,
            Key=KEY,
            ExtraArgs={'ContentType': 'text/csv'}
        )
        print(f"File successfully uploaded to MinIO: {BUCKET_NAME}/{KEY}")
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
    dag_id="get_l1_l2_dag",
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
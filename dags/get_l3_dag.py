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
    KEY_READ = f"data/{today_str}/l1_l2_data.csv"
    KEY_LOAD = f"data/{today_str}/l3_data.csv"


    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        region_name="us-east-1"
    )

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY_READ)
    df = pd.read_csv(obj)

    cookies_dict = endpoints.get_cookies(SITE_EMAIL, SITE_PASSWORD)
    session = endpoints.get_session()

    temp_dfs = []
    for l2_id in df['l2_id']:
        data = endpoints.get_l3(yesterday_str, l2_id, today_str, session, cookies_dict)
        df_temp = pd.json_normalize(data['data'])
        df_temp['l2_id'] = l2_id
        temp_dfs.append(df_temp)
        time.sleep(1)

    if temp_dfs:
        df_l3 = pd.concat(temp_dfs, ignore_index=True)
        df_l3 = df_l3.rename(columns={'id': 'l3_id', 'name': 'l3_name'})\
            .drop(['url', 'path', 'isFavorite'], axis=1)
    else:
        df_l3 = pd.DataFrame()

    # Сохранение DataFrame в CSV в памяти
    csv_buffer = BytesIO()
    df_l3.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)  # Переместить указатель в начало буфера

    # Загрузка файла в MinIO
    try:
        s3.upload_fileobj(
            Fileobj=csv_buffer,
            Bucket=BUCKET_NAME,
            Key=KEY_LOAD,
            ExtraArgs={'ContentType': 'text/csv'}
        )
        print(f"Файл успешно загружен в MinIO: {BUCKET_NAME}/{KEY_LOAD}")
    except:
        print(f"Ошибка при загрузке в MinIO")

args = {
    "email": [],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "params": {"priority": "P0"},
}

@dag(
    dag_id="get_l3_dag",
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
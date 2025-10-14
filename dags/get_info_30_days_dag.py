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
    KEY_READ = f"data/l3_data_mini.csv"
    KEY_LOAD = f"data/{today_str}/info_30_days.csv"

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        region_name="us-east-1"
    )

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY_READ)
    df = pd.read_csv(obj['Body'])
    df = df[['l3_id']].drop_duplicates()
    df = df[['l3_id']].astype(str)

    cookies_dict = endpoints.get_cookies(SITE_EMAIL, SITE_PASSWORD)
    session = endpoints.get_session()

    temp_dfs = []
    lenght = len(df['l3_id'])
    count = 0
    for l3_id in df['l3_id']:
        count+=1
        if count % 10 == 0:
            print(f'{count}/{lenght}')
        data = endpoints.get_info_30_days(yesterday_str, l3_id, today_str, session, cookies_dict)
        df_temp = pd.json_normalize(data)
        df_temp = df_temp[['name', 'trend']]
        df_temp['l3_id'] = l3_id
        # Шаг 1: Разворачиваем список в столбце trend
        df_exploded = df_temp.explode('trend')

        # Шаг 2: Нормализуем словари в столбце trend
        trend_df = pd.json_normalize(df_exploded['trend'])

        # Шаг 3: Объединяем развернутые данные с исходным DataFrame
        df_exploded = df_exploded.drop(columns=['trend']).reset_index(drop=True)
        df_temp = pd.concat([df_exploded, trend_df], axis=1)

        temp_dfs.append(df_temp)
        time.sleep(1)

    print('FINISH')

    if temp_dfs:
        df_info = pd.concat(temp_dfs, ignore_index=True)
    else:
        df_info = pd.DataFrame()

    # Сохранение DataFrame в CSV в памяти
    csv_buffer = BytesIO()
    df_info.to_csv(csv_buffer, index=False)
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
    dag_id="get_info_30_days_dag",
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
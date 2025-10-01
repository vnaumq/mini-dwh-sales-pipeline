import asyncio
import aiohttp
import pandas as pd
import json
from urllib.parse import quote
from datetime import datetime, timedelta
import boto3
from config import MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, SITE_EMAIL, SITE_PASSWORD
from io import BytesIO
from airflow.sdk import dag, task
import endpoints


async def process(yesterday_str, subject_id, today_str, session, cookies_dict):
    data = await endpoints.get_async_info_subjects_30_days(yesterday_str, subject_id, today_str, session, cookies_dict)
    if not data:
        return pd.DataFrame()

    df_temp = pd.json_normalize(data)
    if df_temp.empty:
        return pd.DataFrame()

    df_temp = df_temp[['name', 'trend']]
    df_temp['subject_id'] = subject_id
    df_exploded = df_temp.explode('trend')
    trend_df = pd.json_normalize(df_exploded['trend'])
    df_exploded = df_exploded.drop(columns=['trend']).reset_index(drop=True)
    df_temp = pd.concat([df_exploded, trend_df], axis=1)

    return df_temp


async def main():

    today = datetime.today()
    yesterday = today - timedelta(days=1)
    today_str = today.strftime('%Y-%m-%d')
    yesterday_str = yesterday.strftime('%Y-%m-%d')

    BUCKET_NAME = "airflow-bucket"
    KEY_READ = f"data/info_subjects.csv"
    KEY_LOAD = f"data/{today_str}/info_subject_30_days.csv"

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        region_name="us-east-1"
    )

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY_READ)
    df = pd.read_csv(obj['Body'])
    df[['subject_id']] = df[['subjectId']]
    df = df[['subject_id']].drop_duplicates()
    df = df.head(1000)

    cookies_dict = endpoints.get_cookies(SITE_EMAIL, SITE_PASSWORD)

    async with aiohttp.ClientSession() as session:
        tasks = [
            process(yesterday_str, subject_id, today_str, session, cookies_dict)
            for subject_id in df['subject_id']
        ]
        results = await asyncio.gather(*tasks)

    temp_dfs = [res for res in results if not res.empty]
    print('FINISH')

    if temp_dfs:
        df_info = pd.concat(temp_dfs, ignore_index=True)
    else:
        df_info = pd.DataFrame()

    csv_buffer = BytesIO()
    df_info.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

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


def run():
    asyncio.run(main())

args = {
    "email": [],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "params": {"priority": "P0"},
}

@dag(
    dag_id="get_info_subjects_30_days_async_dag",
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
        run()
    process()
my_dag()

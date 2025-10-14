import pandas as pd
import json
from urllib.parse import quote
from datetime import datetime, timedelta
import boto3
from io import BytesIO
import time

from airflow.sdk import dag, task

from config import MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, SITE_EMAIL, SITE_PASSWORD
import endpoints


def fetch_subject_products_30_days(check_dt: str, subject_id: int, today: str, session, cookies: dict):

    cache = endpoints.cache_subjects(subject_id, session, cookies)

    time.sleep(2)

    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; DataCollector/1.0)"
    })

    query_params = {
        "start": 0,
        "length": 0,
        "checkDate": check_dt,
        "periodDays": "30",
        "trendType": "day",
        "filters": {
            "showFavoritesOnly": {"value": False},
            "showMyProducts": {"value": False},
            "showNewProducts": {"value": False}
        }
    }

    encoded_query = quote(json.dumps(query_params))
    url = f"https://eggheads.solutions/analytics/wbSubject/getProductsList/{subject_id}.json?query={encoded_query}&dns-cache={today}_09-1"

    response = session.get(url, cookies=cookies)
    if response.status_code == 200:
        data = response.json()
        return data.get("totals", [])
    else:
        print(response.text)
        print(url)
        return []


def main():
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

    # Согласуем поля с async-версией: берем subjectId -> subject_id, сортировка по orders и топ-5
    if 'subject_id' not in df.columns and 'subjectId' in df.columns:
        df[['subject_id']] = df[['subjectId']]
    if 'orders' in df.columns:
        df.sort_values('orders', inplace=True)
    df = df[['subject_id']].drop_duplicates()
    df = df.head(5)

    cookies_dict = endpoints.get_cookies(SITE_EMAIL, SITE_PASSWORD)
    session = endpoints.get_session()

    temp_dfs = []
    total = len(df['subject_id'])
    processed = 0

    for subject_id in df['subject_id']:
        processed += 1
        if processed % 5 == 0 or processed == total:
            print(f"{processed}/{total}")

        totals = fetch_subject_products_30_days(yesterday_str, int(subject_id), today_str, session, cookies_dict)
        if not totals:
            time.sleep(1)
            continue

        df_temp = pd.json_normalize(totals)
        if df_temp.empty:
            time.sleep(1)
            continue

        df_temp = df_temp[['name', 'trend']]
        df_temp['subject_id'] = subject_id

        df_exploded = df_temp.explode('trend')
        trend_df = pd.json_normalize(df_exploded['trend'])
        df_exploded = df_exploded.drop(columns=['trend']).reset_index(drop=True)
        df_temp = pd.concat([df_exploded, trend_df], axis=1)

        temp_dfs.append(df_temp)
        time.sleep(1)

    print('FINISH')

    if temp_dfs:
        df_info = pd.concat(temp_dfs, ignore_index=True)
    else:
        df_info = pd.DataFrame()

    csv_buffer = BytesIO()
    print(df_info.shape)
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


args = {
    "email": [],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "params": {"priority": "P0"},
}


@dag(
    dag_id="get_info_subjects_30_days_dag",
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

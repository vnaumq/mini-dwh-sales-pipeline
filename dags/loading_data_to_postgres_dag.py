import asyncio
import aiohttp
import pandas as pd
import json
from urllib.parse import quote
from datetime import datetime, timedelta
import boto3
from config import MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
from io import BytesIO
from airflow.sdk import dag, task
import config

def season_ratio():

    BUCKET_NAME  = "airflow-bucket"
    KEY = f"data/season_ratio.csv"
    SCHEMA_NAME = 'superset'
    TABLE_NAME = 'season_ratio'

    s3 = config.get_s3_client()

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY)
    df = pd.read_csv(obj['Body'])

    df['dt'] = pd.to_datetime(df['date'])
    df.drop(['date'], axis=1, inplace=True)
    df = df[['dt', 'subject_id', 'coefficient']]

    engine = config.get_engine_postgresql()

    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="replace", index=False)
    print(f"✅ Загружено {len(df)} строк в таблицу {TABLE_NAME}")
    print("SUCCESS")

def info_subjects():

    BUCKET_NAME  = "airflow-bucket"
    KEY = f"data/info_subjects.csv"
    SCHEMA_NAME = 'superset'
    TABLE_NAME = 'info_subjects'

    s3 = config.get_s3_client()

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY)
    df = pd.read_csv(obj['Body'])
    df = df[['subjectId', 'name', 'position']]
    engine = config.get_engine_postgresql()

    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="replace", index=False)
    print(f"✅ Загружено {len(df)} строк в таблицу {TABLE_NAME}")
    print("SUCCESS")

def info_subject_30_days():

    BUCKET_NAME  = "airflow-bucket"
    KEY = f"data/info_subject_30_days.csv"
    SCHEMA_NAME = 'superset'
    TABLE_NAME = 'info_subjects_30_days'

    s3 = config.get_s3_client()

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY)
    df = pd.read_csv(obj['Body'])
    df['dt'] = pd.to_datetime(df['date'])
    df.drop(['date'], axis=1, inplace=True)
    df = df[['subject_id', 'name', 'orders', 'ordersSum']]
    engine = config.get_engine_postgresql()

    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="replace", index=False)
    print(f"✅ Загружено {len(df)} строк в таблицу {TABLE_NAME}")
    print("SUCCESS")


def l1_l2():

    BUCKET_NAME  = "airflow-bucket"
    KEY = f"data/l1_l2_data.csv"
    SCHEMA_NAME = 'superset'
    TABLE_NAME = 'l1_l2'

    s3 = config.get_s3_client()

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY)
    df = pd.read_csv(obj['Body'])

    engine = config.get_engine_postgresql()

    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="replace", index=False)
    print(f"✅ Загружено {len(df)} строк в таблицу {TABLE_NAME}")
    print("SUCCESS")

def l3():

    BUCKET_NAME  = "airflow-bucket"
    KEY = f"data/l3_data.csv"
    SCHEMA_NAME = 'superset'
    TABLE_NAME = 'l3'

    s3 = config.get_s3_client()

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY)
    df = pd.read_csv(obj['Body'])

    df = df[['l2_id', 'l3_id', 'l3_name', 'totalProducts', 'rating']]

    engine = config.get_engine_postgresql()

    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="replace", index=False)
    print(f"✅ Загружено {len(df)} строк в таблицу {TABLE_NAME}")
    print("SUCCESS")

def l3_mini():

    BUCKET_NAME  = "airflow-bucket"
    KEY = f"data/l3_data_mini.csv"
    SCHEMA_NAME = 'superset'
    TABLE_NAME = 'l3_mini'

    s3 = config.get_s3_client()

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY)
    df = pd.read_csv(obj['Body'])

    df = df[['l2_id', 'l3_id', 'l3_name', 'totalProducts', 'rating']]

    engine = config.get_engine_postgresql()

    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="replace", index=False)
    print(f"✅ Загружено {len(df)} строк в таблицу {TABLE_NAME}")
    print("SUCCESS")

args = {
    "email": [],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "params": {"priority": "P0"},
}

@dag(
    dag_id="loading_data_to_postgres_dag",
    schedule=None,
    start_date=datetime(year=2025, month=8, day=26),
    catchup=False,
    dagrun_timeout=timedelta(minutes=59),
    default_args=args,
    tags=["PROD"],
)
def my_dag():
    @task
    def process():
        season_ratio()
    process()
my_dag()
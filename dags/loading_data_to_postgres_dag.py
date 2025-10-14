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
from sqlalchemy import create_engine, inspect, text

def get_postgres_schemas():
    engine = config.get_engine_postgresql('superset')


    # Получаем все схемы, исключая системные
    inspector = inspect(engine)

    try:
        # Получаем список схем
        schemas = inspector.get_schema_names()
        print(schemas)

    except Exception as e:
        print(f"Ошибка: {e}")
        return []


def season_ratio():

    BUCKET_NAME  = "airflow-bucket"
    KEY = f"data/season_ratio.csv"
    SCHEMA_NAME = 'sales'
    TABLE_NAME = 'season_ratio'

    s3 = config.get_s3_client()

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY)
    df = pd.read_csv(obj['Body'])

    df['dt'] = pd.to_datetime(df['date'])
    df.drop(['date'], axis=1, inplace=True)
    df = df[['dt', 'subject_id', 'coefficient']]

    engine = config.get_engine_postgresql('superset')

    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="replace", index=False)
    print(f"✅ Загружено {len(df)} строк в таблицу {TABLE_NAME}")
    print("SUCCESS")

def info_subjects():

    BUCKET_NAME  = "airflow-bucket"
    KEY = f"data/info_subjects.csv"
    SCHEMA_NAME = 'sales'
    TABLE_NAME = 'info_subjects'

    s3 = config.get_s3_client()

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY)
    df = pd.read_csv(obj['Body'])
    df[['subject_id', 'name', 'position']] = df[['subjectId', 'name', 'position']]
    engine = config.get_engine_postgresql('superset')

    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="append", index=False)
    print(f"✅ Загружено {len(df)} строк в таблицу {TABLE_NAME}")
    print("SUCCESS")

def info_subject_30_days():

    dt = '2025-10-14'

    BUCKET_NAME  = "airflow-bucket"
    KEY = f"data/{dt}/info_subject_30_days.csv"
    SCHEMA_NAME = 'sales'
    TABLE_NAME = 'info_subjects_days'

    s3 = config.get_s3_client()

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY)
    df = pd.read_csv(obj['Body'])
    df['dt'] = pd.to_datetime(df['date'])
    df.drop(['date'], axis=1, inplace=True)
    df = df[['subject_id', 'name', 'orders', 'ordersSum', 'dt']]
    engine = config.get_engine_postgresql('superset')

    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="replace", index=False)
    print(f"✅ Загружено {len(df)} строк в таблицу {TABLE_NAME}")
    print("SUCCESS")


def l1_l2():

    BUCKET_NAME  = "airflow-bucket"
    KEY = f"data/l1_l2_data.csv"
    SCHEMA_NAME = 'sales'
    TABLE_NAME = 'category_l1_l2'

    s3 = config.get_s3_client()

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY)
    df = pd.read_csv(obj['Body'])

    df['l1_id'] = df['l1_id'].astype(str)
    df['l2_id'] = df['l2_id'].astype(str)

    engine = config.get_engine_postgresql('superset')

    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="replace", index=False)
    print(f"✅ Загружено {len(df)} строк в таблицу {TABLE_NAME}")
    print("SUCCESS")

def l3():

    BUCKET_NAME  = "airflow-bucket"
    KEY = f"data/l3_data.csv"
    SCHEMA_NAME = 'sales'
    TABLE_NAME = 'category_l3'

    s3 = config.get_s3_client()

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY)
    df = pd.read_csv(obj['Body'])

    for col in df.columns:
        if df[col].dtype == 'uint64':
        # Check if values fit in int64 range
            if df[col].max() <= 9223372036854775807:  # max int64 value
                df[col] = df[col].astype('int64')
            else:
                df[col] = df[col].astype('float64')

    df = df[['l2_id', 'l3_id', 'l3_name', 'totalProducts', 'rating']]

    df['l2_id'] = df['l2_id'].astype(str)
    df['l3_id'] = df['l3_id'].astype(str)
    engine = config.get_engine_postgresql('superset')

    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="replace", index=False)
    print(f"✅ Загружено {len(df)} строк в таблицу {TABLE_NAME}")
    print("SUCCESS")

def l3_mini():

    BUCKET_NAME  = "airflow-bucket"
    KEY = f"data/l3_data_mini.csv"
    SCHEMA_NAME = 'sales'
    TABLE_NAME = 'category_l3_mini'

    s3 = config.get_s3_client()

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY)
    df = pd.read_csv(obj['Body'])

    df = df[['l2_id', 'l3_id', 'l3_name', 'totalProducts', 'rating']]
    df['l2_id'] = df['l2_id'].astype(str)
    df['l3_id'] = df['l3_id'].astype(str)

    for col in df.columns:
        if df[col].dtype == 'uint64':
            # Check if values fit in int64 range
            if df[col].max() <= 9223372036854775807:  # max int64 value
                df[col] = df[col].astype('int64')
            else:
                df[col] = df[col].astype('float64')

    engine = config.get_engine_postgresql('superset')

    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="replace", index=False)
    print(f"✅ Загружено {len(df)} строк в таблицу {TABLE_NAME}")
    print("SUCCESS")

def info_30_days():
    dt = '2025-10-14'

    BUCKET_NAME  = "airflow-bucket"
    KEY = f"data/{dt}/info_30_days.csv"
    SCHEMA_NAME = 'sales'
    TABLE_NAME = 'category_info_days'

    s3 = config.get_s3_client()

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY)
    df = pd.read_csv(obj['Body'])

    df['dt'] = pd.to_datetime(df['date'])
    df.drop(['date'], axis=1, inplace=True)
    df = df[['dt', 'l3_id', 'name', 'ordersSum', 'orders']]
    df['l3_id'] = df['l3_id'].astype(str)
    for col in df.columns:
        if df[col].dtype == 'uint64':
            # Check if values fit in int64 range
            if df[col].max() <= 9223372036854775807:  # max int64 value
                df[col] = df[col].astype('int64')
            else:
                df[col] = df[col].astype('float64')

    engine = config.get_engine_postgresql('superset')

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
        info_subject_30_days()
    process()
my_dag()
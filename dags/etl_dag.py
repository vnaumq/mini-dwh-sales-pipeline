from config import MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import boto3
from sqlalchemy import create_engine
import pandas as pd
import requests

def extract():
    url = 'https://mpstats.io/public/sellers.json?v=513946'
    response = requests.get(url)
    data = response.json()



def main():

    BUCKET_NAME  = "airflow-bucket"
    KEY = 'file.txt'
    TABLE_NAME = 'test'
    SCHEMA_NAME = 'dbo'
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        region_name="us-east-1"
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=KEY,
        Body="Текст",
        ContentType="text/plain"
    )

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=KEY)
    content = obj["Body"].read().decode("latin1")  # получаем строку 'Текст'
    df = pd.DataFrame([content], columns=["text"])  # один ряд и один столбец

    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )

    df.to_sql(TABLE_NAME, engine, schema=SCHEMA_NAME, if_exists="replace", index=False)
    print(f"✅ Загружено {len(df)} строк в таблицу {TABLE_NAME}")
    print("SUCCESS")

    return 0


args = {
    "email": [],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "params": {"priority": "P0"},
}

@dag(
    dag_id="test_dag",
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

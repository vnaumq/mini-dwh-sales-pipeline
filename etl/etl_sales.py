import pandas as pd
import boto3
from io import BytesIO
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from pathlib import Path


load_dotenv()

# CONFIG
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
FILE_NAME = os.getenv("FILE_NAME")

PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
TABLE_NAME = os.getenv("TABLE_NAME")


def upload_if_not_exists(local_file_path, bucket_name, s3_file_name):
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    # 1️⃣ Проверяем, существует ли bucket
    try:
        s3.head_bucket(Bucket=bucket_name)
    except:
        # если нет — создаём
        s3.create_bucket(Bucket=bucket_name)
        print(f"🪣 Bucket '{bucket_name}' создан.")

    # 2️⃣ Проверяем, есть ли объект
    try:
        s3.head_object(Bucket=bucket_name, Key=s3_file_name)
        print(f"✅ Файл '{s3_file_name}' уже в MinIO, пропускаем загрузку.")
    except:
        # если нет — загружаем
        if Path(local_file_path).exists():
            s3.upload_file(local_file_path, bucket_name, s3_file_name)
            print(f"🚀 Файл '{s3_file_name}' загружен в MinIO.")
        else:
            raise FileNotFoundError(f"Локальный файл '{local_file_path}' не найден!")

# EXTRACT
def extract_from_minio():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)
    df = pd.read_csv(BytesIO(obj["Body"].read()), encoding="latin1")

    return df

# ---------- TRANSFORM ----------
def transform(df):
    # Убираем пустые значения
    df = df.dropna(subset=["CUSTOMERNAME", "ORDERNUMBER"])
    # Приводим к правильным типам
    df["ORDERDATE"] = pd.to_datetime(df["ORDERDATE"], errors="coerce")
    return df

# ---------- LOAD ----------
def load_to_postgres(df):
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
    print(f"✅ Загружено {len(df)} строк в таблицу {TABLE_NAME}")

# ---------- MAIN ----------
if __name__ == "__main__":
        # Путь к локальному CSV
    LOCAL_FILE = "./sales_data_sample.csv"
    # Debug environment variables
    # Загружаем в MinIO, если ещё нет
    upload_if_not_exists(LOCAL_FILE, BUCKET_NAME, FILE_NAME)

    print("🔹 Extract...")
    df = extract_from_minio()
    print(df.head())

    print("🔹 Transform...")
    df = transform(df)
    print(df.dtypes)

    print("🔹 Load...")
    load_to_postgres(df)
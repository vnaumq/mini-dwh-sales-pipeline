FROM apache/airflow:2.8.4-python3.11

WORKDIR /opt/airflow
RUN pip install --no-cache-dir --upgrade pip
COPY requirements.txt .
COPY dags/ ./dags/
RUN pip install --no-cache-dir -r requirements.txt
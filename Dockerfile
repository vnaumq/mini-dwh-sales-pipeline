FROM apache/airflow:2.10.2
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
COPY requirements.txt /opt/airflow/requirements.txt
COPY dags/ ./dags/
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

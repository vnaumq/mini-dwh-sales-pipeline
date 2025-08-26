FROM apache/airflow:2.8.4-python3.11

RUN pip install --no-cache-dir uv

# WORKDIR /app

COPY pyproject.toml .

RUN uv pip install --system .

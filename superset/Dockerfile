FROM apache/superset:latest

USER root

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    pkg-config \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Установка psycopg2
RUN pip install psycopg2-binary

# Копирование базы данных
COPY superset.db /app/superset_home/superset.db

USER superset
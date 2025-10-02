### Mini DWH Sales Pipeline

Кластер для локальной разработки: Apache Airflow (+API Server), PostgreSQL, MinIO, Apache Superset, pgAdmin и Selenium. Проект предназначен для загрузки и обработки данных (ETL/DAGи в `dags/`), хранения артефактов в MinIO и визуализации в Superset.

---

### Быстрый старт

1) Установите зависимости: Docker Desktop и Docker Compose v2

2) Скопируйте переменные окружения:
```bash
cp .env.example .env
```
Отредактируйте значения при необходимости.

3) Запустите кластер:
```bash
docker compose up -d --build
```

4) Откройте сервисы:
- Airflow API Server: `http://localhost:8080` (для healthcheck), UI через стандартный Webserver не поднимается; используйте Scheduler и API
- Airflow Scheduler health: `http://localhost:8974/health`
- Superset: `http://localhost:8088`
- MinIO (S3 API): `http://localhost:9000`, Консоль: `http://localhost:9001`
- pgAdmin: `http://localhost:5050`
- Selenium Grid: `http://localhost:4444`

Остановить кластер и удалить ресурсы:
```bash
docker compose down -v
```

---

### Дефолтные доступы (из .env.example)

- Airflow admin: `AIRFLOW_WWW_USER_USERNAME` / `AIRFLOW_WWW_USER_PASSWORD`
- Superset admin: `SUPERSET_ADMIN_USERNAME` / `SUPERSET_ADMIN_PASSWORD`
- MinIO: `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`
- pgAdmin: `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD`

Примечание: Docker Compose автоматически загружает `.env` из корня проекта. Отдельно указывать `env_file` в `docker-compose.yaml` не требуется.

---

### Структура

- `dags/` — DAGи, вспомогательные модули, тестовые файлы
- `config/airflow.cfg` — кастомная конфигурация Airflow (монтируется в контейнер)
- `superset/` — домашний каталог Superset (монтируется)
- `logs/` — логи Airflow
- `docker-compose.yaml` — описание всех сервисов
- `requirements.txt` — Python-зависимости для кастомного образа Airflow
- `Dockerfile` — расширение официального образа Airflow и установка зависимостей

---

### Переменные окружения

См. `.env.example` и при необходимости скопируйте в `.env`. Ключевые параметры:

- PostgreSQL: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
- Airflow: `AIRFLOW_UID`, `AIRFLOW_WWW_USER_USERNAME`, `AIRFLOW_WWW_USER_PASSWORD`, `_PIP_ADDITIONAL_REQUIREMENTS`
- MinIO: `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`
- Superset: `SUPERSET_SECRET_KEY`, `SUPERSET_ADMIN_USERNAME`, `SUPERSET_ADMIN_PASSWORD`
- pgAdmin: `PGADMIN_DEFAULT_EMAIL`, `PGADMIN_DEFAULT_PASSWORD`
- ETL-секреты: `SITE_EMAIL`, `SITE_PASSWORD` (если используются вашими DAGами)

---

### Типовой рабочий цикл

1) Размещайте/обновляйте DAGи в `dags/`
2) Пересоберите и перезапустите Airflow при изменении зависимостей в `requirements.txt`:
```bash
docker compose build --no-cache airflow-scheduler airflow-dag-processor airflow-apiserver airflow-init
docker compose up -d
```
3) Просматривайте логи в `logs/` или через UI сервисов

---

### Полезные команды

Зайти в контейнер Airflow CLI:
```bash
docker compose run --rm airflow-cli bash
```

Проверить доступность БД и MinIO:
```bash
docker compose ps
docker compose logs -f postgres minio
```

Инициализация Superset выполняется автоматически сервисом `superset-init`.

---

### Схема проекта

```
graph LR
  subgraph Developer
    DEV[Локальная машина]
  end

  DEV -->|docker compose| DC[(Docker Compose)]

  subgraph Airflow
    APS[Airflow API Server]
    AS[Airflow Scheduler]
    ADP[Airflow Dag Processor]
  end

  subgraph Storage
    PG[(PostgreSQL)]
    MINIO[(MinIO S3)]
  end

  subgraph BI
    SS[Apache Superset]
    PGADMIN[pgAdmin]
  end

  SEL[Selenium]

  DC --> APS
  DC --> AS
  DC --> ADP
  DC --> PG
  DC --> MINIO
  DC --> SS
  DC --> PGADMIN
  DC --> SEL

  AS -->|запуск DAG| ADP
  APS -->|Execution API| AS

  AS -->|подключение| PG
  ADP -->|метаданные Airflow| PG

  AS -->|чтение/запись артефактов| MINIO

  SS -->|OLAP/метаданные Superset| PG
  SS -->|доступ к артефактам (опц.)| MINIO

  PGADMIN -->|админка| PG

  AS -->|веб-скрапинг/автотесты| SEL
```

---

### Примечания по безопасности

- Значения по умолчанию удобны для локальной разработки, но небезопасны для продакшена
- Секреты и пароли храните только в `.env` (не коммитьте в Git), используйте менеджеры секретов в реальной среде

---

### Лицензия

См. лицензии соответствующих открытых проектов (Airflow, Superset, MinIO и др.)

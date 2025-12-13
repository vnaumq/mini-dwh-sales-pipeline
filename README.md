# Mini DWH Sales Pipeline

Проект представляет собой мини-хранилище данных (Data Warehouse) для аналитики продаж на платформе Wildberries. Система автоматически извлекает данные из аналитической платформы [eggheads.solutions](https://eggheads.solutions), обрабатывает их и загружает в PostgreSQL для последующего анализа и визуализации в Apache Superset.

## Описание проекта

Проект реализует полноценный ETL-пайплайн для сбора и анализа данных о продажах:

- **Extract (Извлечение)**: Автоматический сбор данных через API eggheads.solutions с использованием Selenium для аутентификации
- **Transform (Трансформация)**: Обработка и нормализация данных о категориях, продажах, товарах и сезонности
- **Load (Загрузка)**: Сохранение промежуточных данных в MinIO (S3-совместимое хранилище) и финальная загрузка в PostgreSQL

### Собираемые данные

1. **Иерархия категорий**:
   - L1/L2 категории (верхний уровень категорий)
   - L3 категории (детальные категории с рейтингом и количеством товаров)

2. **Аналитика продаж по категориям**:
   - Данные за 30 дней по брендам в категориях L3
   - Тренды продаж (orders, ordersSum) по дням
   - Информация о брендах и их динамике

3. **Данные по товарам (subjects)**:
   - Рейтинг товаров
   - Позиции в категориях
   - Аналитика продаж по товарам за 30 дней

4. **Сезонность**:
   - Коэффициенты сезонности для товаров
   - Данные для прогнозирования спроса

## Технологический стек

### Основные компоненты

- **Apache Airflow 3.0.4** — оркестрация ETL-процессов
  - API Server (порт 8080)
  - Scheduler (планировщик задач)
  - DAG Processor (обработчик DAG-ов)
- **PostgreSQL 13** — основное хранилище данных
- **MinIO** — S3-совместимое хранилище для промежуточных файлов (CSV)
- **Apache Superset** — BI-платформа для визуализации и дашбордов
- **Selenium Grid** — автоматизация браузера для аутентификации
- **pgAdmin** — веб-интерфейс для управления PostgreSQL

### Используемые библиотеки

- `pandas` — обработка данных
- `aiohttp`, `asyncio` — асинхронные HTTP-запросы
- `selenium` — автоматизация браузера
- `boto3` — работа с MinIO (S3 API)
- `psycopg2` — подключение к PostgreSQL
- `sqlalchemy` — ORM для работы с БД

## Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│                    ETL Pipeline Flow                        │
└─────────────────────────────────────────────────────────────┘

1. Аутентификация (Selenium)
   └─> Получение cookies для доступа к API

2. Извлечение данных (Airflow DAGs)
   ├─> get_l1_l2_dag → Иерархия категорий L1/L2
   ├─> get_l3_dag → Категории L3
   ├─> get_info_30_days_*_dag → Аналитика по категориям (30 дней)
   ├─> get_info_subjects_*_dag → Данные по товарам
   └─> get_season_ratio_*_dag → Коэффициенты сезонности

3. Промежуточное хранение (MinIO)
   └─> CSV файлы в bucket: airflow-bucket/data/

4. Загрузка в DWH (PostgreSQL)
   └─> loading_data_to_postgres_dag → Загрузка в схему 'sales'

5. Визуализация (Superset)
   └─> Дашборды и аналитика на основе данных из PostgreSQL
```

## Структура проекта

```
mini-dwh-sales-pipeline/
├── dags/                          # Airflow DAG-и и модули
│   ├── config.py                  # Конфигурация подключений (MinIO, PostgreSQL)
│   ├── endpoints.py               # Функции для работы с API eggheads.solutions
│   ├── get_l1_l2_dag.py          # DAG: получение категорий L1/L2
│   ├── get_l3_dag.py             # DAG: получение категорий L3
│   ├── get_info_30_days_*_dag.py # DAG: аналитика по категориям (30 дней)
│   ├── get_info_subjects_*_dag.py # DAG: данные по товарам
│   ├── get_season_ratio_*_dag.py  # DAG: коэффициенты сезонности
│   ├── loading_data_to_postgres_dag.py # DAG: загрузка в PostgreSQL
│   └── files/                     # Локальные CSV файлы (для разработки)
├── config/
│   └── airflow.cfg               # Кастомная конфигурация Airflow
├── init_postgres/
│   └── 01_create_schema.sql      # Инициализация схемы БД
├── superset/                      # Конфигурация Superset
├── logs/                          # Логи Airflow
├── docker-compose.yaml            # Определение всех сервисов
├── requirements.txt               # Python зависимости
└── Dockerfile                     # Расширение официального образа Airflow
```

## Схема данных в PostgreSQL

Данные загружаются в схему `sales` со следующими таблицами:

- `category_l1_l2` — иерархия категорий L1/L2
- `category_l3` / `category_l3_mini` — категории L3
- `category_info_days` — аналитика продаж по категориям (по дням)
- `info_subjects` — информация о товарах
- `info_subjects_days` — аналитика продаж по товарам (по дням)
- `season_ratio` — коэффициенты сезонности

## Быстрый старт

### Требования

- Docker Desktop (или Docker Engine) и Docker Compose v2
- Минимум 4GB RAM и 2 CPU для Docker
- Файл `.env` с необходимыми переменными окружения

### Установка и запуск

1. **Подготовка переменных окружения**:
   ```bash
   cp .env.example .env
   # Отредактируйте .env и укажите:
   # - SITE_EMAIL и SITE_PASSWORD (учетные данные для eggheads.solutions)
   # - Остальные параметры (Airflow, Superset, MinIO, PostgreSQL)
   ```

2. **Запуск всех сервисов**:
   ```bash
   docker compose up -d --build
   ```

3. **Проверка статуса**:
   ```bash
   docker compose ps
   docker compose logs -f airflow-scheduler
   ```

4. **Остановка и очистка**:
   ```bash
   docker compose down -v
   ```

## Доступ к сервисам

После запуска доступны следующие интерфейсы:

| Сервис | URL | Описание |
|--------|-----|----------|
| **Airflow UI** | http://localhost:8080 | Веб-интерфейс для управления DAG-ами |
| **Superset** | http://localhost:8088 | BI-платформа для визуализации |
| **MinIO Console** | http://localhost:9001 | Управление объектным хранилищем |
| **MinIO API** | http://localhost:9000 | S3-совместимый API |
| **pgAdmin** | http://localhost:5050 | Управление PostgreSQL |
| **Selenium Grid** | http://localhost:4444 | Статус Selenium Grid |

Учетные данные настраиваются в файле `.env`.

## Переменные окружения

### Обязательные для работы ETL

```bash
# Учетные данные для eggheads.solutions
SITE_EMAIL=your_email@example.com
SITE_PASSWORD=your_password

# PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Airflow
AIRFLOW_UID=50000
AIRFLOW_WWW_USER_USERNAME=airflow
AIRFLOW_WWW_USER_PASSWORD=airflow

# Superset
SUPERSET_SECRET_KEY=your_secret_key
SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=admin

# pgAdmin
PGADMIN_DEFAULT_EMAIL=admin@example.com
PGADMIN_DEFAULT_PASSWORD=admin
```

## Работа с DAG-ами

### Доступные DAG-и

1. **get_l1_l2_dag** — получение иерархии категорий L1/L2
2. **get_l3_dag** — получение категорий L3
3. **get_info_30_days_dag** / **get_info_30_days_async_dag** — аналитика по категориям (синхронная/асинхронная версии)
4. **get_info_subjects_dag** / **get_info_subjects_30_days_*_dag** — данные по товарам
5. **get_season_ratio_async_dag** — коэффициенты сезонности
6. **loading_data_to_postgres_dag** — загрузка обработанных данных в PostgreSQL

### Запуск DAG-ов

1. Откройте Airflow UI: http://localhost:8080
2. Найдите нужный DAG в списке
3. Включите DAG (переключатель слева)
4. Запустите вручную (кнопка "Play") или дождитесь запланированного запуска

### Разработка новых DAG-ов

1. Добавьте/обновите код в `dags/`
2. Если изменили `requirements.txt`, пересоберите образы:
   ```bash
   docker compose build --no-cache airflow-scheduler airflow-dag-processor airflow-apiserver airflow-init
   docker compose up -d
   ```
3. Проверьте логи в `logs/` или через Airflow UI

## Поток данных

### Пример полного цикла обработки

1. **Сбор категорий**:
   ```
   get_l1_l2_dag → MinIO: data/l1_l2_data.csv
   get_l3_dag → MinIO: data/l3_data.csv
   ```

2. **Сбор аналитики**:
   ```
   get_info_30_days_async_dag → MinIO: data/YYYY-MM-DD/info_30_days.csv
   get_info_subjects_30_days_async_dag → MinIO: data/YYYY-MM-DD/info_subject_30_days.csv
   get_season_ratio_async_dag → MinIO: data/season_ratio.csv
   ```

3. **Загрузка в DWH**:
   ```
   loading_data_to_postgres_dag → PostgreSQL schema 'sales'
   ```

4. **Визуализация**:
   ```
   Superset → Подключение к PostgreSQL → Создание дашбордов
   ```

## Полезные команды

### Работа с Airflow CLI

```bash
# Открыть shell с Airflow CLI
docker compose run --rm airflow-cli bash

# Просмотр логов
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-dag-processor
```

### Работа с данными

```bash
# Логи PostgreSQL и MinIO
docker compose logs -f postgres minio

# Подключение к PostgreSQL
docker compose exec postgres psql -U airflow -d airflow
```

### Перезапуск сервисов

```bash
# Перезапуск конкретного сервиса
docker compose restart airflow-scheduler

# Полная перезагрузка
docker compose down
docker compose up -d
```

## Особенности реализации

### Асинхронная обработка

Для ускорения сбора данных используются асинхронные DAG-и (`*_async_dag.py`):
- Параллельные HTTP-запросы через `aiohttp`
- Обработка rate limiting (429 ошибки) с экспоненциальным backoff
- Семафоры для контроля параллельности

### Аутентификация

- Selenium используется для автоматического входа в систему eggheads.solutions
- Полученные cookies используются для всех последующих API-запросов
- Поддержка headless-режима для работы в Docker

### Обработка ошибок

- Retry-логика для HTTP-запросов
- Обработка временных сбоев (429, 500)
- Логирование ошибок в Airflow

## Troubleshooting

### Недостаточно ресурсов Docker

Увеличьте память до ≥4GB и CPU до ≥2 в настройках Docker Desktop.

### Переменные окружения не применяются

Убедитесь, что `.env` находится в корне проекта и значения не заключены в кавычки.

### Медленная сборка

Проверьте кэш Docker и список пакетов в `requirements.txt`. Используйте `--no-cache` только при необходимости.

### Ошибки аутентификации

Проверьте правильность `SITE_EMAIL` и `SITE_PASSWORD` в `.env`. Убедитесь, что Selenium Grid доступен (`http://localhost:4444`).

### Проблемы с подключением к PostgreSQL

Проверьте, что сервис `postgres` запущен и здоров:
```bash
docker compose ps postgres
docker compose logs postgres
```

## Безопасность

⚠️ **Важно**: Данная конфигурация предназначена только для разработки!

- Храните секреты в `.env` и не коммитьте их в Git
- Используйте сильные пароли в production
- Настройте firewall для production-развертывания
- Используйте HTTPS для production-окружения

## Лицензия

Применяются лицензии соответствующих open-source проектов (Airflow, Superset, MinIO, PostgreSQL и др.).

---

## Архитектурная диаграмма

```
graph LR
  subgraph Developer
    DEV[Local machine]
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

  AS -->|run DAG| ADP
  APS -->|Execution API| AS

  AS -->|connects to| PG
  ADP -->|Airflow metadata| PG

  AS -->|read/write artifacts| MINIO

  SS -->|OLAP/metadata| PG
  SS -->|artifacts access opt.| MINIO

  PGADMIN -->|admin UI| PG

  AS -->|web scraping auth| SEL
```

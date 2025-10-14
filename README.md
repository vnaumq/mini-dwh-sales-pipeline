## Mini DWH Sales Pipeline

Local analytics stack for ETL and visualization: Apache Airflow (API Server, Scheduler, Dag Processor), PostgreSQL, MinIO, Apache Superset, pgAdmin, and Selenium. Suitable for developing and debugging data ingestion/processing pipelines from `dags/`, storing artifacts in MinIO, and building dashboards in Superset.

---

### Requirements
- Docker Desktop (or Docker Engine) and Docker Compose v2
- `.env` file based on `.env.example`

---

### Quick start
1) Prepare environment variables:
```bash
cp .env.example .env
```
2) Start the stack:
```bash
docker compose up -d --build
```
3) Check statuses and logs:
```bash
docker compose ps
docker compose logs -f airflow-scheduler
```
4) Stop and clean resources when needed:
```bash
docker compose down -v
```

---

### Services and endpoints
- Airflow API Server: `http://localhost:8080`
- Airflow Scheduler health: `http://localhost:8974/health`
- Superset: `http://localhost:8088`
- MinIO API/Console: `http://localhost:9000` / `http://localhost:9001`
- pgAdmin: `http://localhost:5050`
- Selenium Grid: `http://localhost:4444`

Default credentials come from `.env`:
- Airflow admin: `AIRFLOW_WWW_USER_USERNAME` / `AIRFLOW_WWW_USER_PASSWORD`
- Superset admin: `SUPERSET_ADMIN_USERNAME` / `SUPERSET_ADMIN_PASSWORD`
- MinIO: `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`
- pgAdmin: `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD`

Note: Docker Compose automatically picks up `.env` from the project root.

---

### Environment variables (essentials)
- PostgreSQL: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
- Airflow: `AIRFLOW_UID`, `AIRFLOW_WWW_USER_USERNAME`, `AIRFLOW_WWW_USER_PASSWORD`, `_PIP_ADDITIONAL_REQUIREMENTS`
- MinIO: `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`
- Superset: `SUPERSET_SECRET_KEY`, `SUPERSET_ADMIN_USERNAME`, `SUPERSET_ADMIN_PASSWORD`
- pgAdmin: `PGADMIN_DEFAULT_EMAIL`, `PGADMIN_DEFAULT_PASSWORD`
- ETL secrets (if used by DAGs): `SITE_EMAIL`, `SITE_PASSWORD`

---

### Working with DAGs
1) Add/update your code in `dags/`.
2) If you changed `requirements.txt`, rebuild related services:
```bash
docker compose build --no-cache airflow-scheduler airflow-dag-processor airflow-apiserver airflow-init
docker compose up -d
```
3) Inspect task logs in `logs/` or via service UIs.

---

### Useful commands
Open a shell with Airflow CLI:
```bash
docker compose run --rm airflow-cli bash
```
Follow Postgres and MinIO logs:
```bash
docker compose logs -f postgres minio
```
Superset is initialized automatically by the `superset-init` service.

---

### Project structure
- `dags/` — DAGs and helper modules
- `config/airflow.cfg` — custom Airflow configuration (mounted)
- `superset/` — Superset config/data (mounted)
- `logs/` — Airflow logs
- `docker-compose.yaml` — all services definition
- `requirements.txt` — Python dependencies for the Airflow image
- `Dockerfile` — extension of the official Airflow image

---

### Architecture (diagram)
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
  SS -->|artifacts access (opt.)| MINIO

  PGADMIN -->|admin UI| PG

  AS -->|web scraping / autotests| SEL
```

---

### Troubleshooting
- Not enough Docker resources: increase memory to ≥4GB and CPU ≥2 (see tips printed by `airflow-init`).
- Env vars not applied: ensure `.env` is in project root and values are unquoted.
- Slow build: check Docker cache and `requirements.txt` package list.

---

### Security
- Defaults are for development only.
- Keep secrets in `.env`; do not commit them to Git.

---

### License
Licenses of respective open-source projects (Airflow, Superset, MinIO, etc.) apply.

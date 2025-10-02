-- Создание базы данных superset если не существует
SELECT 'CREATE DATABASE superset'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec

-- Создание пользователя superset если не существует
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'superset') THEN
        CREATE USER superset WITH PASSWORD 'superset';
    END IF;
END
$$;

-- Выдача прав
GRANT ALL PRIVILEGES ON DATABASE superset TO superset;
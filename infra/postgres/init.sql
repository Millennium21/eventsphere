-- Runs automatically on first container startup (mounted into
-- /docker-entrypoint-initdb.d/). Alembic's migration also creates these
-- schemas defensively (see migrations/versions/..._initial_schema.py),
-- so this is mainly about making a fresh `docker compose up` obviously
-- correct without waiting on the api container to run migrations first.
CREATE SCHEMA IF NOT EXISTS api;
CREATE SCHEMA IF NOT EXISTS inventory;

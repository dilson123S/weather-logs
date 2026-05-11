#!/usr/bin/env bash
set -euo pipefail

# Ejecuta los scripts SQL de inicialización en el contenedor de Postgres
echo "Inicializando base de datos..."

docker compose exec -T postgres psql -U ${POSTGRES_USER:-weather_user} -d ${POSTGRES_DB:-weather_db} -f /docker-entrypoint-initdb.d/01_schema.sql
docker compose exec -T postgres psql -U ${POSTGRES_USER:-weather_user} -d ${POSTGRES_DB:-weather_db} -f /docker-entrypoint-initdb.d/02_seed.sql

echo "Inicialización completada."

#!/usr/bin/env pwsh
Param()
Write-Host "Inicializando base de datos (PowerShell)..."

$user = $env:POSTGRES_USER -or 'weather_user'
$db = $env:POSTGRES_DB -or 'weather_db'

docker compose exec -T postgres psql -U $user -d $db -f /docker-entrypoint-initdb.d/01_schema.sql
docker compose exec -T postgres psql -U $user -d $db -f /docker-entrypoint-initdb.d/02_seed.sql

Write-Host "Inicialización completada."

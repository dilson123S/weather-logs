#!/usr/bin/env bash
set -euo pipefail

echo "Ejecutando pruebas de validación básicas..."

# 1) Publicar un mensaje inválido para forzar dead-letter
docker compose exec -T producer python - <<'PY'
from main import get_config, build_stations, WeatherProducer
config = get_config()
st = build_stations(1)[0]
producer = WeatherProducer(config)
producer._connect()
bad = st.read()
bad.temperature = 9999  # valor inválido
producer.publish(bad, 'weather.station')
producer.stop()
print('Publicado mensaje inválido')
PY

sleep 2

# 2) Consultar la cola dead-letter (si RabbitMQ UI no está disponible)
echo "Revisando alerts en BD (últimas 5)"
docker compose exec -T postgres psql -U ${POSTGRES_USER:-weather_user} -d ${POSTGRES_DB:-weather_db} -c "SELECT station_id, alert_type, severity, created_at FROM weather_alerts ORDER BY created_at DESC LIMIT 5;"

echo "Pruebas completadas. Revisa RabbitMQ UI y la tabla weather_alerts para más detalle."

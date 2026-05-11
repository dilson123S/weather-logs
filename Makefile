# ============================================================
# Makefile – Comandos de gestión del sistema
# ============================================================
.PHONY: help up down restart logs ps status clean test shell-db shell-rabbit

# Variables
COMPOSE = docker compose
PROJECT = weather-logs

help: ## Muestra esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: ## Levanta todos los servicios
	cp -n .env.example .env 2>/dev/null || true
	$(COMPOSE) up -d --build
	@echo ""
	@echo "✅ Servicios iniciados:"
	@echo "  → RabbitMQ UI:  http://localhost:15672  (admin/admin123)"
	@echo "  → Grafana:      http://localhost:3000   (admin/grafana123)"
	@echo "  → Prometheus:   http://localhost:9090"
	@echo "  → PostgreSQL:   localhost:5432"

down: ## Detiene todos los servicios
	$(COMPOSE) down

restart: ## Reinicia todos los servicios
	$(COMPOSE) restart

logs: ## Muestra logs de todos los servicios
	$(COMPOSE) logs -f --tail=100

logs-producer: ## Logs del productor
	$(COMPOSE) logs -f producer

logs-consumer: ## Logs del consumidor
	$(COMPOSE) logs -f consumer

ps: ## Estado de los contenedores
	$(COMPOSE) ps

status: ps ## Alias de ps

clean: ## Elimina contenedores y volúmenes (¡borra datos!)
	$(COMPOSE) down -v --remove-orphans
	@echo "⚠️  Datos eliminados."

scale-consumers: ## Escala consumidores: make scale-consumers N=3
	$(COMPOSE) up -d --scale consumer=$(N)

test-producer: ## Envía 10 mensajes de prueba
	$(COMPOSE) exec producer python -c "
import main, time
config = main.get_config()
stations = main.build_stations(1)
p = main.WeatherProducer(config)
p._connect()
for _ in range(10):
    r = stations[0].read()
    p.publish(r, 'weather.station')
    print(f'Publicado: {r.station_id} temp={r.temperature}')
p.stop()
"

shell-db: ## Abre consola PostgreSQL
	$(COMPOSE) exec postgres psql -U weather_user -d weather_db

shell-rabbit: ## Abre bash en RabbitMQ
	$(COMPOSE) exec rabbitmq bash

query-logs: ## Muestra últimos 10 logs de BD
	$(COMPOSE) exec postgres psql -U weather_user -d weather_db -c \
	"SELECT station_id, temperature, humidity, pressure, has_alerts, processed_at \
	FROM weather_logs ORDER BY processed_at DESC LIMIT 10;"

query-alerts: ## Muestra alertas recientes
	$(COMPOSE) exec postgres psql -U weather_user -d weather_db -c \
	"SELECT station_id, alert_type, severity, message, created_at \
	FROM weather_alerts ORDER BY created_at DESC LIMIT 20;"

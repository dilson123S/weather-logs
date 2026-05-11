# 🌩️ Weather Station Log Management System

Sistema distribuido de gestión de logs de estaciones meteorológicas basado en microservicios con RabbitMQ, PostgreSQL, Python 3.13+, Prometheus y Grafana.

---

## Arquitectura del sistema

```text
┌─────────────────────────────────────────────────────────────────────┐
│                         Docker Network: weather_net                  │
│                                                                      │
│  ┌──────────────┐     ┌──────────────────┐    ┌──────────────────┐  │
│  │  PRODUCTOR   │     │    RABBITMQ      │    │   CONSUMIDOR(es) │  │
│  │  (Python)    │────▶│  weather.exchange│───▶│   (Python x2)   │  │
│  │              │     │  weather.logs    │    │                  │  │
│  │ 5 estaciones │     │  weather.dead    │    │ ack manual       │  │
│  │ simuladas    │     │  (DLX)           │    │ prefetch=1       │  │
│  └──────────────┘     └──────────────────┘    └────────┬─────────┘  │
│                              │                          │            │
│                       ┌──────┴──────┐                  │            │
│                       │  MGMT UI    │                  ▼            │
│                       │ :15672      │         ┌──────────────────┐  │
│                       └─────────────┘         │   POSTGRESQL     │  │
│                                               │  weather_logs    │  │
│  ┌──────────────┐    ┌──────────────┐        │  weather_alerts  │  │
│  │   GRAFANA    │◀───│  PROMETHEUS  │        │  station_metrics │  │
│  │   :3000      │    │   :9090      │        └──────────────────┘  │
│  └──────────────┘    └──────────────┘                              │
└─────────────────────────────────────────────────────────────────────┘
```text

### Flujo de datos

1. **Productor** genera lecturas cada N segundos por estación
2. Publica al exchange `weather.exchange` con routing key `weather.<station_id>`
3. **RabbitMQ** enruta mensajes a la cola durable `weather.logs`
4. **Consumidores** leen con `prefetch_count=1` y `ack` manual
5. Validan rangos → persisten en **PostgreSQL** → generan alertas
6. Mensajes inválidos → Dead Letter Queue `weather.dead`
7. **Prometheus** recolecta métricas → **Grafana** visualiza

---

## Requisitos

- Docker 24+ y Docker Compose v2.20+
- 2 GB RAM disponibles
- Puertos libres: 5432, 5672, 3000, 9090, 15672

---

## Inicio rápido

```bash
# 1. Clonar el repositorio
git clone <repo-url>
cd weather-logs

# 2. Copiar variables de entorno
cp .env.example .env

# 3. Levantar todos los servicios
make up
# o alternativamente:
docker compose up -d --build
```

### URLs de acceso

| Servicio | URL | Credenciales |
| --- | --- | --- |
| RabbitMQ Management | [http://localhost:15672](http://localhost:15672) | admin / admin123 |
| Grafana | [http://localhost:3000](http://localhost:3000) | admin / grafana123 |
| Prometheus | [http://localhost:9090](http://localhost:9090) | — |
| PostgreSQL | localhost:5432 | weather_user / weather_pass |

---

## Estructura del proyecto

```
weather-logs/
├── docker-compose.yml          # Orquestación principal
├── .env.example                # Variables de entorno de ejemplo
├── Makefile                    # Comandos de gestión
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py                 # Simulador de estaciones
│
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                 # Consumidor con ack manual
│   ├── validator.py            # Validación de rangos
│   └── database.py             # Capa de acceso a PostgreSQL
│
├── db/
│   └── init/
│       ├── 01_schema.sql       # Esquema completo
│       └── 02_seed.sql         # Datos de referencia
│
├── rabbitmq/
│   ├── rabbitmq.conf           # Configuración del broker
│   └── definitions.json        # Exchanges, queues y bindings
│
└── monitoring/
    ├── prometheus/
    │   └── prometheus.yml
    └── grafana/
        ├── datasources.yml
        └── dashboards/
```

---

## Esquema de base de datos

### `weather_logs` (tabla principal)

| Columna | Tipo | Descripción |
| --- | --- | --- |
| `id` | UUID | Clave primaria |
| `station_id` | VARCHAR(50) | ID de la estación |
| `station_name` | VARCHAR(100) | Nombre legible |
| `latitude/longitude` | NUMERIC | Coordenadas |
| `temperature` | NUMERIC | °C |
| `humidity` | NUMERIC | % |
| `pressure` | NUMERIC | hPa |
| `wind_speed` | NUMERIC | km/h |
| `wind_direction` | SMALLINT | Grados 0-360 |
| `precipitation` | NUMERIC | mm |
| `uv_index` | NUMERIC | Índice UV |
| `visibility` | NUMERIC | km |
| `raw_message` | JSONB | Mensaje original completo |
| `has_alerts` | BOOLEAN | Si generó alertas |
| `alert_details` | JSONB | Detalle de alertas |
| `station_ts` | TIMESTAMPTZ | Timestamp de la estación |
| `processed_at` | TIMESTAMPTZ | Timestamp de procesamiento |

### `weather_alerts`

Registro individual de cada alerta generada con severidad (`warning` / `critical`).

### `station_metrics`

Agregados por hora por estación (temperatura min/max/avg, humedad, presión, etc.).

### `processing_errors`

Mensajes que no pudieron procesarse, con detalle del error.

---

## Umbrales de alertas

Configurables vía variables de entorno:

| Variable | Default | Descripción |
| --- | --- | --- |
| `TEMP_MIN` | -50 | Temperatura mínima °C |
| `TEMP_MAX` | 60 | Temperatura máxima °C |
| `HUMIDITY_MIN` | 0 | Humedad mínima % |
| `HUMIDITY_MAX` | 100 | Humedad máxima % |
| `PRESSURE_MIN` | 870 | Presión mínima hPa |
| `PRESSURE_MAX` | 1084 | Presión máxima hPa |
| `WIND_MAX` | 120 | Viento máximo km/h |

Severidad `critical` se activa en valores extremos (>55°C, <-45°C, >130 km/h viento).

---

## Comandos útiles

```bash
# Ver estado de contenedores
make ps

# Seguir logs en tiempo real
make logs

# Solo logs del productor
make logs-producer

# Escalar a 3 consumidores
make scale-consumers N=3

# Consultar últimos logs en BD
make query-logs

# Ver alertas recientes
make query-alerts

# Consola PostgreSQL
make shell-db

# Eliminar todo (incluye datos)
make clean
```

---

## Configuración RabbitMQ

### Exchange
- **Nombre**: `weather.exchange`
- **Tipo**: `topic` (permite routing keys con wildcards)
- **Durable**: sí

### Cola principal
- **Nombre**: `weather.logs`
- **Durable**: sí
- **Dead-letter exchange**: `weather.dlx`
- **TTL mensajes**: 24 horas
- **Capacidad máxima**: 100,000 mensajes

### Cola dead-letter
- **Nombre**: `weather.dead`
- Recibe mensajes rechazados sin reencolar (`basic_nack(requeue=False)`)

---

## Escalabilidad horizontal

```bash
# Escalar consumidores según carga
docker compose up -d --scale consumer=4
```

Cada instancia tiene un `consumer_id` único (hostname del contenedor). RabbitMQ distribuye mensajes en round-robin entre los consumidores activos.

---

## Extensiones posibles

1. **API REST**: FastAPI sobre `weather_logs` para reportes históricos
2. **Alertas en tiempo real**: Webhook o email cuando `severity=critical`
3. **Dashboard Grafana**: Conectar PostgreSQL como datasource adicional
4. **Clustering RabbitMQ**: Múltiples nodos para alta disponibilidad
5. **TLS**: Cifrado en tránsito para conexiones AMQP y PostgreSQL

---

## Pruebas de validación

```bash
# Verificar que el productor está publicando
docker compose logs producer | grep "📡"

# Verificar que el consumidor está procesando
docker compose logs consumer | grep "✅"

# Verificar alertas generadas
make query-alerts

# Verificar mensajes en dead-letter (si hay errores)
# En RabbitMQ UI → Queues → weather.dead
```

---

## Autor y licencia

Sistema desarrollado como prototipo académico de gestión de logs meteorológicos distribuidos.  
Licencia MIT.

---

## Documentación adicional

He añadido documentación y scripts de entrega en la carpeta `docs/` y `scripts/`.

- **Arquitectura y diagrama visual:** docs/architecture.md
- **Checklist de entregables y guía de publicación de video:** docs/DELIVERABLES.md
- **Scripts de inicialización y pruebas:** scripts/init_db.sh, scripts/init_db.ps1, scripts/run_validation_tests.sh

Sigue las instrucciones en `docs/DELIVERABLES.md` para generar el video demostrativo, ejecutar las pruebas y publicar en el foro.

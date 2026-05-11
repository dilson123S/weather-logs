"""
producer/main.py
================
Servicio productor de datos meteorológicos simulados.

Simula N estaciones que publican lecturas periódicas a un exchange
de RabbitMQ usando mensajes persistentes (delivery_mode=2).

Características:
- Reconexión automática con backoff exponencial
- Mensajes en formato JSON con routing key dinámica
- Logging estructurado
- Señales de shutdown limpio
"""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

import pika
import pika.exceptions

# ──────────────────────────────────────────────────────────────
# Configuración de logging
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("weather.producer")


# ──────────────────────────────────────────────────────────────
# Modelo de datos
# ──────────────────────────────────────────────────────────────
@dataclass
class WeatherReading:
    """Lectura completa de una estación meteorológica."""

    station_id: str
    station_name: str
    latitude: float
    longitude: float
    timestamp: str                    # ISO 8601 UTC
    temperature: float                # °C
    humidity: float                   # %
    pressure: float                   # hPa
    wind_speed: float                 # km/h
    wind_direction: int               # grados 0-360
    precipitation: float              # mm
    uv_index: float
    visibility: float                 # km
    schema_version: str = "1.0"


@dataclass
class StationSimulator:
    """Simula una estación con valores base y deriva aleatoria."""

    station_id: str
    station_name: str
    latitude: float
    longitude: float
    base_temp: float
    base_humidity: float
    base_pressure: float

    _temperature: float = field(init=False)
    _humidity: float = field(init=False)
    _pressure: float = field(init=False)

    def __post_init__(self) -> None:
        self._temperature = self.base_temp
        self._humidity = self.base_humidity
        self._pressure = self.base_pressure

    def _drift(self, value: float, delta: float, lo: float, hi: float) -> float:
        """Aplica deriva aleatoria con límites."""
        value += random.uniform(-delta, delta)
        return max(lo, min(hi, value))

    def read(self) -> WeatherReading:
        """Genera una lectura simulada con pequeña deriva respecto a la anterior."""
        # Ocasionalmente introduce valores extremos para probar alertas (~5% prob)
        if random.random() < 0.05:
            self._temperature = random.choice([
                random.uniform(55, 65),   # Temperatura extrema alta
                random.uniform(-55, -40), # Temperatura extrema baja
                self._temperature,
            ])

        self._temperature = self._drift(self._temperature, 0.5, -60, 60)
        self._humidity = self._drift(self._humidity, 1.0, 0, 100)
        self._pressure = self._drift(self._pressure, 0.3, 870, 1084)

        wind_speed = max(0, random.gauss(15, 8))
        # Ráfagas ocasionales para probar alertas de viento
        if random.random() < 0.03:
            wind_speed = random.uniform(110, 140)

        return WeatherReading(
            station_id=self.station_id,
            station_name=self.station_name,
            latitude=self.latitude,
            longitude=self.longitude,
            timestamp=datetime.now(tz=timezone.utc).isoformat(),
            temperature=round(self._temperature, 2),
            humidity=round(self._humidity, 2),
            pressure=round(self._pressure, 2),
            wind_speed=round(wind_speed, 2),
            wind_direction=random.randint(0, 359),
            precipitation=round(max(0, random.gauss(2, 5)), 2),
            uv_index=round(max(0, min(11, random.gauss(4, 2))), 1),
            visibility=round(max(0.1, random.gauss(10, 3)), 2),
        )


# ──────────────────────────────────────────────────────────────
# Configuración desde variables de entorno
# ──────────────────────────────────────────────────────────────
def get_config() -> dict:
    return {
        "host":         os.getenv("RABBITMQ_HOST", "localhost"),
        "port":         int(os.getenv("RABBITMQ_PORT", "5672")),
        "user":         os.getenv("RABBITMQ_USER", "admin"),
        "password":     os.getenv("RABBITMQ_PASS", "admin123"),
        "exchange":     os.getenv("RABBITMQ_EXCHANGE", "weather.exchange"),
        "routing_key":  os.getenv("RABBITMQ_ROUTING_KEY", "weather.station"),
        "interval":     float(os.getenv("PUBLISH_INTERVAL", "3")),
        "stations":     int(os.getenv("STATIONS_COUNT", "5")),
    }


# ──────────────────────────────────────────────────────────────
# Estaciones predefinidas
# ──────────────────────────────────────────────────────────────
STATION_DEFINITIONS = [
    ("STATION_001", "Cartagena Centro",   10.3910,  -75.4794, 30.0,  80.0, 1013.0),
    ("STATION_002", "Barranquilla Norte", 10.9878,  -74.7889, 28.0,  75.0, 1012.0),
    ("STATION_003", "Bogotá El Dorado",    4.7016,  -74.1469, 14.0,  65.0, 1007.0),
    ("STATION_004", "Medellín Olaya",      6.1670,  -75.5900, 22.0,  70.0, 1010.0),
    ("STATION_005", "Cali Alfonso B.",     3.5432,  -76.3816, 24.0,  68.0, 1011.0),
]


def build_stations(count: int) -> list[StationSimulator]:
    defs = (STATION_DEFINITIONS * ((count // len(STATION_DEFINITIONS)) + 1))[:count]
    return [StationSimulator(*d) for d in defs]


# ──────────────────────────────────────────────────────────────
# Productor RabbitMQ con reconexión automática
# ──────────────────────────────────────────────────────────────
class WeatherProducer:
    def __init__(self, config: dict) -> None:
        self.config = config
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel = None
        self._running = False

    def _connect(self) -> None:
        """Establece conexión con backoff exponencial."""
        credentials = pika.PlainCredentials(
            self.config["user"], self.config["password"]
        )
        params = pika.ConnectionParameters(
            host=self.config["host"],
            port=self.config["port"],
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=300,
            connection_attempts=3,
            retry_delay=2,
        )
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

        # Declarar exchange (idempotente)
        self.channel.exchange_declare(
            exchange=self.config["exchange"],
            exchange_type="topic",
            durable=True,
        )
        logger.info(
            "Conectado a RabbitMQ %s:%s – exchange=%s",
            self.config["host"], self.config["port"], self.config["exchange"],
        )

    def publish(self, reading: WeatherReading, routing_key: str) -> None:
        """Publica un mensaje persistente al exchange."""
        body = json.dumps(asdict(reading), ensure_ascii=False)
        self.channel.basic_publish(
            exchange=self.config["exchange"],
            routing_key=routing_key,
            body=body.encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,  # Mensaje persistente
                content_type="application/json",
                content_encoding="utf-8",
                message_id=f"{reading.station_id}-{reading.timestamp}",
                timestamp=int(time.time()),
            ),
        )

    def run(self, stations: list[StationSimulator]) -> None:
        """Bucle principal de publicación."""
        self._running = True
        retries = 0
        max_retries = 10

        while self._running:
            try:
                if self.connection is None or self.connection.is_closed:
                    wait = min(2 ** retries, 60)
                    if retries > 0:
                        logger.info("Reconectando en %ds (intento %d)…", wait, retries)
                        time.sleep(wait)
                    self._connect()
                    retries = 0

                # Publicar lectura de cada estación en ronda robin
                for station in stations:
                    if not self._running:
                        break
                    reading = station.read()
                    routing_key = f"weather.{station.station_id.lower()}"
                    self.publish(reading, routing_key)
                    logger.info(
                        "📡 [%s] temp=%.1f°C hum=%.1f%% pres=%.1fhPa viento=%.1fkm/h",
                        reading.station_id,
                        reading.temperature,
                        reading.humidity,
                        reading.pressure,
                        reading.wind_speed,
                    )

                time.sleep(self.config["interval"])

            except pika.exceptions.AMQPConnectionError as exc:
                logger.warning("Error de conexión AMQP: %s", exc)
                self.connection = None
                retries += 1
                if retries > max_retries:
                    logger.critical("Máximo de reintentos alcanzado. Abortando.")
                    break

            except pika.exceptions.AMQPChannelError as exc:
                logger.warning("Error de canal AMQP: %s", exc)
                self.connection = None
                retries += 1

            except KeyboardInterrupt:
                break

        self.stop()

    def stop(self) -> None:
        self._running = False
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info("Conexión cerrada correctamente.")
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────
# Punto de entrada
# ──────────────────────────────────────────────────────────────
def main() -> None:
    config = get_config()
    stations = build_stations(config["stations"])

    logger.info(
        "🚀 Productor iniciado | estaciones=%d | intervalo=%.1fs",
        len(stations), config["interval"],
    )
    for s in stations:
        logger.info("  → %s (%s)", s.station_id, s.station_name)

    producer = WeatherProducer(config)

    # Shutdown limpio ante señales del sistema
    def _shutdown(signum, _frame):
        logger.info("Señal %d recibida. Deteniendo productor…", signum)
        producer.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    producer.run(stations)


if __name__ == "__main__":
    main()

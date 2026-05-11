"""
consumer/main.py
================
Microservicio consumidor de mensajes meteorológicos.

Características:
- Conexión a RabbitMQ con ack manual y prefetch_count=1
- Validación de rangos de valores
- Persistencia en PostgreSQL con manejo transaccional
- Dead-letter queue para mensajes inválidos
- Reconexión automática con backoff exponencial
- Logging estructurado y métricas básicas
"""

from __future__ import annotations

import json
import logging
import os
import signal
import socket
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import pika
import pika.exceptions

from database import Database
from validator import validate

# ──────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("weather.consumer")

# ID único del consumidor (por hostname/contenedor)
CONSUMER_ID = socket.gethostname()


# ──────────────────────────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────────────────────────
def get_config() -> dict:
    return {
        "host":          os.getenv("RABBITMQ_HOST", "localhost"),
        "port":          int(os.getenv("RABBITMQ_PORT", "5672")),
        "user":          os.getenv("RABBITMQ_USER", "admin"),
        "password":      os.getenv("RABBITMQ_PASS", "admin123"),
        "queue":         os.getenv("RABBITMQ_QUEUE", "weather.logs"),
        "exchange":      os.getenv("RABBITMQ_EXCHANGE", "weather.exchange"),
        "routing_key":   os.getenv("RABBITMQ_ROUTING_KEY", "weather.#"),
        "prefetch":      int(os.getenv("PREFETCH_COUNT", "1")),
    }


# ──────────────────────────────────────────────────────────────
# Métricas en memoria
# ──────────────────────────────────────────────────────────────
class Metrics:
    def __init__(self) -> None:
        self.total_received = 0
        self.total_processed = 0
        self.total_errors = 0
        self.total_alerts = 0
        self.start_time = datetime.now(tz=timezone.utc)

    def log_summary(self) -> None:
        elapsed = (datetime.now(tz=timezone.utc) - self.start_time).total_seconds()
        rate = self.total_processed / elapsed if elapsed > 0 else 0
        logger.info(
            "📊 Métricas [%s] | recibidos=%d procesados=%d errores=%d "
            "alertas=%d tasa=%.2f msg/s",
            CONSUMER_ID,
            self.total_received,
            self.total_processed,
            self.total_errors,
            self.total_alerts,
            rate,
        )


metrics = Metrics()


# ──────────────────────────────────────────────────────────────
# Callback de procesamiento
# ──────────────────────────────────────────────────────────────
def make_callback(db: Database):
    """Genera el callback de procesamiento con referencia a la BD."""

    def callback(ch, method, properties, body: bytes) -> None:
        metrics.total_received += 1
        raw = body.decode("utf-8", errors="replace")

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.error("JSON inválido (delivery_tag=%s): %s", method.delivery_tag, exc)
            db.save_error(raw, "JSONDecodeError", str(exc), CONSUMER_ID)
            metrics.total_errors += 1
            # Rechazar sin reencolar → va al dead-letter
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        try:
            # Validar campos y rangos
            alerts = validate(data)

            if alerts:
                for alert in alerts:
                    level = logging.CRITICAL if alert.severity == "critical" else logging.WARNING
                    logger.log(
                        level,
                        "⚠️  ALERTA [%s/%s] %s",
                        data.get("station_id"), alert.severity.upper(), alert.message,
                    )
                metrics.total_alerts += len(alerts)

            # Persistir en PostgreSQL
            log_id = db.save_reading(data, alerts)

            logger.info(
                "✅ [%s] %s → id=%s alerts=%d",
                CONSUMER_ID,
                data.get("station_id", "UNKNOWN"),
                log_id,
                len(alerts),
            )

            metrics.total_processed += 1

            # ACK manual tras procesamiento exitoso
            ch.basic_ack(delivery_tag=method.delivery_tag)

            # Log de métricas cada 50 mensajes
            if metrics.total_processed % 50 == 0:
                metrics.log_summary()

        except ValueError as exc:
            # Mensaje con estructura inválida → dead-letter
            logger.warning("Validación fallida: %s", exc)
            db.save_error(raw, "ValidationError", str(exc), CONSUMER_ID)
            metrics.total_errors += 1
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        except Exception as exc:
            # Error interno → reencolar para retry
            logger.error("Error procesando mensaje: %s", exc, exc_info=True)
            db.save_error(raw, type(exc).__name__, str(exc), CONSUMER_ID)
            metrics.total_errors += 1
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    return callback


# ──────────────────────────────────────────────────────────────
# Consumidor RabbitMQ
# ──────────────────────────────────────────────────────────────
class WeatherConsumer:
    def __init__(self, config: dict, db: Database) -> None:
        self.config = config
        self.db = db
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel = None
        self._running = False

    def _connect(self) -> None:
        credentials = pika.PlainCredentials(
            self.config["user"], self.config["password"]
        )
        params = pika.ConnectionParameters(
            host=self.config["host"],
            port=self.config["port"],
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=300,
        )
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

        # Declaraciones idempotentes
        self.channel.exchange_declare(
            exchange=self.config["exchange"],
            exchange_type="topic",
            durable=True,
        )
        self.channel.queue_declare(
            queue=self.config["queue"],
            durable=True,
            arguments={
                "x-dead-letter-exchange": "weather.dlx",
                "x-dead-letter-routing-key": "weather.dead",
                "x-message-ttl": 86400000,
                "x-max-length": 100000,
            },
        )
        self.channel.queue_bind(
            queue=self.config["queue"],
            exchange=self.config["exchange"],
            routing_key=self.config["routing_key"],
        )

        # prefetch_count=1 garantiza procesamiento ordenado
        self.channel.basic_qos(prefetch_count=self.config["prefetch"])

        logger.info(
            "Consumidor [%s] conectado | queue=%s prefetch=%d",
            CONSUMER_ID, self.config["queue"], self.config["prefetch"],
        )

    def run(self) -> None:
        self._running = True
        retries = 0

        while self._running:
            try:
                if self.connection is None or self.connection.is_closed:
                    wait = min(2 ** retries, 60)
                    if retries > 0:
                        logger.info("Reconectando en %ds…", wait)
                        time.sleep(wait)
                    self._connect()
                    retries = 0

                self.channel.basic_consume(
                    queue=self.config["queue"],
                    on_message_callback=make_callback(self.db),
                    auto_ack=False,
                )

                logger.info("🎧 Esperando mensajes en '%s'…", self.config["queue"])
                self.channel.start_consuming()

            except pika.exceptions.AMQPConnectionError as exc:
                logger.warning("Conexión perdida: %s", exc)
                self.connection = None
                retries += 1

            except pika.exceptions.AMQPChannelError as exc:
                logger.warning("Error de canal: %s", exc)
                self.connection = None
                retries += 1

            except KeyboardInterrupt:
                break

        self.stop()

    def stop(self) -> None:
        self._running = False
        metrics.log_summary()
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
            if self.connection and not self.connection.is_closed:
                self.connection.close()
        except Exception:
            pass
        self.db.close()
        logger.info("Consumidor [%s] detenido.", CONSUMER_ID)


# ──────────────────────────────────────────────────────────────
# Punto de entrada
# ──────────────────────────────────────────────────────────────
def main() -> None:
    config = get_config()
    logger.info("🚀 Iniciando consumidor [%s]", CONSUMER_ID)

    db = Database()
    consumer = WeatherConsumer(config, db)

    def _shutdown(signum, _frame):
        logger.info("Señal %d recibida. Apagando consumidor…", signum)
        consumer.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    consumer.run()


if __name__ == "__main__":
    main()

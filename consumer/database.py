"""
consumer/database.py
====================
Capa de acceso a PostgreSQL con:
- Pool de conexiones mediante psycopg2
- Reconexión automática con backoff exponencial
- Persistencia de logs, alertas y métricas
- Registro de errores de procesamiento
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Optional

import psycopg2
import psycopg2.extras
import psycopg2.pool

logger = logging.getLogger("weather.consumer.db")


def get_dsn() -> str:
    return (
        f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
        f"port={os.getenv('POSTGRES_PORT', '5432')} "
        f"dbname={os.getenv('POSTGRES_DB', 'weather_db')} "
        f"user={os.getenv('POSTGRES_USER', 'weather_user')} "
        f"password={os.getenv('POSTGRES_PASSWORD', 'weather_pass')} "
        f"connect_timeout=10 "
        f"application_name=weather_consumer"
    )


class Database:
    """Gestor de conexión PostgreSQL con reconexión automática."""

    def __init__(self) -> None:
        self._conn: Optional[psycopg2.extensions.connection] = None
        self._connect_with_retry()

    # ──────────────────────────────────────────────────────
    # Conexión y reconexión
    # ──────────────────────────────────────────────────────
    def _connect(self) -> None:
        dsn = get_dsn()
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = False
        psycopg2.extras.register_uuid()
        logger.info(
            "Conectado a PostgreSQL %s/%s",
            os.getenv("POSTGRES_HOST", "localhost"),
            os.getenv("POSTGRES_DB", "weather_db"),
        )

    def _connect_with_retry(self, max_retries: int = 20) -> None:
        for attempt in range(1, max_retries + 1):
            try:
                self._connect()
                return
            except psycopg2.OperationalError as exc:
                wait = min(2 ** attempt, 60)
                logger.warning(
                    "No se pudo conectar a PostgreSQL (intento %d/%d): %s. "
                    "Reintentando en %ds…",
                    attempt, max_retries, exc, wait,
                )
                time.sleep(wait)

        raise RuntimeError("No se pudo conectar a PostgreSQL tras múltiples intentos.")

    def _ensure_connected(self) -> None:
        try:
            if self._conn is None or self._conn.closed:
                raise psycopg2.OperationalError("Conexión cerrada")
            self._conn.cursor().execute("SELECT 1")
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            logger.warning("Conexión perdida. Reconectando…")
            self._connect_with_retry()

    # ──────────────────────────────────────────────────────
    # Persistencia de lecturas
    # ──────────────────────────────────────────────────────
    def save_reading(self, data: dict, alerts: list) -> str:
        """
        Persiste una lectura meteorológica completa.

        Retorna el UUID del registro creado.
        """
        self._ensure_connected()
        has_alerts = len(alerts) > 0
        alert_details = [asdict(a) for a in alerts] if has_alerts else None

        # Parsear timestamp de la estación
        station_ts = _parse_timestamp(data.get("timestamp"))

        sql_log = """
            INSERT INTO weather_logs (
                station_id, station_name, latitude, longitude,
                temperature, humidity, pressure,
                wind_speed, wind_direction, precipitation,
                uv_index, visibility,
                raw_message, has_alerts, alert_details, station_ts
            ) VALUES (
                %(station_id)s, %(station_name)s, %(latitude)s, %(longitude)s,
                %(temperature)s, %(humidity)s, %(pressure)s,
                %(wind_speed)s, %(wind_direction)s, %(precipitation)s,
                %(uv_index)s, %(visibility)s,
                %(raw_message)s, %(has_alerts)s, %(alert_details)s, %(station_ts)s
            )
            RETURNING id::text
        """
        params = {
            "station_id":    data.get("station_id"),
            "station_name":  data.get("station_name"),
            "latitude":      data.get("latitude"),
            "longitude":     data.get("longitude"),
            "temperature":   data.get("temperature"),
            "humidity":      data.get("humidity"),
            "pressure":      data.get("pressure"),
            "wind_speed":    data.get("wind_speed"),
            "wind_direction": data.get("wind_direction"),
            "precipitation": data.get("precipitation"),
            "uv_index":      data.get("uv_index"),
            "visibility":    data.get("visibility"),
            "raw_message":   json.dumps(data),
            "has_alerts":    has_alerts,
            "alert_details": json.dumps(alert_details) if alert_details else None,
            "station_ts":    station_ts,
        }

        try:
            with self._conn.cursor() as cur:
                cur.execute(sql_log, params)
                log_id = cur.fetchone()[0]

                # Persistir alertas individuales
                if alerts:
                    self._save_alerts(cur, log_id, data["station_id"], alerts)

                # Actualizar métricas por hora
                self._update_metrics(
                    cur,
                    station_id=data["station_id"],
                    hour_bucket=_truncate_to_hour(station_ts),
                    temperature=data.get("temperature", 0),
                    humidity=data.get("humidity", 0),
                    pressure=data.get("pressure", 0),
                    wind_speed=data.get("wind_speed", 0),
                    has_alert=has_alerts,
                )

                self._conn.commit()
                return log_id

        except Exception:
            self._conn.rollback()
            raise

    def _save_alerts(self, cur, log_id: str, station_id: str, alerts: list) -> None:
        sql = """
            INSERT INTO weather_alerts (
                log_id, station_id, alert_type, severity,
                field_name, field_value, threshold_min, threshold_max, message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        rows = [
            (
                log_id, station_id,
                a.alert_type, a.severity,
                a.field_name, a.field_value,
                a.threshold_min, a.threshold_max,
                a.message,
            )
            for a in alerts
        ]
        psycopg2.extras.execute_batch(cur, sql, rows)

    def _update_metrics(self, cur, station_id: str, hour_bucket: datetime,
                        temperature: float, humidity: float, pressure: float,
                        wind_speed: float, has_alert: bool) -> None:
        cur.execute(
            "SELECT upsert_station_metrics(%s, %s, %s, %s, %s, %s, %s)",
            (station_id, hour_bucket, temperature, humidity, pressure,
             wind_speed, has_alert),
        )

    # ──────────────────────────────────────────────────────
    # Registro de errores
    # ──────────────────────────────────────────────────────
    def save_error(self, raw_message: str, error_type: str,
                   error_detail: str, consumer_id: str) -> None:
        self._ensure_connected()
        sql = """
            INSERT INTO processing_errors
                (raw_message, error_type, error_detail, consumer_id)
            VALUES (%s, %s, %s, %s)
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, (raw_message, error_type, error_detail, consumer_id))
            self._conn.commit()
        except Exception as exc:
            logger.error("No se pudo guardar el error en BD: %s", exc)
            self._conn.rollback()

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Conexión a PostgreSQL cerrada.")


# ──────────────────────────────────────────────────────────────
# Utilidades
# ──────────────────────────────────────────────────────────────
def _parse_timestamp(ts_str: Optional[str]) -> datetime:
    if ts_str is None:
        return datetime.now(tz=timezone.utc)
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return datetime.now(tz=timezone.utc)


def _truncate_to_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)

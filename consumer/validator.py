"""
consumer/validator.py
=====================
Validación de rangos de valores meteorológicos.

Define umbrales configurables y genera alertas cuando
algún valor supera los límites establecidos.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Threshold:
    field: str
    label: str
    unit: str
    min_val: Optional[float]
    max_val: Optional[float]


@dataclass
class AlertDetail:
    alert_type: str
    severity: str           # 'warning' | 'critical'
    field_name: str
    field_value: float
    threshold_min: Optional[float]
    threshold_max: Optional[float]
    message: str


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except ValueError:
        return default


def get_thresholds() -> list[Threshold]:
    """Lee umbrales desde variables de entorno."""
    return [
        Threshold("temperature",  "Temperatura",  "°C",
                  _env_float("TEMP_MIN",     -50),
                  _env_float("TEMP_MAX",      60)),
        Threshold("humidity",     "Humedad",     "%",
                  _env_float("HUMIDITY_MIN",   0),
                  _env_float("HUMIDITY_MAX", 100)),
        Threshold("pressure",     "Presión",     "hPa",
                  _env_float("PRESSURE_MIN", 870),
                  _env_float("PRESSURE_MAX", 1084)),
        Threshold("wind_speed",   "Viento",      "km/h",
                  None,
                  _env_float("WIND_MAX",     120)),
        Threshold("uv_index",     "UV",          "",
                  None,
                  _env_float("UV_MAX",        11)),
    ]


def validate(data: dict) -> list[AlertDetail]:
    """
    Valida los valores de una lectura meteorológica.

    Retorna lista de AlertDetail (vacía si no hay alertas).
    Lanza ValueError si el mensaje tiene estructura inválida.
    """
    required_fields = {"station_id", "station_name", "timestamp", "temperature"}
    missing = required_fields - set(data.keys())
    if missing:
        raise ValueError(f"Campos requeridos faltantes: {missing}")

    alerts: list[AlertDetail] = []
    thresholds = get_thresholds()

    for thr in thresholds:
        value = data.get(thr.field)
        if value is None:
            continue

        try:
            value = float(value)
        except (TypeError, ValueError):
            continue

        out_low  = thr.min_val is not None and value < thr.min_val
        out_high = thr.max_val is not None and value > thr.max_val

        if not (out_low or out_high):
            continue

        # Determinar severidad
        severity = _get_severity(thr.field, value, thr.min_val, thr.max_val)

        if out_low:
            msg = (f"{thr.label} ({value}{thr.unit}) por debajo del mínimo "
                   f"permitido ({thr.min_val}{thr.unit})")
        else:
            msg = (f"{thr.label} ({value}{thr.unit}) supera el máximo "
                   f"permitido ({thr.max_val}{thr.unit})")

        alerts.append(AlertDetail(
            alert_type=thr.field,
            severity=severity,
            field_name=thr.field,
            field_value=value,
            threshold_min=thr.min_val,
            threshold_max=thr.max_val,
            message=msg,
        ))

    return alerts


def _get_severity(field: str, value: float,
                  min_val: Optional[float], max_val: Optional[float]) -> str:
    """Determina la severidad de una alerta."""
    critical_rules = {
        "temperature":  lambda v: v > 55 or v < -45,
        "wind_speed":   lambda v: v > 130,
        "humidity":     lambda v: v > 98,
        "pressure":     lambda v: v < 875 or v > 1080,
    }
    rule = critical_rules.get(field)
    if rule and rule(value):
        return "critical"
    return "warning"

-- ============================================================
-- init.sql – Esquema inicial para el sistema de logs meteorológicos
-- Se ejecuta automáticamente al crear el contenedor PostgreSQL
-- ============================================================

-- Extensiones útiles
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- ------------------------------------------------------------
-- Tabla principal de logs de estaciones meteorológicas
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_logs (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    station_id      VARCHAR(50)     NOT NULL,
    station_name    VARCHAR(100)    NOT NULL,
    latitude        NUMERIC(9, 6),
    longitude       NUMERIC(9, 6),
    temperature     NUMERIC(6, 2),          -- °C
    humidity        NUMERIC(5, 2),          -- %
    pressure        NUMERIC(8, 2),          -- hPa
    wind_speed      NUMERIC(6, 2),          -- km/h
    wind_direction  SMALLINT,               -- grados 0-360
    precipitation   NUMERIC(6, 2),          -- mm
    uv_index        NUMERIC(4, 1),
    visibility      NUMERIC(7, 2),          -- km
    raw_message     JSONB,                  -- Mensaje original completo
    has_alerts      BOOLEAN         DEFAULT FALSE,
    alert_details   JSONB,                  -- Detalles de alertas disparadas
    processed_at    TIMESTAMPTZ     DEFAULT NOW(),
    station_ts      TIMESTAMPTZ     NOT NULL,   -- Timestamp de la estación
    created_at      TIMESTAMPTZ     DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Tabla de alertas generadas
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_alerts (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    log_id          UUID NOT NULL REFERENCES weather_logs(id) ON DELETE CASCADE,
    station_id      VARCHAR(50) NOT NULL,
    alert_type      VARCHAR(50) NOT NULL,   -- 'temperature', 'wind', etc.
    severity        VARCHAR(20) NOT NULL,   -- 'warning', 'critical'
    field_name      VARCHAR(50),
    field_value     NUMERIC,
    threshold_min   NUMERIC,
    threshold_max   NUMERIC,
    message         TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Tabla de errores de procesamiento
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processing_errors (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    raw_message     TEXT,
    error_type      VARCHAR(100),
    error_detail    TEXT,
    consumer_id     VARCHAR(100),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Tabla de métricas por estación (agregados)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS station_metrics (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    station_id      VARCHAR(50) NOT NULL,
    hour_bucket     TIMESTAMPTZ NOT NULL,   -- Truncado por hora
    msg_count       INTEGER DEFAULT 0,
    avg_temperature NUMERIC(6, 2),
    min_temperature NUMERIC(6, 2),
    max_temperature NUMERIC(6, 2),
    avg_humidity    NUMERIC(5, 2),
    avg_pressure    NUMERIC(8, 2),
    avg_wind_speed  NUMERIC(6, 2),
    alert_count     INTEGER DEFAULT 0,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (station_id, hour_bucket)
);

-- ------------------------------------------------------------
-- Índices para consultas frecuentes
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_weather_logs_station_id    ON weather_logs(station_id);
CREATE INDEX IF NOT EXISTS idx_weather_logs_station_ts    ON weather_logs(station_ts DESC);
CREATE INDEX IF NOT EXISTS idx_weather_logs_processed_at  ON weather_logs(processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_weather_logs_has_alerts    ON weather_logs(has_alerts) WHERE has_alerts = TRUE;
CREATE INDEX IF NOT EXISTS idx_weather_logs_raw           ON weather_logs USING GIN (raw_message);

CREATE INDEX IF NOT EXISTS idx_alerts_station_id          ON weather_alerts(station_id);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at          ON weather_alerts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_severity            ON weather_alerts(severity);

CREATE INDEX IF NOT EXISTS idx_metrics_station_hour       ON station_metrics(station_id, hour_bucket DESC);

-- ------------------------------------------------------------
-- Vista para últimas lecturas por estación
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_latest_readings AS
SELECT DISTINCT ON (station_id)
    station_id,
    station_name,
    latitude,
    longitude,
    temperature,
    humidity,
    pressure,
    wind_speed,
    wind_direction,
    precipitation,
    uv_index,
    has_alerts,
    station_ts,
    processed_at
FROM weather_logs
ORDER BY station_id, station_ts DESC;

-- ------------------------------------------------------------
-- Vista de alertas recientes (últimas 24h)
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_recent_alerts AS
SELECT
    a.id,
    a.station_id,
    a.alert_type,
    a.severity,
    a.field_name,
    a.field_value,
    a.message,
    a.created_at,
    l.station_name,
    l.latitude,
    l.longitude
FROM weather_alerts a
JOIN weather_logs l ON l.id = a.log_id
WHERE a.created_at >= NOW() - INTERVAL '24 hours'
ORDER BY a.created_at DESC;

-- ------------------------------------------------------------
-- Función para actualizar métricas por hora (upsert)
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION upsert_station_metrics(
    p_station_id    VARCHAR,
    p_hour_bucket   TIMESTAMPTZ,
    p_temperature   NUMERIC,
    p_humidity      NUMERIC,
    p_pressure      NUMERIC,
    p_wind_speed    NUMERIC,
    p_has_alert     BOOLEAN
) RETURNS VOID AS $$
BEGIN
    INSERT INTO station_metrics (
        station_id, hour_bucket,
        msg_count, avg_temperature, min_temperature, max_temperature,
        avg_humidity, avg_pressure, avg_wind_speed, alert_count, updated_at
    ) VALUES (
        p_station_id, p_hour_bucket,
        1, p_temperature, p_temperature, p_temperature,
        p_humidity, p_pressure, p_wind_speed,
        CASE WHEN p_has_alert THEN 1 ELSE 0 END,
        NOW()
    )
    ON CONFLICT (station_id, hour_bucket) DO UPDATE SET
        msg_count       = station_metrics.msg_count + 1,
        avg_temperature = (station_metrics.avg_temperature * station_metrics.msg_count + p_temperature)
                          / (station_metrics.msg_count + 1),
        min_temperature = LEAST(station_metrics.min_temperature, p_temperature),
        max_temperature = GREATEST(station_metrics.max_temperature, p_temperature),
        avg_humidity    = (station_metrics.avg_humidity * station_metrics.msg_count + p_humidity)
                          / (station_metrics.msg_count + 1),
        avg_pressure    = (station_metrics.avg_pressure * station_metrics.msg_count + p_pressure)
                          / (station_metrics.msg_count + 1),
        avg_wind_speed  = (station_metrics.avg_wind_speed * station_metrics.msg_count + p_wind_speed)
                          / (station_metrics.msg_count + 1),
        alert_count     = station_metrics.alert_count + CASE WHEN p_has_alert THEN 1 ELSE 0 END,
        updated_at      = NOW();
END;
$$ LANGUAGE plpgsql;

-- Confirmar inicialización
DO $$
BEGIN
    RAISE NOTICE 'Base de datos weather_db inicializada correctamente';
END $$;

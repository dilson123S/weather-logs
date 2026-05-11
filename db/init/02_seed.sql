-- ============================================================
-- 02_seed.sql – Datos de referencia de estaciones
-- ============================================================

-- Tabla de referencia de estaciones registradas
CREATE TABLE IF NOT EXISTS stations (
    station_id      VARCHAR(50) PRIMARY KEY,
    station_name    VARCHAR(100) NOT NULL,
    country         VARCHAR(50),
    region          VARCHAR(100),
    latitude        NUMERIC(9, 6),
    longitude       NUMERIC(9, 6),
    altitude_m      INTEGER,
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO stations (station_id, station_name, country, region, latitude, longitude, altitude_m)
VALUES
    ('STATION_001', 'Cartagena Centro',   'Colombia', 'Bolívar',         10.3910,  -75.4794,   2),
    ('STATION_002', 'Barranquilla Norte', 'Colombia', 'Atlántico',       10.9878,  -74.7889,  18),
    ('STATION_003', 'Bogotá El Dorado',   'Colombia', 'Cundinamarca',     4.7016,  -74.1469, 2547),
    ('STATION_004', 'Medellín Olaya',     'Colombia', 'Antioquia',        6.1670,  -75.5900, 1495),
    ('STATION_005', 'Cali Alfonso B.',    'Colombia', 'Valle del Cauca',  3.5432,  -76.3816,  965)
ON CONFLICT (station_id) DO NOTHING;

DO $$ BEGIN
    RAISE NOTICE 'Datos de estaciones insertados correctamente';
END $$;

SET search_path = reference, public;

-- ============================================================
-- Section 1: Load airports (filtered to large/medium/small)
-- ============================================================

CREATE TEMP TABLE airports_raw (
    id                INTEGER,
    ident             TEXT,
    type              TEXT,
    name              TEXT,
    latitude_deg      DOUBLE PRECISION,
    longitude_deg     DOUBLE PRECISION,
    elevation_ft      INTEGER,
    continent         TEXT,
    iso_country       TEXT,
    iso_region        TEXT,
    municipality      TEXT,
    scheduled_service TEXT,
    icao_code         TEXT,
    iata_code         TEXT,
    gps_code          TEXT,
    local_code        TEXT,
    home_link         TEXT,
    wikipedia_link    TEXT,
    keywords          TEXT
);

COPY airports_raw FROM '/tmp/airports.csv' WITH (FORMAT csv, HEADER true);

INSERT INTO airports (
    ident,
    name,
    type,
    latitude_deg,
    longitude_deg,
    elevation_ft,
    continent,
    iso_country,
    iso_region,
    municipality,
    scheduled_service,
    iata_code,
    gps_code,
    wikipedia_link
)
SELECT
    ident,
    name,
    type,
    latitude_deg,
    longitude_deg,
    elevation_ft,
    continent,
    iso_country,
    iso_region,
    municipality,
    scheduled_service = 'yes',
    iata_code,
    gps_code,
    wikipedia_link
FROM airports_raw
WHERE type IN ('large_airport', 'medium_airport', 'small_airport');

DROP TABLE airports_raw;


-- ============================================================
-- Section 2: Populate PostGIS geometry column
-- ============================================================

UPDATE airports
SET geom = ST_SetSRID(ST_MakePoint(longitude_deg, latitude_deg), 4326);


-- ============================================================
-- Section 3: Load runways (only for airports already loaded)
-- ============================================================

CREATE TEMP TABLE runways_raw (
    id                        INTEGER,
    airport_ref               INTEGER,
    airport_ident             TEXT,
    length_ft                 INTEGER,
    width_ft                  INTEGER,
    surface                   TEXT,
    lighted                   INTEGER,
    closed                    INTEGER,
    le_ident                  TEXT,
    le_latitude_deg           DOUBLE PRECISION,
    le_longitude_deg          DOUBLE PRECISION,
    le_elevation_ft           INTEGER,
    le_heading_deg_true       DOUBLE PRECISION,
    le_displaced_threshold_ft INTEGER,
    he_ident                  TEXT,
    he_latitude_deg           DOUBLE PRECISION,
    he_longitude_deg          DOUBLE PRECISION,
    he_elevation_ft           INTEGER,
    he_heading_deg_true       DOUBLE PRECISION,
    he_displaced_threshold_ft INTEGER
);

COPY runways_raw FROM '/tmp/runways.csv' WITH (FORMAT csv, HEADER true);

INSERT INTO runways (
    id,
    airport_ident,
    length_ft,
    width_ft,
    surface,
    lighted,
    closed,
    le_ident,
    le_latitude_deg,
    le_longitude_deg,
    le_elevation_ft,
    le_heading_deg_true,
    le_displaced_threshold_ft,
    he_ident,
    he_latitude_deg,
    he_longitude_deg,
    he_elevation_ft,
    he_heading_deg_true,
    he_displaced_threshold_ft
)
SELECT
    id,
    airport_ident,
    length_ft,
    width_ft,
    surface,
    lighted = 1,
    closed  = 1,
    le_ident,
    le_latitude_deg,
    le_longitude_deg,
    le_elevation_ft,
    le_heading_deg_true,
    le_displaced_threshold_ft,
    he_ident,
    he_latitude_deg,
    he_longitude_deg,
    he_elevation_ft,
    he_heading_deg_true,
    he_displaced_threshold_ft
FROM runways_raw
WHERE airport_ident IN (SELECT ident FROM airports);

DROP TABLE runways_raw;


-- ============================================================
-- Section 4: Row count summary
-- ============================================================

SELECT 'airports' AS tbl, COUNT(*) AS rows FROM airports
UNION ALL
SELECT 'runways',          COUNT(*) AS rows FROM runways;

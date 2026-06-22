CREATE TABLE IF NOT EXISTS airports (
    ident             TEXT PRIMARY KEY,
    name              TEXT NOT NULL,
    type              TEXT NOT NULL,
    latitude_deg      DOUBLE PRECISION NOT NULL,
    longitude_deg     DOUBLE PRECISION NOT NULL,
    elevation_ft      INTEGER,
    continent         TEXT,
    iso_country       TEXT,
    iso_region        TEXT,
    municipality      TEXT,
    scheduled_service BOOLEAN,
    iata_code         TEXT,
    gps_code          TEXT,
    wikipedia_link    TEXT,
    properties        JSONB
);

SELECT AddGeometryColumn('airports', 'geom', 4326, 'POINT', 2);

CREATE INDEX IF NOT EXISTS airports_geom_idx
    ON airports USING GIST (geom);

CREATE INDEX IF NOT EXISTS airports_type_idx
    ON airports (type);

CREATE INDEX IF NOT EXISTS airports_country_idx
    ON airports (iso_country);

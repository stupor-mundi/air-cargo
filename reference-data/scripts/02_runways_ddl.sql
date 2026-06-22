CREATE TABLE IF NOT EXISTS runways (
    id                        INTEGER PRIMARY KEY,
    airport_ident             TEXT NOT NULL REFERENCES airports(ident),
    length_ft                 INTEGER,
    width_ft                  INTEGER,
    surface                   TEXT,
    lighted                   BOOLEAN,
    closed                    BOOLEAN,
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

CREATE INDEX IF NOT EXISTS runways_airport_idx
    ON runways (airport_ident);

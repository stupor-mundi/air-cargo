SET search_path = reference, public;

-- ============================================================
-- Coverage zones — hand-drawn ADS-B coverage classification
-- polygons. Edited directly in QGIS against this PostGIS table.
-- Read by GapClassifier batch job via JDBC.
--
-- zone_type values:
--   NORMAL_COVERAGE   good receiver density — gaps here are anomalous
--   COVERAGE_HOLE     land area with no receivers — gaps ambiguous
--   OCEAN             open water — landing impossible, flight continued
-- ============================================================

CREATE TABLE IF NOT EXISTS coverage_zones (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    zone_type   TEXT NOT NULL
                CHECK (zone_type IN (
                    'NORMAL_COVERAGE',
                    'COVERAGE_HOLE',
                    'OCEAN'
                )),
    notes       TEXT,
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    geom        geometry(Polygon, 4326)
);

CREATE INDEX IF NOT EXISTS coverage_zones_geom_idx
    ON coverage_zones USING GIST (geom);

CREATE INDEX IF NOT EXISTS coverage_zones_type_idx
    ON coverage_zones (zone_type);

-- ============================================================
-- poll_outages — missing or near-zero poll windows detected
-- by PollCoverageAnalyser tool. Read by GapClassifier to
-- suppress false aircraft-level gap detection during outages.
-- ============================================================

CREATE TABLE IF NOT EXISTS poll_outages (
    id                SERIAL PRIMARY KEY,
    snapshot_time_ms  BIGINT NOT NULL,
    aircraft_count    INTEGER NOT NULL,
    detected_at_ms    BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS poll_outages_snapshot_idx
    ON poll_outages (snapshot_time_ms);



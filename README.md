# air-cargo

A real-time ADS-B ingestion and analysis pipeline for tracking cargo aircraft,
detecting anomalous behaviour, and inferring freight trade flows.

Built as a learning project for Apache Flink, Sedona, geospatial stream
processing — with a domain focus on air cargo intelligence using open-source
data from the [OpenSky Network](https://opensky-network.org/).

---

## What it does

The pipeline polls global aircraft position data from OpenSky every 10 minutes,
explodes raw snapshots into per-aircraft position streams, runs a stateful flight
phase detection engine, and enriches each position with spatial context from
PostGIS. Completed flight records and gap events are persisted to Delta Lake for
long-term analytical storage.

The analytical goal is to identify patterns that matter for cargo intelligence:
unusual operators appearing at unexpected airports, aircraft disappearing over
well-covered areas, coverage gap classification (ocean vs structural hole vs
anomalous), and eventually trade flow inference from observed routing patterns.

---

## Architecture

```
OpenSky Network API
      │
      │  OAuth2, every 10 min
      ▼
adsb-collector
      │
      │  raw JSON snapshot (one message per poll)
      ▼
Kafka: adsb.states  ──────────────────────────────── 30 day retention
      │
      │  Flink flatMap: explode snapshot → AircraftPosition per aircraft
      │  Flink keyBy(icao24)
      │  Flink KeyedProcessFunction: FlightStateMachine
      │    - detects flight phase transitions
      │    - calls PostGIS for airport proximity on takeoff/landing
      │    - maintains per-aircraft state in RocksDB
      │
      ├──► Kafka: adsb.tracks        Protobuf (PositionUpdate), 30 days
      │           enriched positions with flight_phase, flight_id
      │
      ├──► Kafka: adsb.plane_state   Protobuf (PlaneState), log compacted
      │           current state per aircraft, frontend/tools subscribe here
      │
      ├──► Delta Lake: flights        completed flight records, on landing
      └──► Delta Lake: gaps           gap records, on reappearance

PostGIS (reference data):
  reference.airports        ~47,000 airports from OurAirports
  reference.runways         ~35,000 runways with headings
  reference.coverage_zones  hand-drawn ADS-B coverage polygons

Delta Lake (analytical archive):
  flights         one row per completed flight, MERGE on update
  tracks          OBSERVED positions only, Z-ordered by lat/lon
  gaps            gap facts, classified by batch job
  implied_tracks  tessellated great circle display geometries

Flink batch jobs (periodic):
  GapClassifier          joins gaps with coverage_zones, classifies
  ImpliedTrackGenerator  tessellates great circles for ocean gaps
```

---

## Why these technology choices

**Apache Flink over Spark** — the core processing is stateful and per-aircraft.
Each aircraft has its own flight state that accumulates across many poll cycles.
Flink's `KeyedProcessFunction` with RocksDB state backend is the natural fit for
this. Spark Structured Streaming could work but adds complexity for a use case
that is fundamentally about per-key stateful processing.

**Redpanda over vanilla Kafka** — operationally simpler for single-node
development. No ZooKeeper, faster startup, compatible Kafka API. The same
consumer and producer code works unchanged.

**Three-topic design** — `adsb.states` preserves raw data for replay (30 days
of raw snapshots means you can reprocess with an improved algorithm without
re-collecting). `adsb.tracks` provides the enriched per-aircraft position
history. `adsb.plane_state` is log-compacted to always hold the latest state
per aircraft — the right shape for a frontend or monitoring tool to subscribe
to. Each topic serves a distinct purpose and no two overlap.

**PostGIS over a pure-Java spatial library** — airport proximity detection
(`ST_DWithin` with geography cast for metre-accurate distance) is a single
SQL query against an indexed spatial table. PostGIS handles the spatial
indexing correctly. The JDBC connection is opened once in Flink's `open()`
and reused across records.

**Delta Lake over plain PostgreSQL for completed records** — PostgreSQL is
used for operational state and reference data. Delta Lake handles the
analytical archive because it supports MERGE semantics on Parquet files
(allowing anomaly score updates and landing enrichment without rewriting
records), Z-order spatial indexing for efficient spatial queries via DuckDB,
and retention beyond Kafka's 30-day window.

**Protobuf for internal topics** — `adsb.tracks` and `adsb.plane_state`
carry derived fields (flight_id, flight_phase, anomaly_score) alongside
position data. The schema is defined and owned by this pipeline. Protobuf
gives compact binary serialisation and schema enforcement. Raw input
(`adsb.states`) stays as JSON and is never transcoded.

---

## Modules

| Module | Description |
|---|---|
| [adsb-collector](adsb-collector/README.md) | Polls OpenSky Network, writes raw JSON snapshots to Kafka |
| [adsb-analyser](adsb-analyser/README.md) | Flink job: explode, state machine, PostGIS enrichment, Kafka + Delta Lake sinks |
| [adsb-tools](adsb-tools/README.md) | Standalone diagnostic utilities: GeoJSON exporters, poll coverage analyser |
| [common](common/README.md) | Shared models: `AircraftPosition` POJO |
| [reference-data](reference-data/README.md) | PostGIS DDL scripts and OurAirports CSV loader |

---

## Prerequisites

- Java 17
- Maven 3.8+
- Docker (for PostGIS)
- Redpanda (or any Kafka-compatible broker)
- IntelliJ IDEA (Community Edition works)

**PostGIS container:**
```bash
docker run -d --name postgis \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres \
  -p 5432:5432 \
  postgis/postgis:16-3.4
```

**OpenSky credentials** — register at [opensky-network.org](https://opensky-network.org/)
and set environment variables:
```bash
export OPENSKY_CLIENT_ID=your-client-id
export OPENSKY_CLIENT_SECRET=your-client-secret
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

**Java 17 / Flink** — Flink's Kryo serialiser requires `--add-opens` flags
under Java 17. See [adsb-analyser/README.md](adsb-analyser/README.md) for
the required VM options.

---

## Getting started

**1. Load reference data into PostGIS:**
```bash
cd reference-data/scripts
# copy CSVs into the PostGIS container
docker cp airports.csv postgis:/tmp/airports.csv
docker cp runways.csv postgis:/tmp/runways.csv
# run DDL and load scripts via psql or DBeaver
psql -h localhost -U postgres -f 01_airports_ddl.sql
psql -h localhost -U postgres -f 02_runways_ddl.sql
psql -h localhost -U postgres -f 03_load_data.sql
```

**2. Create Kafka topics** in Redpanda Console or via CLI:
```
adsb.states       retention 30 days, delete policy
adsb.tracks       retention 30 days, delete policy
adsb.plane_state  log compacted
```

**3. Start the collector:**
```bash
cd adsb-collector
mvn spring-boot:run
```

**4. Start the analyser** (from IntelliJ with VM options, or):
```bash
cd adsb-analyser
mvn exec:java
```

**5. Export to GeoJSON for QGIS inspection:**
```bash
cd adsb-tools
mvn exec:java -Dexec.mainClass=io.aircargo.tools.PlaneStateExporter
mvn exec:java -Dexec.mainClass=io.aircargo.tools.TracksExporter
```
Load `output/plane_state.geojson` and `output/tracks.geojson` as layers in QGIS.

---

## Current state

| Component | Status |
|---|---|
| adsb-collector | ✅ Working — polls every 10 min, writes to `adsb.states` |
| adsb-analyser | ✅ Working — full pipeline end-to-end confirmed |
| FlightStateMachine | ✅ Working — phase detection, airport proximity enrichment |
| PostGIS reference data | ✅ Loaded — 47,000 airports, 35,000 runways |
| PlaneStateExporter | ✅ Working — GeoJSON output verified in QGIS |
| TracksExporter | ✅ Working — LineString per flight_id, direction arrows in QGIS |
| Delta Lake | ⬜ Planned |
| GapClassifier batch job | ⬜ Planned |
| Coverage zone polygons | ⬜ Planned |
| Anomaly scoring | ⬜ Placeholder (always 0.0) |
| Aircraft metadata registry | ⬜ Planned |

---

## Data sources

- **OpenSky Network** — ADS-B position data, free tier with OAuth2
- **OurAirports** — airport and runway reference data, public domain

---

## License

MIT


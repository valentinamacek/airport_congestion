# RealTime Airport Congestion and Traffic Flow Analytics

An end-to-end streaming pipeline that monitors live aircraft activity around major
airports in the Alpine Europe region. Live aircraft positions are ingested from the
OpenSky Network, processed with Apache Flink, stored in PostgreSQL, and visualised on
live Grafana dashboards (congestion score, arrivals/departures, holding-pattern
detection, a regional heatmap, and more).

## Architecture (at a glance)

```
OpenSky REST API ──► Kafka Connect ──► Kafka (state_vectors) ──► PyFlink job ──► PostgreSQL ──► Grafana
                     (OpenSky source)                            (windows + classify)   ▲
                                                                                        │
                  data/registrations.tsv ──────────────────────────────────────────────┘
                  (static aircraft metadata, loaded once into PostgreSQL on first start)
```

The live state vectors flow through Kafka and Flink. The static aircraft metadata
(type / operator) is **not** streamed — it is loaded once from `data/registrations.tsv`
into a PostgreSQL lookup table and joined with the live data at query time by Grafana.

## Prerequisites

- **Docker** and **Docker Compose** (Docker Desktop on Windows/Mac).
- That's it — every component runs in a container; nothing else needs to be installed.

## Quick start

From the project root:

```bash
docker compose up -d
```

The first run builds the Flink image and pulls the other images, so it can take a few
minutes. Compose starts the services in dependency order (Kafka, PostgreSQL, etc. become
healthy before the dependent services start). On first start, PostgreSQL automatically
creates its schema and loads the aircraft metadata from `data/registrations.tsv` — no
manual data-loading step is required.

Check everything is up:

```bash
docker compose ps
```

Wait until the core services show `healthy`. `kafka-init` and `flink-job` are one-shot
containers and will show `Exited (0)` — that is expected (they create the topic and
submit the Flink job, then exit).

## Accessing the system

| Service        | URL                     | Credentials (from `.env`)              |
|----------------|-------------------------|----------------------------------------|
| Grafana        | http://localhost:3000   | `GRAFANA_ADMIN_USER` / `..._PASSWORD`  |
| Flink Web UI   | http://localhost:8081   | —                                      |
| pgAdmin        | http://localhost:5050   | `PGADMIN_EMAIL` / `PGADMIN_PASSWORD`   |
| Kafka Connect  | http://localhost:8083   | —                                      |
| PostgreSQL     | localhost:5432          | `POSTGRES_USER` / `POSTGRES_PASSWORD`  |

The dashboard is in Grafana under **Dashboards → Airport Analytics → Airport Congestion
Analytics**. Give it a few minutes of live data before the panels fill in (metrics are
aggregated over one-minute windows).

## Configuration

All configuration lives in `.env` (read automatically by Docker Compose). The values that
matter most:

- `BBOX_LAMIN / LAMAX / LOMIN / LOMAX` — the geographic bounding box polled from OpenSky.
- `OPENSKY_API_INTERVAL` — polling interval in seconds (default `60`).

The airports tracked by the Flink job are defined in `flink/flink_job.py` (`AIRPORTS`),
and should sit inside the bounding box.

## A note on the OpenSky rate limit

The pipeline uses OpenSky's **anonymous** access, which is rate-limited by a daily credit
budget charged in proportion to the queried area. A large bounding box or frequent polling
can exhaust the budget and return `429 Too Many Requests`, which pauses ingestion. If the
dashboards stop receiving new data:

- Check the connector logs: `docker logs kafka-connect --tail 20` (look for `429`).
- If rate-limited, stop the connector to let the budget recover:
  `docker compose stop kafka-connect`, wait, then `docker compose start kafka-connect`.
- A smaller bounding box and a longer `OPENSKY_API_INTERVAL` make the budget last longer.

The pipeline itself (Kafka → Flink → PostgreSQL → Grafana) keeps running regardless; only
the inflow of new live data depends on OpenSky.

## Project layout

```
.
├── docker-compose.yml          # the whole stack
├── .env                        # configuration (credentials, bounding box, interval)
├── data/
│   └── registrations.tsv       # static aircraft metadata (loaded into PostgreSQL)
├── flink/
│   ├── flink_job.py            # the PyFlink stream-processing job
│   ├── kafka_utils.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── flink-sql-connector-kafka-3.1.0-1.18.jar
├── kafka-connect/
!   ├── kafka-connect-opensky/
!   ├── plugins/
│   ├── entrypoint.sh
│   └── source_state_vectors.json
├── postgres/
│   └── init/                   # *.sql run automatically on first start
│       ├── 01_schema.sql
│       └── 02_aircraft_metadata.sql
├── grafana/
│   └── provisioning/           # datasource + dashboard (provisioned automatically)
```

## Stopping

```bash
docker compose stop      # pause everything (data is preserved)
docker compose start     # resume
docker compose down      # remove containers (named volumes / data preserved)
```

Do **not** use `docker compose down -v` unless you intend to wipe all stored data — that
deletes the PostgreSQL and Grafana volumes, after which the next `up` reloads the schema
and metadata from scratch.

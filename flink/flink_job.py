#!/usr/bin/env python3
"""
flink_job.py
────────────
PyFlink stream processing job for RealTime Airport Congestion Analytics.

Scope:
  - Consume the `state_vectors` topic from Kafka
  - Parse and normalise the camelCase fields produced by the OpenSky connector
  - Assign each aircraft to the nearest airport using Haversine distance
  - Classify each aircraft: inbound / outbound / runway load
  - Aggregate per airport in a 1-minute tumbling window (+ a simple congestion score)
  - Write the aggregated metrics to PostgreSQL (airport_metrics table)

How it runs (same pattern as the lab `processor.py` in lab_flink_weather):
  - The job is SUBMITTED to a running Flink session cluster with
        flink run -m flink-jobmanager:8081 -pyfs kafka_utils.py -py flink_job.py
    (see flink/Dockerfile and docker-compose.yml — the flink-job service does this).
  - The Kafka connector JAR is shipped to the cluster via env.add_jars(...),
    exactly like the lab does — it is NOT expected to be pre-installed in
    /opt/flink/lib.
  - It can also be run standalone for debugging (local mini-cluster) with
        python flink_job.py --bootstrap-servers localhost:9092
"""

import os
import sys
import math
import logging
from pathlib import Path
from argparse import ArgumentParser
from datetime import datetime, timezone

from pyflink.common import Types
from pyflink.common.time import Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

import psycopg2
from json import loads

from kafka_utils import wait_for_topics


# ── Config (CLI overrides env, env overrides defaults) ─────────────────────────
# In the docker-compose setup these env vars are passed to the flink-job container
# and captured here at *submit time*, then serialised into the job graph — so the
# TaskManager does not need them in its own environment.
def parse_args():
    parser = ArgumentParser(description="Airport congestion analytics Flink processor")
    parser.add_argument("--bootstrap-servers",
                        default=os.getenv("KAFKA_BOOTSTRAP", "kafka:29092"),
                        help="Kafka bootstrap servers")
    parser.add_argument("--rewind", action="store_true",
                        help="(re)process the topic from the beginning instead of latest")
    parser.add_argument("--window-minutes", type=int,
                        default=int(os.getenv("WINDOW_MINUTES", "1")),
                        help="tumbling window length in minutes")
    parser.add_argument("--radius-km", type=float,
                        default=float(os.getenv("AIRPORT_RADIUS_KM", "50")),
                        help="max distance to assign an aircraft to an airport")
    parser.add_argument("--log-level", default="info",
                        help="debug|info|warn|error")
    return parser.parse_args()


PG_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB", "airport_analytics"),
    "user":     os.getenv("POSTGRES_USER", "airport"),
    "password": os.getenv("POSTGRES_PASSWORD", "airport123"),
}


# ── Airport static data ───────────────────────────────────────────────────────
AIRPORTS = [
    {"icao": "LOWW", "lat": 48.1103, "lon": 16.5697, "name": "Vienna"},
    {"icao": "LOWS", "lat": 47.7933, "lon": 13.0043, "name": "Salzburg"},
    {"icao": "LOWG", "lat": 46.9911, "lon": 15.4396, "name": "Graz"},
    {"icao": "LOWI", "lat": 47.2602, "lon": 11.3440, "name": "Innsbruck"},
    {"icao": "EDDF", "lat": 50.0264, "lon":  8.5431, "name": "Frankfurt"},
    {"icao": "EDDM", "lat": 48.3537, "lon": 11.7750, "name": "Munich"},
    {"icao": "EDDS", "lat": 48.6899, "lon":  9.2220, "name": "Stuttgart"},
    {"icao": "EDDL", "lat": 51.2895, "lon":  6.7668, "name": "Düsseldorf"},
    {"icao": "LSZH", "lat": 47.4647, "lon":  8.5492, "name": "Zurich"},
    {"icao": "LSGG", "lat": 46.2380, "lon":  6.1089, "name": "Geneva"},
    {"icao": "LIMC", "lat": 45.6306, "lon":  8.7281, "name": "Milan Malpensa"},
    {"icao": "LIME", "lat": 45.6739, "lon":  9.7042, "name": "Milan Bergamo"},
    {"icao": "LIML", "lat": 45.4453, "lon":  9.2767, "name": "Milan Linate"},
    {"icao": "LIPZ", "lat": 45.5053, "lon": 12.3519, "name": "Venice"},
    {"icao": "LJLJ", "lat": 46.2237, "lon": 14.4576, "name": "Ljubljana"},
    {"icao": "LKPR", "lat": 50.1008, "lon": 14.2600, "name": "Prague"},
    {"icao": "EPWA", "lat": 52.1657, "lon": 20.9671, "name": "Warsaw"},
    {"icao": "LHBP", "lat": 47.4298, "lon": 19.2610, "name": "Budapest"},
]


# ── Haversine + nearest-airport assignment ─────────────────────────────────────
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def nearest_airport(lat, lon, radius_km):
    best_icao, best_dist = None, float("inf")
    for ap in AIRPORTS:
        d = haversine_km(lat, lon, ap["lat"], ap["lon"])
        if d < best_dist:
            best_dist, best_icao = d, ap["icao"]
    if best_dist <= radius_km:
        return best_icao, round(best_dist, 2)
    return None, best_dist


# ── Flink: parse + enrich ─────────────────────────────────────────────────────
class ParseAndEnrich(MapFunction):
    """
    Parse raw JSON from the state_vectors topic. Field names are camelCase as
    produced by the OpenSky Kafka Connect connector:
        id, callsign, originCountry, latitude, longitude,
        barometricAltitude, onGround, velocity, heading, verticalRate
    Returns a dict, or None for rows we cannot use (filtered out downstream).
    """

    def __init__(self, radius_km):
        self.radius_km = radius_km

    def map(self, raw):
        try:
            s = loads(raw)
        except Exception:
            return None

        lat = s.get("latitude")
        lon = s.get("longitude")
        if lat is None or lon is None:
            return None

        airport_icao, dist_km = nearest_airport(lat, lon, self.radius_km)
        if airport_icao is None:
            return None

        icao24    = s.get("id", "unknown")
        callsign  = (s.get("callsign") or "").strip() or None
        altitude  = s.get("barometricAltitude") or s.get("geometricAltitude") or 0.0
        velocity  = s.get("velocity") or 0.0
        on_ground = bool(s.get("onGround", False))
        heading   = s.get("heading") or 0.0
        vert_rate = s.get("verticalRate") or 0.0

        # Inbound: on ground, or descending at low altitude toward the airport
        is_inbound  = on_ground or (altitude < 5000 and vert_rate < -1.0)
        # Outbound: climbing away at low altitude
        is_outbound = (not on_ground) and altitude < 5000 and vert_rate > 1.0
        # Runway load proxy: on ground or very low
        runway_load = 1 if (on_ground or altitude < 1500) else 0

        return {
            "icao24":       icao24,
            "callsign":     callsign,
            "airport_icao": airport_icao,
            "distance_km":  dist_km,
            "latitude":     lat,
            "longitude":    lon,
            "altitude_m":   round(float(altitude), 1),
            "velocity_ms":  round(float(velocity), 1),
            "heading":      round(float(heading), 1),
            "on_ground":    on_ground,
            "vert_rate":    vert_rate,
            "is_inbound":   is_inbound,
            "is_outbound":  is_outbound,
            "runway_load":  runway_load,
        }


# ── Flink: window aggregation ─────────────────────────────────────────────────
class AirportWindowAggregator(ProcessWindowFunction):
    """Aggregate per-airport metrics over one tumbling window."""

    def process(self, key, context, elements):
        window = context.window()

        # De-duplicate: keep the last record per icao24 within the window
        seen = {}
        for item in elements:
            seen[item["icao24"]] = item
        aircraft = list(seen.values())

        inbound  = sum(1 for a in aircraft if a["is_inbound"])
        outbound = sum(1 for a in aircraft if a["is_outbound"])
        runway   = sum(a["runway_load"] for a in aircraft)
        total    = len(aircraft)

        # Simple real-time congestion index. Weighted sum of the signals; tune
        # the weights as you like. Holding-pattern detection (which needs state
        # across windows) is intentionally left out of this single-window job.
        congestion_score = round(
            inbound * 2.0 + outbound * 1.5 + runway * 1.0 + total * 0.5, 2
        )

        yield {
            "airport_icao":     key,
            "window_start":     datetime.fromtimestamp(window.start / 1000, tz=timezone.utc).isoformat(),
            "window_end":       datetime.fromtimestamp(window.end / 1000, tz=timezone.utc).isoformat(),
            "inbound_count":    inbound,
            "outbound_count":   outbound,
            "runway_load":      runway,
            "total_aircraft":   total,
            "congestion_score": congestion_score,
        }


# ── Flink: PostgreSQL sink ─────────────────────────────────────────────────────
class PostgresSink(MapFunction):
    """
    Write each aggregated metric to PostgreSQL.

    Implemented as a MapFunction with open()/close() so the connection is opened
    ONCE per task (on the TaskManager) and reused — instead of reconnecting per
    record. The DB config is passed in via the constructor so it is serialised
    with the job and the TaskManager does not need the env vars itself.
    """

    SQL = """
        INSERT INTO airport_metrics
            (airport_icao, window_start, window_end,
             inbound_count, outbound_count, runway_load,
             total_aircraft, congestion_score)
        VALUES
            (%(airport_icao)s, %(window_start)s, %(window_end)s,
             %(inbound_count)s, %(outbound_count)s, %(runway_load)s,
             %(total_aircraft)s, %(congestion_score)s)
        ON CONFLICT (airport_icao, window_start)
        DO UPDATE SET
            window_end       = EXCLUDED.window_end,
            inbound_count    = EXCLUDED.inbound_count,
            outbound_count   = EXCLUDED.outbound_count,
            runway_load      = EXCLUDED.runway_load,
            total_aircraft   = EXCLUDED.total_aircraft,
            congestion_score = EXCLUDED.congestion_score;
    """

    def __init__(self, pg_config):
        self.pg_config = pg_config
        self.conn = None
        self.log = None

    def open(self, runtime_context):
        self.log = logging.getLogger("postgres-sink")
        self.conn = psycopg2.connect(**self.pg_config)
        self.conn.autocommit = True
        self.log.info("PostgreSQL connection opened to %s/%s",
                      self.pg_config["host"], self.pg_config["dbname"])

    def _ensure_conn(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(**self.pg_config)
            self.conn.autocommit = True

    def map(self, metric):
        try:
            self._ensure_conn()
            with self.conn.cursor() as cur:
                cur.execute(self.SQL, metric)
            self.log.info(
                "%s  win=%s  in=%d out=%d runway=%d total=%d score=%.1f",
                metric["airport_icao"], metric["window_start"][11:19],
                metric["inbound_count"], metric["outbound_count"],
                metric["runway_load"], metric["total_aircraft"],
                metric["congestion_score"],
            )
        except Exception as exc:
            if self.log:
                self.log.error("DB write failed: %s | %s", exc, metric)
            # drop the (possibly broken) connection so the next record reconnects
            try:
                if self.conn:
                    self.conn.close()
            finally:
                self.conn = None
        return metric

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()

    logging.basicConfig(
        level=logging.getLevelName(args.log_level.upper()),
        format="%(asctime)s (%(levelname).1s) %(message)s [%(threadName)s]",
        stream=sys.stdout,
    )
    log = logging.getLogger("flink-job")

    # Wait until the input topic exists (created by the kafka-init container)
    wait_for_topics(args.bootstrap_servers, "state_vectors")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Ship the Kafka connector JAR to the cluster (same approach as the lab).
    jar_url = Path(Path(__file__).parent, "flink-sql-connector-kafka-3.1.0-1.18.jar").resolve().as_uri()
    env.add_jars(jar_url)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.bootstrap_servers)
        .set_topics("state_vectors")
        .set_group_id("flink-airport-job")
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest() if args.rewind
            else KafkaOffsetsInitializer.latest()
        )
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    metrics = (
        env
        .from_source(kafka_source, WatermarkStrategy.no_watermarks(), "state_vectors_source")
        .map(ParseAndEnrich(args.radius_km), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda x: x is not None)
        .key_by(lambda x: x["airport_icao"])
        .window(TumblingProcessingTimeWindows.of(Time.minutes(args.window_minutes)))
        .process(AirportWindowAggregator(), output_type=Types.PICKLED_BYTE_ARRAY())
    )

    # Sink to PostgreSQL
    metrics.map(PostgresSink(PG_CONFIG), output_type=Types.PICKLED_BYTE_ARRAY())

    log.info(
        "Starting Flink job | kafka=%s | window=%dmin | radius=%.0fkm | airports=%d",
        args.bootstrap_servers, args.window_minutes, args.radius_km, len(AIRPORTS),
    )
    env.execute("Airport Congestion Analytics")


if __name__ == "__main__":
    main()

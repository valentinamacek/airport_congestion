#!/usr/bin/env python3
"""
flink_job.py
────────────
PyFlink stream processing job for RealTime Airport Congestion Analytics.

Day 2 scope:
  - Consume state_vectors topic from Kafka
  - Parse and normalise camelCase fields from the OpenSky connector
  - Assign each aircraft to the nearest airport using Haversine distance
  - Classify each aircraft: inbound / outbound / runway load
  - Aggregate per airport in a 1-minute tumbling window
  - Write results to PostgreSQL airport_metrics table

Runs inside Docker via docker compose.
    See flink/Dockerfile and docker-compose.yml.
"""

import json
import logging
import math
import os
from datetime import datetime, timezone

import psycopg2
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
)
log = logging.getLogger("flink-job")


# ── Load .env ─────────────────────────────────────────────────────────────────
def load_env():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())

load_env()

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",   "kafka:29092")
PG_HOST          = os.getenv("POSTGRES_HOST",      "postgres")
PG_PORT          = int(os.getenv("POSTGRES_PORT",  "5432"))
PG_DB            = os.getenv("POSTGRES_DB",        "airport_analytics")
PG_USER          = os.getenv("POSTGRES_USER",      "airport")
PG_PASS          = os.getenv("POSTGRES_PASSWORD",  "airport123")

WINDOW_MINUTES   = 1
AIRPORT_RADIUS_KM = 50

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


# ── Haversine ─────────────────────────────────────────────────────────────────
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def nearest_airport(lat, lon):
    best_icao, best_dist = None, float("inf")
    for ap in AIRPORTS:
        d = haversine_km(lat, lon, ap["lat"], ap["lon"])
        if d < best_dist:
            best_dist = d
            best_icao = ap["icao"]
    if best_dist <= AIRPORT_RADIUS_KM:
        return best_icao, round(best_dist, 2)
    return None, best_dist


# ── Flink: parse + enrich ─────────────────────────────────────────────────────
class ParseAndEnrich(MapFunction):
    """
    Parse raw JSON from state_vectors topic.
    Field names are camelCase as produced by the OpenSky Kafka Connect connector:
      id, callsign, originCountry, latitude, longitude,
      barometricAltitude, onGround, velocity, heading, verticalRate
    """

    def map(self, raw: str):
        try:
            s = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None

        lat = s.get("latitude")
        lon = s.get("longitude")
        if lat is None or lon is None:
            return None

        icao24    = s.get("id", "unknown")
        callsign  = (s.get("callsign") or "").strip() or None
        altitude  = s.get("barometricAltitude") or s.get("geometricAltitude") or 0.0
        velocity  = s.get("velocity") or 0.0
        on_ground = bool(s.get("onGround", False))
        heading   = s.get("heading") or 0.0
        vert_rate = s.get("verticalRate") or 0.0

        airport_icao, dist_km = nearest_airport(lat, lon)
        if airport_icao is None:
            return None

        # Inbound: descending toward airport OR on ground
        is_inbound = on_ground or (altitude < 5000 and vert_rate < -1.0)
        # Outbound: climbing away from airport
        is_outbound = not on_ground and altitude < 5000 and vert_rate > 1.0
        # Runway load: on ground or very low
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
    """Aggregate per-airport metrics over a 1-minute tumbling window."""

    def process(self, key: str, context, elements):
        window = context.window()

        # De-duplicate: keep last record per icao24 within the window
        seen = {}
        for item in elements:
            seen[item["icao24"]] = item
        aircraft = list(seen.values())

        inbound  = sum(1 for a in aircraft if a["is_inbound"])
        outbound = sum(1 for a in aircraft if a["is_outbound"])
        runway   = sum(a["runway_load"] for a in aircraft)

        yield {
            "airport_icao":   key,
            "window_start":   datetime.fromtimestamp(window.start / 1000, tz=timezone.utc).isoformat(),
            "window_end":     datetime.fromtimestamp(window.end   / 1000, tz=timezone.utc).isoformat(),
            "inbound_count":  inbound,
            "outbound_count": outbound,
            "runway_load":    runway,
            "total_aircraft": len(aircraft),
        }


# ── Flink: PostgreSQL sink ────────────────────────────────────────────────────
def write_to_postgres(metric: dict):
    """Write a single metric dict to PostgreSQL. Called from a map sink."""
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
    )
    sql = """
        INSERT INTO airport_metrics
            (airport_icao, window_start, window_end,
             inbound_count, outbound_count, runway_load)
        VALUES
            (%(airport_icao)s, %(window_start)s, %(window_end)s,
             %(inbound_count)s, %(outbound_count)s, %(runway_load)s)
        ON CONFLICT (airport_icao, window_start)
        DO UPDATE SET
            inbound_count  = EXCLUDED.inbound_count,
            outbound_count = EXCLUDED.outbound_count,
            runway_load    = EXCLUDED.runway_load;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, metric)
        conn.commit()
        log.info(
            "✈  %s  window=%s  inbound=%d  outbound=%d  runway=%d  total=%d",
            metric["airport_icao"],
            metric["window_start"][11:19],
            metric["inbound_count"],
            metric["outbound_count"],
            metric["runway_load"],
            metric["total_aircraft"],
        )
    except Exception as exc:
        log.error("DB write failed: %s | %s", exc, metric)
        conn.rollback()
    finally:
        conn.close()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Kafka connector JAR is pre-installed in /opt/flink/lib by the Dockerfile
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics("state_vectors")
        .set_group_id("flink-airport-job")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = (
        env
        .from_source(kafka_source, WatermarkStrategy.no_watermarks(), "state_vectors_source")
        .map(ParseAndEnrich(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda x: x is not None)
        .key_by(lambda x: x["airport_icao"])
        .window(TumblingProcessingTimeWindows.of(Time.minutes(WINDOW_MINUTES)))
        .process(AirportWindowAggregator(), output_type=Types.PICKLED_BYTE_ARRAY())
    )

    stream.map(lambda m: write_to_postgres(m) or m, output_type=Types.PICKLED_BYTE_ARRAY())

    log.info(
        "Starting Flink job | Kafka=%s | window=%dmin | airports=%d",
        KAFKA_BOOTSTRAP, WINDOW_MINUTES, len(AIRPORTS),
    )
    env.execute("Airport Congestion Analytics")


if __name__ == "__main__":
    main()

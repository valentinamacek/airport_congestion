#!/usr/bin/env python3
"""
flink_job.py
────────────
PyFlink stream processing job for RealTime Airport Congestion Analytics.

Pipeline:

  state_vectors ─► ParseAndEnrich ─┐
                                   │ key_by(icao24)
  registrations ─► ParseReg ───────┤ key_by(icao24)
                                   ▼
                           RegistrationJoin            (KeyedCoProcessFunction:
                           (enrich with manufacturer/   keeps each aircraft's
                            model/registration)         metadata in keyed state)
                                   │  enriched stream
        ┌──────────────────────────┼──────────────────────────┐
        ▼                          ▼                          ▼
  window(airport) ─► agg ─► MetricsSink     filter(is_inbound)    filter(is_holding)
        ├─ airport_metrics                  └─ InboundAircraftSink └─ HoldingAircraftSink
        └─ congestion_history                  (incl. registration)

The registrations topic carries static aircraft metadata (icao24 -> registration,
manufacturer, model), loaded once by the FilePulse connector. It is read from the
beginning so the keyed state is populated; the live state-vector stream then looks
that state up per aircraft.

Submitted to a Flink session cluster:
    flink run -m flink-jobmanager:8081 -pyfs kafka_utils.py -py flink_job.py
"""

import os
import sys
import math
import logging
from pathlib import Path
from argparse import ArgumentParser
from datetime import datetime, timezone
from json import loads

import psycopg2
from pyflink.common import Types
from pyflink.common.time import Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import (
    MapFunction, ProcessWindowFunction, KeyedCoProcessFunction,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

from kafka_utils import wait_for_topics


# ── Config ─────────────────────────────────────────────────────────────────────
def parse_args():
    p = ArgumentParser(description="Airport congestion analytics Flink processor")
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:29092"))
    p.add_argument("--rewind", action="store_true",
                   help="process the state_vectors topic from the beginning")
    p.add_argument("--window-minutes", type=int, default=int(os.getenv("WINDOW_MINUTES", "1")))
    p.add_argument("--radius-km", type=float, default=float(os.getenv("AIRPORT_RADIUS_KM", "50")))
    p.add_argument("--log-level", default="info")
    return p.parse_args()


PG_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB", "airport_analytics"),
    "user":     os.getenv("POSTGRES_USER", "airport"),
    "password": os.getenv("POSTGRES_PASSWORD", "airport123"),
}

AIRPORTS = [
    # Only airports inside the OpenSky bounding box (lat 45-49, lon 6-17).
    # Airports outside the box would never receive traffic, so they are omitted.
    {"icao": "LOWW", "lat": 48.1103, "lon": 16.5697},
    {"icao": "LOWS", "lat": 47.7933, "lon": 13.0043},
    {"icao": "LOWG", "lat": 46.9911, "lon": 15.4396},
    {"icao": "LOWI", "lat": 47.2602, "lon": 11.3440},
    {"icao": "EDDM", "lat": 48.3537, "lon": 11.7750},
    {"icao": "EDDS", "lat": 48.6899, "lon":  9.2220},
    {"icao": "LSZH", "lat": 47.4647, "lon":  8.5492},
    {"icao": "LSGG", "lat": 46.2380, "lon":  6.1089},
    {"icao": "LIMC", "lat": 45.6306, "lon":  8.7281},
    {"icao": "LIME", "lat": 45.6739, "lon":  9.7042},
    {"icao": "LIML", "lat": 45.4453, "lon":  9.2767},
    {"icao": "LIPZ", "lat": 45.5053, "lon": 12.3519},
    {"icao": "LJLJ", "lat": 46.2237, "lon": 14.4576},
]


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


# ── Parse state vectors ───────────────────────────────────────────────────────
class ParseAndEnrich(MapFunction):
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

        icao24    = (s.get("id") or "unknown").strip().lower()
        callsign  = (s.get("callsign") or "").strip() or None
        altitude  = float(s.get("barometricAltitude") or s.get("geometricAltitude") or 0.0)
        velocity  = s.get("velocity") or 0.0
        on_ground = bool(s.get("onGround", False))
        heading   = s.get("heading") or 0.0
        vert_rate = s.get("verticalRate") or 0.0

        is_inbound   = on_ground or (altitude < 5000 and vert_rate < -1.0)
        is_outbound  = (not on_ground) and altitude < 5000 and vert_rate > 1.0
        is_arrival   = is_inbound and (on_ground or altitude < 500)
        is_departure = is_outbound and altitude < 2000
        is_holding   = (not on_ground) and 1000 <= altitude <= 4000 \
                       and abs(vert_rate) < 2.0 and dist_km < 35
        runway_load  = 1 if (on_ground or altitude < 1500) else 0

        return {
            "kind":         "state",
            "icao24":       icao24,
            "callsign":     callsign,
            "airport_icao": airport_icao,
            "distance_km":  dist_km,
            "latitude":     lat,
            "longitude":    lon,
            "altitude_m":   round(altitude, 1),
            "velocity_ms":  round(float(velocity), 1),
            "heading":      round(float(heading), 1),
            "on_ground":    on_ground,
            "vert_rate":    vert_rate,
            "is_inbound":   is_inbound,
            "is_outbound":  is_outbound,
            "is_arrival":   is_arrival,
            "is_departure": is_departure,
            "is_holding":   is_holding,
            "runway_load":  runway_load,
            # filled in by RegistrationJoin
            "model":        None,
            "operator":     None,
        }


# ── Parse registrations (static aircraft metadata) ────────────────────────────
class ParseRegistration(MapFunction):
    """
    Parse one record from the registrations topic.

    The source TSV has exactly: icao24, typecode, operatoricao, built.
    So we expose aircraft type (typecode) and operator (operatoricao);
    there is no registration or manufacturer column in this data.
    """
    def map(self, raw):
        try:
            r = loads(raw)
        except Exception:
            return None

        icao24 = (r.get("icao24") or "").strip().lower()
        if not icao24:
            return None

        typecode = (r.get("typecode") or "").strip() or None
        operator = (r.get("operatoricao") or "").strip() or None
        return {
            "kind":     "reg",
            "icao24":   icao24,
            "model":    typecode,     # aircraft type code, e.g. BE36
            "operator": operator,     # operator ICAO, e.g. DLH
        }


# ── Keyed join: enrich state vectors with registration metadata ───────────────
class RegistrationJoin(KeyedCoProcessFunction):
    """
    Keyed by icao24. Stream 1 = state vectors, Stream 2 = registrations.
    Registration records update per-aircraft keyed state; state vectors read it.
    """
    def open(self, runtime_context):
        self.meta = runtime_context.get_state(
            ValueStateDescriptor("aircraft_meta", Types.PICKLED_BYTE_ARRAY())
        )

    def process_element1(self, sv, ctx):     # state vector
        m = self.meta.value()
        if m:
            sv["model"]    = m.get("model")
            sv["operator"] = m.get("operator")
        yield sv

    def process_element2(self, reg, ctx):    # registration metadata
        self.meta.update(reg)                # store; emit nothing
        return None


# ── Window aggregation ────────────────────────────────────────────────────────
class AirportWindowAggregator(ProcessWindowFunction):
    def process(self, key, context, elements):
        window = context.window()

        seen = {}
        for item in elements:
            seen[item["icao24"]] = item
        aircraft = list(seen.values())

        inbound    = sum(1 for a in aircraft if a["is_inbound"])
        outbound   = sum(1 for a in aircraft if a["is_outbound"])
        arrivals   = sum(1 for a in aircraft if a["is_arrival"])
        departures = sum(1 for a in aircraft if a["is_departure"])
        holding    = sum(1 for a in aircraft if a["is_holding"])
        runway     = sum(a["runway_load"] for a in aircraft)
        total      = len(aircraft)

        congestion_score = round(
            inbound * 2.0 + outbound * 1.5 + holding * 2.5 + runway * 1.0 + total * 0.5, 2
        )

        yield {
            "airport_icao":     key,
            "window_start":     datetime.fromtimestamp(window.start / 1000, tz=timezone.utc).isoformat(),
            "window_end":       datetime.fromtimestamp(window.end / 1000, tz=timezone.utc).isoformat(),
            "inbound_count":    inbound,
            "outbound_count":   outbound,
            "arrivals":         arrivals,
            "departures":       departures,
            "holding_count":    holding,
            "runway_load":      runway,
            "congestion_score": congestion_score,
        }


# ── PostgreSQL sinks ──────────────────────────────────────────────────────────
class _PgSink(MapFunction):
    def __init__(self, pg_config):
        self.pg_config = pg_config
        self.conn = None
        self.log = None

    def open(self, runtime_context):
        self.log = logging.getLogger(self.__class__.__name__)
        self._connect()

    def _connect(self):
        self.conn = psycopg2.connect(**self.pg_config)
        self.conn.autocommit = True

    def _ensure(self):
        if self.conn is None or self.conn.closed:
            self._connect()

    def _run(self, statements):
        try:
            self._ensure()
            with self.conn.cursor() as cur:
                for sql, params in statements:
                    cur.execute(sql, params)
            return True
        except Exception as exc:
            if self.log:
                self.log.error("DB write failed: %s", exc)
            try:
                if self.conn:
                    self.conn.close()
            finally:
                self.conn = None
            return False

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()


class MetricsSink(_PgSink):
    SQL_METRICS = """
        INSERT INTO airport_metrics
            (airport_icao, window_start, window_end,
             inbound_count, outbound_count, arrivals, departures,
             holding_count, runway_load, congestion_score)
        VALUES
            (%(airport_icao)s, %(window_start)s, %(window_end)s,
             %(inbound_count)s, %(outbound_count)s, %(arrivals)s, %(departures)s,
             %(holding_count)s, %(runway_load)s, %(congestion_score)s)
        ON CONFLICT (airport_icao, window_start) DO UPDATE SET
            window_end       = EXCLUDED.window_end,
            inbound_count    = EXCLUDED.inbound_count,
            outbound_count   = EXCLUDED.outbound_count,
            arrivals         = EXCLUDED.arrivals,
            departures       = EXCLUDED.departures,
            holding_count    = EXCLUDED.holding_count,
            runway_load      = EXCLUDED.runway_load,
            congestion_score = EXCLUDED.congestion_score;
    """
    SQL_HISTORY = """
        INSERT INTO congestion_history
            (airport_icao, ts, congestion_score, inbound_count, holding_count)
        VALUES
            (%(airport_icao)s, %(window_end)s, %(congestion_score)s,
             %(inbound_count)s, %(holding_count)s);
    """

    def map(self, m):
        ok = self._run([(self.SQL_METRICS, m), (self.SQL_HISTORY, m)])
        if ok and self.log:
            self.log.info("%s win=%s in=%d out=%d arr=%d dep=%d hold=%d rwy=%d score=%.1f",
                          m["airport_icao"], m["window_start"][11:19],
                          m["inbound_count"], m["outbound_count"], m["arrivals"],
                          m["departures"], m["holding_count"], m["runway_load"],
                          m["congestion_score"])
        return m


class InboundAircraftSink(_PgSink):
    SQL = """
        INSERT INTO inbound_aircraft
            (icao24, airport_icao, callsign, latitude, longitude,
             altitude_m, velocity_ms, heading, on_ground,
             model, operator, distance_km, last_seen)
        VALUES
            (%(icao24)s, %(airport_icao)s, %(callsign)s, %(latitude)s, %(longitude)s,
             %(altitude_m)s, %(velocity_ms)s, %(heading)s, %(on_ground)s,
             %(model)s, %(operator)s, %(distance_km)s, now())
        ON CONFLICT (icao24, airport_icao) DO UPDATE SET
            callsign    = EXCLUDED.callsign,
            latitude    = EXCLUDED.latitude,
            longitude   = EXCLUDED.longitude,
            altitude_m  = EXCLUDED.altitude_m,
            velocity_ms = EXCLUDED.velocity_ms,
            heading     = EXCLUDED.heading,
            on_ground   = EXCLUDED.on_ground,
            model       = COALESCE(EXCLUDED.model, inbound_aircraft.model),
            operator    = COALESCE(EXCLUDED.operator, inbound_aircraft.operator),
            distance_km = EXCLUDED.distance_km,
            last_seen   = now();
    """

    def map(self, a):
        self._run([(self.SQL, a)])
        return a


class HoldingAircraftSink(_PgSink):
    SQL = """
        INSERT INTO holding_aircraft
            (icao24, airport_icao, callsign, altitude_m,
             holding_since, holding_duration_min, last_seen)
        VALUES
            (%(icao24)s, %(airport_icao)s, %(callsign)s, %(altitude_m)s,
             now(), 0, now())
        ON CONFLICT (icao24, airport_icao) DO UPDATE SET
            callsign      = EXCLUDED.callsign,
            altitude_m    = EXCLUDED.altitude_m,
            holding_since = CASE
                WHEN now() - holding_aircraft.last_seen > interval '3 minutes'
                THEN now() ELSE holding_aircraft.holding_since END,
            last_seen     = now(),
            holding_duration_min = EXTRACT(EPOCH FROM (now() - (CASE
                WHEN now() - holding_aircraft.last_seen > interval '3 minutes'
                THEN now() ELSE holding_aircraft.holding_since END))) / 60.0;
    """

    def map(self, a):
        self._run([(self.SQL, a)])
        return a


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()

    logging.basicConfig(
        level=logging.getLevelName(args.log_level.upper()),
        format="%(asctime)s (%(levelname).1s) %(message)s [%(threadName)s]",
        stream=sys.stdout,
    )
    log = logging.getLogger("flink-job")

    wait_for_topics(args.bootstrap_servers, "state_vectors", "registrations")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    jar_url = Path(Path(__file__).parent, "flink-sql-connector-kafka-3.1.0-1.18.jar").resolve().as_uri()
    env.add_jars(jar_url)

    state_source = (
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

    # Registrations are static metadata: always read from the beginning.
    reg_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.bootstrap_servers)
        .set_topics("registrations")
        .set_group_id("flink-registrations")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    state_stream = (
        env.from_source(state_source, WatermarkStrategy.no_watermarks(), "state_vectors_source")
        .map(ParseAndEnrich(args.radius_km), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda x: x is not None)
    )

    reg_stream = (
        env.from_source(reg_source, WatermarkStrategy.no_watermarks(), "registrations_source")
        .map(ParseRegistration(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda x: x is not None)
    )

    # Enrich each aircraft with its registration metadata (keyed join on icao24).
    enriched = (
        state_stream.key_by(lambda x: x["icao24"])
        .connect(reg_stream.key_by(lambda r: r["icao24"]))
        .process(RegistrationJoin(), output_type=Types.PICKLED_BYTE_ARRAY())
    )

    # Branch 1: per-airport window metrics
    (
        enriched
        .key_by(lambda x: x["airport_icao"])
        .window(TumblingProcessingTimeWindows.of(Time.minutes(args.window_minutes)))
        .process(AirportWindowAggregator(), output_type=Types.PICKLED_BYTE_ARRAY())
        .map(MetricsSink(PG_CONFIG), output_type=Types.PICKLED_BYTE_ARRAY())
    )

    # Branch 2: inbound aircraft detail (with registration metadata)
    (
        enriched
        .filter(lambda a: a["is_inbound"])
        .map(InboundAircraftSink(PG_CONFIG), output_type=Types.PICKLED_BYTE_ARRAY())
    )

    # Branch 3: holding aircraft detail
    (
        enriched
        .filter(lambda a: a["is_holding"])
        .map(HoldingAircraftSink(PG_CONFIG), output_type=Types.PICKLED_BYTE_ARRAY())
    )

    log.info("Starting Flink job | kafka=%s | window=%dmin | radius=%.0fkm | airports=%d",
             args.bootstrap_servers, args.window_minutes, args.radius_km, len(AIRPORTS))
    env.execute("Airport Congestion Analytics")


if __name__ == "__main__":
    main()

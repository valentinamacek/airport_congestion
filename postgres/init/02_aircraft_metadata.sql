-- ============================================================
--  aircraft_metadata: static aircraft lookup (icao24 -> type / operator)
--  Replaces the in-Flink registrations join. Loaded once from the TSV;
--  Grafana joins it at query time on icao24.
--
--  typecode can hold full type descriptions (e.g. "BD-700-1A10 Global 6000"),
--  not just short codes, so the text columns are generously sized.
-- ============================================================

CREATE TABLE IF NOT EXISTS aircraft_metadata (
    icao24       VARCHAR(10) PRIMARY KEY,
    typecode     VARCHAR(100),
    operatoricao VARCHAR(100),
    built        VARCHAR(100)
);

-- Staging table (no PK) used to bulk-load the TSV, then upserted above.
CREATE UNLOGGED TABLE IF NOT EXISTS aircraft_metadata_staging (
    icao24       VARCHAR(10),
    typecode     VARCHAR(100),
    operatoricao VARCHAR(100),
    built        VARCHAR(100)
);

-- ============================================================
--  aircraft_metadata: static aircraft lookup (icao24 -> type / operator)
--  Loaded automatically on first `docker compose up` from data/registrations.tsv.
--  Grafana joins this table at query time on icao24 to enrich the inbound /
--  holding panels with aircraft type and operator. No manual step required.
--
--  Requires the data folder to be mounted into the postgres container:
--      volumes:
--        - ./data:/data:ro
--  (server-side COPY reads /data/registrations.tsv from inside the container)
--
--  typecode can hold full type descriptions (e.g. "BD-700-1A10 Global 6000"),
--  so the text columns are generously sized.
-- ============================================================

CREATE TABLE IF NOT EXISTS aircraft_metadata (
    icao24       VARCHAR(10) PRIMARY KEY,
    typecode     VARCHAR(100),
    operatoricao VARCHAR(100),
    built        VARCHAR(100)
);

-- Bulk-load the TSV into a temporary staging table, then upsert into the real
-- table. DISTINCT ON + ON CONFLICT make the load tolerant of duplicate or blank
-- icao24 rows that exist in the source file.
CREATE TEMP TABLE _metadata_staging (
    icao24       text,
    typecode     text,
    operatoricao text,
    built        text
);

COPY _metadata_staging (icao24, typecode, operatoricao, built)
    FROM '/data/registrations.tsv'
    WITH (FORMAT csv, DELIMITER E'\t', HEADER true);

INSERT INTO aircraft_metadata (icao24, typecode, operatoricao, built)
SELECT DISTINCT ON (icao24)
       icao24,
       NULLIF(typecode, ''),
       NULLIF(operatoricao, ''),
       NULLIF(built, '')
FROM _metadata_staging
WHERE icao24 IS NOT NULL AND icao24 <> ''
ON CONFLICT (icao24) DO NOTHING;

DROP TABLE _metadata_staging;

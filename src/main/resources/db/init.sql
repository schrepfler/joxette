-- =============================================================================
-- Joxette schema initialisation reference
-- =============================================================================
-- This file documents every DDL statement that SchemaManager executes at
-- startup.  It is NOT executed directly by the application (SchemaManager
-- runs the statements programmatically), but it can be used for:
--   - local ad-hoc testing:   duckdb ./data/joxette.ducklake < src/main/resources/db/init.sql
--   - schema review / CI lint
--   - documentation
--
-- Sections:
--   1. DuckLake extension + catalog attachment
--   2. DuckDB scalar macros  (headers_*)
--   3. Config tables         (plain DuckDB, main schema)
--   4. DuckLake tables       (Parquet-backed, lake.main schema)
--      4a. known_entities
--      4b. General cassette template
--      4c. Entity cassette template
-- =============================================================================


-- =============================================================================
-- 1. DuckLake extension + catalog attachment
-- =============================================================================

INSTALL ducklake;
LOAD ducklake;

-- DATA_PATH is the object-storage root for Parquet files.
-- Replace placeholders with values from application.yml.
ATTACH IF NOT EXISTS 'ducklake:./data/joxette.ducklake' AS lake
    (DATA_PATH 's3://my-bucket/joxette/data');

-- Local-only alternative (no S3):
-- ATTACH IF NOT EXISTS 'ducklake:./data/joxette.ducklake' AS lake
--     (DATA_PATH './data/joxette_data');


-- =============================================================================
-- 2. DuckDB scalar macros
-- =============================================================================
-- Headers are stored as STRUCT(key VARCHAR, value BLOB)[].
-- All macros are idempotent (CREATE OR REPLACE).

-- headers_get(headers, key)
--   Returns the BLOB value of the first header whose key matches, or NULL.
CREATE OR REPLACE MACRO headers_get(headers, hkey) AS
    list_filter(headers, h -> h.key = hkey)[1].value;

-- headers_get_all(headers, key)
--   Returns a BLOB[] of all values for matching header keys (preserves order).
CREATE OR REPLACE MACRO headers_get_all(headers, hkey) AS
    list_transform(list_filter(headers, h -> h.key = hkey), h -> h.value);

-- headers_put(headers, key, value)
--   Appends a new STRUCT(key, value) entry; does not deduplicate.
CREATE OR REPLACE MACRO headers_put(headers, hkey, hvalue) AS
    list_append(headers, {'key': hkey, 'value': hvalue});

-- headers_to_map(headers)
--   Converts the header list to MAP(VARCHAR, BLOB).
--   When duplicate keys exist the last value wins (consistent with Kafka
--   semantics where later entries shadow earlier ones in some clients).
CREATE OR REPLACE MACRO headers_to_map(headers) AS
    MAP(
        list_transform(headers, h -> h.key),
        list_transform(headers, h -> h.value)
    );


-- =============================================================================
-- 3. Config tables  (plain DuckDB, main schema — NOT DuckLake)
-- =============================================================================
-- These tables store control-plane configuration.  They live in the main DuckDB
-- schema of the same file used by the DuckLake catalog so that a single file
-- contains both the DuckLake metadata and the application config.

CREATE TABLE IF NOT EXISTS topic_configs (
    topic      VARCHAR PRIMARY KEY,
    mode       VARCHAR NOT NULL
                 CHECK (mode IN ('general', 'entity_only', 'both')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS entity_type_configs (
    entity_type  VARCHAR PRIMARY KEY,
    bucket_count INTEGER NOT NULL DEFAULT 256,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE SEQUENCE IF NOT EXISTS seq_entity_source_mappings START 1;

CREATE TABLE IF NOT EXISTS entity_source_mappings (
    id                   INTEGER PRIMARY KEY
                           DEFAULT nextval('seq_entity_source_mappings'),
    entity_type          VARCHAR NOT NULL,
    topic                VARCHAR NOT NULL,
    entity_id_source     VARCHAR NOT NULL
                           CHECK (entity_id_source IN ('key', 'value', 'headers')),
    entity_id_expression VARCHAR NOT NULL,  -- JSONPath expression
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE SEQUENCE IF NOT EXISTS seq_compaction_history START 1;

CREATE TABLE IF NOT EXISTS compaction_history (
    id           INTEGER PRIMARY KEY
                   DEFAULT nextval('seq_compaction_history'),
    table_name   VARCHAR NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    files_before INTEGER,
    files_after  INTEGER,
    bytes_before BIGINT,
    bytes_after  BIGINT,
    status       VARCHAR NOT NULL
                   CHECK (status IN ('running', 'completed', 'failed'))
);


-- =============================================================================
-- 4. DuckLake tables  (lake.main schema — Parquet-backed)
-- =============================================================================
-- These tables are backed by Parquet files written to DATA_PATH.
-- DuckLake manages snapshots, schema evolution, and compaction metadata.
--
-- Column type for flexible JSON payloads:
--   VARIANT  — preferred; survives DuckLake→Parquet round-trip in DuckDB 1.2+.
--   JSON     — fallback when VARIANT is not available.
-- SchemaManager probes at startup and picks the appropriate type automatically.
-- The templates below show VARIANT; replace with JSON if needed.


-- 4a. known_entities
-- Append-only log of every entity_id first/last observed per entity type.
-- No PRIMARY KEY: DuckLake tables are columnar and append-optimised.
-- Deduplication happens at query time (e.g. MAX(last_seen) GROUP BY entity_id).
CREATE TABLE IF NOT EXISTS lake.main.known_entities (
    entity_type VARCHAR NOT NULL,
    entity_id   VARCHAR NOT NULL,
    bucket      INTEGER NOT NULL,
    first_seen  TIMESTAMPTZ NOT NULL,
    last_seen   TIMESTAMPTZ NOT NULL
);


-- 4b. General cassette table template
-- One table per topic whose mode is 'general' or 'both'.
-- Actual table name: general_<normalised_topic>  (e.g. general_orders_events)
--
-- CREATE TABLE IF NOT EXISTS lake.main.general_<topic> (
--     recorded_at     TIMESTAMPTZ NOT NULL,   -- wall-clock time of recording
--     kafka_offset    BIGINT      NOT NULL,   -- Kafka partition offset
--     kafka_partition INTEGER     NOT NULL,   -- Kafka partition number
--     kafka_timestamp TIMESTAMPTZ NOT NULL,   -- producer timestamp from Kafka
--     kafka_key       BLOB,                   -- raw message key bytes
--     kafka_value     BLOB,                   -- raw message value bytes
--     kafka_value_str VARCHAR,                -- UTF-8 decoded value (if valid)
--     metadata        VARIANT,               -- decoded JSON envelope for queries
--     headers         STRUCT(key VARCHAR, value BLOB)[]
-- );
CREATE TABLE IF NOT EXISTS lake.main.general_orders_events (
    recorded_at     TIMESTAMPTZ NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_timestamp TIMESTAMPTZ NOT NULL,
    kafka_key       BLOB,
    kafka_value     BLOB,
    kafka_value_str VARCHAR,
    metadata        VARIANT,
    headers         STRUCT(key VARCHAR, value BLOB)[]
);

CREATE TABLE IF NOT EXISTS lake.main.general_audit_log (
    recorded_at     TIMESTAMPTZ NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_timestamp TIMESTAMPTZ NOT NULL,
    kafka_key       BLOB,
    kafka_value     BLOB,
    kafka_value_str VARCHAR,
    metadata        VARIANT,
    headers         STRUCT(key VARCHAR, value BLOB)[]
);


-- 4c. Entity cassette table template
-- One table per entity type.
-- Actual table name: entity_<normalised_type>  (e.g. entity_order)
--
-- CREATE TABLE IF NOT EXISTS lake.main.entity_<type> (
--     recorded_at     TIMESTAMPTZ NOT NULL,   -- wall-clock time of recording
--     entity_id       VARCHAR     NOT NULL,   -- extracted entity identifier
--     bucket          INTEGER     NOT NULL,   -- hash(entity_id) mod bucket_count
--     topic           VARCHAR     NOT NULL,   -- source Kafka topic
--     kafka_offset    BIGINT      NOT NULL,
--     kafka_partition INTEGER     NOT NULL,
--     kafka_timestamp TIMESTAMPTZ NOT NULL,
--     kafka_key       BLOB,
--     kafka_value     BLOB,
--     kafka_value_str VARCHAR,
--     metadata        VARIANT,
--     headers         STRUCT(key VARCHAR, value BLOB)[]
-- );
CREATE TABLE IF NOT EXISTS lake.main.entity_order (
    recorded_at     TIMESTAMPTZ NOT NULL,
    entity_id       VARCHAR     NOT NULL,
    bucket          INTEGER     NOT NULL,
    topic           VARCHAR     NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_timestamp TIMESTAMPTZ NOT NULL,
    kafka_key       BLOB,
    kafka_value     BLOB,
    kafka_value_str VARCHAR,
    metadata        VARIANT,
    headers         STRUCT(key VARCHAR, value BLOB)[]
);

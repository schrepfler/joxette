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
--      4a. General cassette template
--      4b. Entity cassette template
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
    paused     BOOLEAN NOT NULL DEFAULT false,
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
    id          INTEGER PRIMARY KEY
                  DEFAULT nextval('seq_entity_source_mappings'),
    entity_type VARCHAR NOT NULL,
    topic       VARCHAR NOT NULL,
    mode        VARCHAR NOT NULL DEFAULT 'entity_only'
                  CHECK (mode IN ('entity_only', 'both')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (entity_type, topic)
);

CREATE SEQUENCE IF NOT EXISTS seq_entity_source_matchers START 1;

-- Each source mapping can have multiple matchers — one per message variant
-- that carries the entity ID (e.g. marketSet, resultSet, coverage all carry
-- fixtureId for the same logical fixture entity).
CREATE TABLE IF NOT EXISTS entity_source_matchers (
    id           INTEGER PRIMARY KEY
                   DEFAULT nextval('seq_entity_source_matchers'),
    entity_type  VARCHAR NOT NULL,
    topic        VARCHAR NOT NULL,
    message_type VARCHAR NOT NULL,
    id_source    VARCHAR NOT NULL
                   CHECK (id_source IN ('key', 'value', 'header')),
    id_expression VARCHAR,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (entity_type, topic, message_type)
);

-- Known-entities registry: plain DuckDB so ON CONFLICT is enforced.
-- PRIMARY KEY (entity_type, entity_id) guarantees deduplication.
CREATE TABLE IF NOT EXISTS known_entities (
    entity_type  VARCHAR NOT NULL,
    entity_id    VARCHAR NOT NULL,
    first_seen   TIMESTAMPTZ NOT NULL,
    last_seen    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (entity_type, entity_id)
);

CREATE SEQUENCE IF NOT EXISTS seq_compaction_history START 1;

CREATE TABLE IF NOT EXISTS compaction_history (
    id              INTEGER PRIMARY KEY
                      DEFAULT nextval('seq_compaction_history'),
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ,
    status          VARCHAR NOT NULL
                      CHECK (status IN ('running', 'completed', 'failed')),
    triggered_by    VARCHAR NOT NULL DEFAULT 'unknown',
    targets         VARCHAR[],
    entity_buckets_compacted     INTEGER NOT NULL DEFAULT 0,
    general_partitions_compacted INTEGER NOT NULL DEFAULT 0,
    error_message   VARCHAR
);

CREATE TABLE IF NOT EXISTS snapshots (
    name        VARCHAR     NOT NULL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    size_bytes  BIGINT
);

-- Message-type matchers for general cassettes.
-- Semantics: first matcher (in insertion order) whose id_source/id_expression
-- extracts a non-null value from a message wins; its message_type is written to
-- the cassette row.  No match → message_type = NULL.
CREATE TABLE IF NOT EXISTS topic_message_type_matchers (
    topic           VARCHAR NOT NULL,
    message_type    VARCHAR NOT NULL,
    id_source       VARCHAR NOT NULL
                      CHECK (id_source IN ('key', 'value', 'header')),
    id_expression   VARCHAR NOT NULL,
    PRIMARY KEY (topic, message_type)
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


-- 4a. General cassette table template
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
--     headers         STRUCT(key VARCHAR, value VARCHAR)[],  -- UTF-8 decoded; binary values base64-encoded
--     message_type    VARCHAR                -- matched topic_message_type_matchers label, or NULL
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
    headers         STRUCT(key VARCHAR, value VARCHAR)[],
    message_type    VARCHAR
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
    headers         STRUCT(key VARCHAR, value VARCHAR)[],
    message_type    VARCHAR
);


-- 4b. Entity cassette table template
-- One table per entity type.
-- Actual table name: entity_<normalised_type>  (e.g. entity_order)
--
-- CREATE TABLE IF NOT EXISTS lake.main.entity_<type> (
--     recorded_at     TIMESTAMPTZ NOT NULL,   -- wall-clock time of recording
--     entity_id       VARCHAR     NOT NULL,   -- extracted entity identifier
--     bucket          INTEGER     NOT NULL,   -- hash(entity_id) mod bucket_count
--     message_type    VARCHAR,                -- discriminator field from message
--     topic           VARCHAR     NOT NULL,   -- source Kafka topic
--     kafka_offset    BIGINT      NOT NULL,
--     kafka_partition INTEGER     NOT NULL,
--     kafka_timestamp TIMESTAMPTZ NOT NULL,
--     kafka_key       BLOB,
--     kafka_value     BLOB,
--     kafka_value_str VARCHAR,
--     metadata        VARIANT,
--     headers         STRUCT(key VARCHAR, value VARCHAR)[]  -- UTF-8 decoded; binary values base64-encoded
-- );
CREATE TABLE IF NOT EXISTS lake.main.entity_order (
    recorded_at     TIMESTAMPTZ NOT NULL,
    entity_id       VARCHAR     NOT NULL,
    bucket          INTEGER     NOT NULL,
    message_type    VARCHAR,
    topic           VARCHAR     NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_timestamp TIMESTAMPTZ NOT NULL,
    kafka_key       BLOB,
    kafka_value     BLOB,
    kafka_value_str VARCHAR,
    metadata        VARIANT,
    headers         STRUCT(key VARCHAR, value VARCHAR)[]
);

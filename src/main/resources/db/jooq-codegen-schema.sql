-- =============================================================================
-- jOOQ codegen schema
-- =============================================================================
-- Creates the config tables so that the jOOQ DDLDatabase can introspect them
-- and emit Java sources.
--
-- Uses standard SQL types (TIMESTAMP WITH TIME ZONE, BOOLEAN) for compatibility
-- with jOOQ's DDL interpreter.  The runtime schema in SchemaManager uses DuckDB
-- aliases (TIMESTAMPTZ) which are equivalent.
--
-- Tables covered:
--   - topic_configs
--   - entity_type_configs
--   - entity_source_mappings
--   - entity_source_matchers
--   - topic_message_type_matchers
--   - known_entities
--   - compaction_history
--   - retention_history
--   - snapshots
--   - transform_presets
--
-- Mirrors the production schema in SchemaManager.createConfigTables().
-- =============================================================================

CREATE SEQUENCE IF NOT EXISTS seq_entity_source_mappings START WITH 1;
CREATE SEQUENCE IF NOT EXISTS seq_entity_source_matchers START WITH 1;
CREATE SEQUENCE IF NOT EXISTS seq_compaction_history START WITH 1;
CREATE SEQUENCE IF NOT EXISTS seq_retention_history START WITH 1;

CREATE TABLE topic_configs (
    topic          VARCHAR PRIMARY KEY,
    mode           VARCHAR NOT NULL,
    paused         BOOLEAN NOT NULL DEFAULT false,
    start_from     VARCHAR NOT NULL DEFAULT 'latest',
    retention_days INTEGER,
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE TABLE entity_type_configs (
    entity_type    VARCHAR PRIMARY KEY,
    bucket_count   INTEGER NOT NULL DEFAULT 256,
    retention_days INTEGER,
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE TABLE entity_source_mappings (
    id          INTEGER PRIMARY KEY DEFAULT nextval('seq_entity_source_mappings'),
    entity_type VARCHAR NOT NULL,
    topic       VARCHAR NOT NULL,
    mode        VARCHAR NOT NULL DEFAULT 'entity_only',
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    UNIQUE (entity_type, topic)
);

-- Each source mapping can have multiple matchers — one per message variant
-- that carries the entity ID.
CREATE TABLE entity_source_matchers (
    id            INTEGER PRIMARY KEY DEFAULT nextval('seq_entity_source_matchers'),
    entity_type   VARCHAR NOT NULL,
    topic         VARCHAR NOT NULL,
    message_type  VARCHAR NOT NULL,
    id_source     VARCHAR NOT NULL,
    id_expression VARCHAR,
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    UNIQUE (entity_type, topic, message_type)
);

-- Known-entities registry: PRIMARY KEY enforces deduplication.
CREATE TABLE known_entities (
    entity_type VARCHAR NOT NULL,
    entity_id   VARCHAR NOT NULL,
    first_seen  TIMESTAMP WITH TIME ZONE NOT NULL,
    last_seen   TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (entity_type, entity_id)
);

CREATE TABLE compaction_history (
    id                           INTEGER PRIMARY KEY DEFAULT nextval('seq_compaction_history'),
    started_at                   TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at                 TIMESTAMP WITH TIME ZONE,
    status                       VARCHAR NOT NULL,
    triggered_by                 VARCHAR NOT NULL DEFAULT 'unknown',
    targets                      VARCHAR[],
    entity_buckets_compacted     INTEGER NOT NULL DEFAULT 0,
    general_partitions_compacted INTEGER NOT NULL DEFAULT 0,
    error_message                VARCHAR
);

CREATE TABLE retention_history (
    id                     INTEGER PRIMARY KEY DEFAULT nextval('seq_retention_history'),
    started_at             TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at           TIMESTAMP WITH TIME ZONE,
    status                 VARCHAR NOT NULL,
    triggered_by           VARCHAR NOT NULL,
    entity_rows_deleted    BIGINT NOT NULL DEFAULT 0,
    general_rows_deleted   BIGINT NOT NULL DEFAULT 0,
    known_entities_deleted BIGINT NOT NULL DEFAULT 0,
    error_message          VARCHAR
);

CREATE TABLE snapshots (
    name        VARCHAR  NOT NULL PRIMARY KEY,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    size_bytes  BIGINT
);

-- Message-type matchers for general cassettes.
-- First matcher (in insertion order) whose id_source/id_expression extracts a
-- non-null value from a message wins; its message_type is stored in the cassette row.
CREATE TABLE topic_message_type_matchers (
    topic           VARCHAR NOT NULL,
    message_type    VARCHAR NOT NULL,
    id_source       VARCHAR NOT NULL,
    id_expression   VARCHAR NOT NULL,
    PRIMARY KEY (topic, message_type)
);

-- Named transform pipeline presets.
-- Steps are stored as a JSON array and deserialised via Jackson at read time.
CREATE TABLE transform_presets (
    name        VARCHAR                  NOT NULL PRIMARY KEY,
    description VARCHAR,
    steps       VARCHAR                  NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

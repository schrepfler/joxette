-- =============================================================================
-- jOOQ codegen schema
-- =============================================================================
-- Creates the five config/entity tables so that the jOOQ DDLDatabase can
-- introspect them and emit Java sources.
--
-- Uses standard SQL types (TIMESTAMP WITH TIME ZONE) for compatibility with
-- jOOQ's DDL interpreter.  The runtime schema in db.SchemaManager uses
-- DuckDB aliases (TIMESTAMPTZ) which are equivalent.
-- =============================================================================

CREATE SEQUENCE IF NOT EXISTS seq_entity_source_mappings START WITH 1;
CREATE SEQUENCE IF NOT EXISTS seq_compaction_history START WITH 1;

CREATE TABLE topic_configs (
    topic      VARCHAR PRIMARY KEY,
    mode       VARCHAR NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE TABLE entity_type_configs (
    entity_type  VARCHAR PRIMARY KEY,
    bucket_count INTEGER NOT NULL DEFAULT 256,
    created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE TABLE entity_source_mappings (
    id                   INTEGER PRIMARY KEY DEFAULT nextval('seq_entity_source_mappings'),
    entity_type          VARCHAR NOT NULL,
    topic                VARCHAR NOT NULL,
    entity_id_source     VARCHAR NOT NULL,
    entity_id_expression VARCHAR NOT NULL,
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    UNIQUE (entity_type, topic)
);

CREATE TABLE compaction_history (
    id           INTEGER PRIMARY KEY DEFAULT nextval('seq_compaction_history'),
    table_name   VARCHAR NOT NULL,
    started_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    files_before INTEGER,
    files_after  INTEGER,
    bytes_before BIGINT,
    bytes_after  BIGINT,
    status       VARCHAR NOT NULL
);

CREATE TABLE known_entities (
    entity_type VARCHAR NOT NULL,
    entity_id   VARCHAR NOT NULL,
    bucket      INTEGER NOT NULL,
    first_seen  TIMESTAMP WITH TIME ZONE NOT NULL,
    last_seen   TIMESTAMP WITH TIME ZONE NOT NULL
);

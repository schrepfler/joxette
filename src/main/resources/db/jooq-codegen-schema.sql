-- =============================================================================
-- jOOQ codegen schema
-- =============================================================================
-- Creates the five config/entity tables in an in-memory DuckDB instance so
-- that the jOOQ Maven codegen plugin can introspect them and emit Java sources.
--
-- Intentionally omits:
--   - DuckLake extension / ATTACH (not available during Maven build)
--   - Scalar macros (headers_*)
--   - Cassette tables (general_*, entity_*) — dynamic, not config-plane
--
-- known_entities lives in lake.main at runtime but is generated here in the
-- default main schema so codegen works without a DuckLake catalog.
-- =============================================================================

CREATE SEQUENCE IF NOT EXISTS seq_entity_source_mappings START 1;
CREATE SEQUENCE IF NOT EXISTS seq_compaction_history START 1;

CREATE TABLE topic_configs (
    topic      VARCHAR PRIMARY KEY,
    mode       VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE entity_type_configs (
    entity_type  VARCHAR PRIMARY KEY,
    bucket_count INTEGER NOT NULL DEFAULT 256,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE entity_source_mappings (
    id                   INTEGER PRIMARY KEY DEFAULT nextval('seq_entity_source_mappings'),
    entity_type          VARCHAR NOT NULL,
    topic                VARCHAR NOT NULL,
    entity_id_source     VARCHAR NOT NULL,
    entity_id_expression VARCHAR NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE compaction_history (
    id           INTEGER PRIMARY KEY DEFAULT nextval('seq_compaction_history'),
    table_name   VARCHAR NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
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
    first_seen  TIMESTAMPTZ NOT NULL,
    last_seen   TIMESTAMPTZ NOT NULL
);

# Joxette — Project Brief

## What it is
Joxette is a Kafka topic cassette recorder and replay service. It records Kafka topics into "cassettes" — replayable archives stored in DuckLake backed by object storage (S3/MinIO). Cassettes can be **general** (raw topic stream in order) or **entity-specific** (messages grouped by a business entity across multiple topics).

## Core goals
- Record Kafka topics to durable, queryable storage with minimal cost (few S3 PUTs, no small-files problem)
- Replay recorded messages via REST API (paginated JSON, SSE, NDJSON)
- Support entity-centric replay: replay all events for a given business entity (e.g. a fixture, order, user) across multiple source topics
- Provide a management UI for topics, entities, compaction, and snapshots
- Enable disaster recovery: export catalog to object storage, rebuild registry from cassette data

## Current status
Active development. Core recording pipeline, replay API, compaction, and UI are implemented. Several bugs were fixed in this session (see activeContext.md).

## Key constraints
- Single-process Java service (DuckDB is embedded, single connection)
- Pure Java (no Kotlin)
- Spring Boot 4.0.5 / JDK 26
- Jox for structured concurrency and Kafka consumption
- DuckLake for Parquet-backed storage

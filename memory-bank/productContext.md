# Product Context

## Why Joxette exists
Testing and debugging event-driven systems requires replaying past Kafka messages. Existing options (Kafka retention, MirrorMaker) are either too short-lived, too expensive, or don't support entity-centric replay. Joxette records everything cheaply (DuckLake inlining → S3 Parquet) and lets you replay any slice of it — by topic, time range, partition, or business entity.

## Problems it solves
1. **Replay for integration testing** — replay a fixture's full event history to a test consumer without needing a live Kafka topic with historical data
2. **Debugging** — inspect what messages were recorded for a given entity across multiple topics
3. **Audit trail** — queryable Parquet archive of all Kafka events
4. **Cost-efficient archival** — DuckLake inlining buffers small writes in the catalog DB before flushing to Parquet, avoiding the S3 small-files problem

## How it should work (user perspective)
1. Register topics and entity types via the management UI or REST API
2. Joxette records all messages from those topics automatically
3. Browse the Topics page → drill into a topic → see recorded messages, filter by time/partition/offset
4. Browse the Entities page → drill into an entity type → see all known entity IDs → click an entity → see its full event history across all source topics
5. Trigger compaction, create snapshots, export to object storage — all from the UI
6. If the local catalog file is lost: re-attach DuckLake catalog, use "Rebuild Known Entities" to restore the registry from S3 Parquet data

## User experience goals
- Fast: paginated queries should return in < 500ms for typical data sizes
- Transparent: show inlined vs flushed data, row counts, table sizes
- Safe: destructive actions (truncate, delete entity, rebuild registry) require confirmation dialogs
- Recoverable: snapshot export to object storage + registry rebuild for disaster recovery

# Catalog Scaling: Three-Stage Migration Path

Joxette uses DuckLake for its lakehouse layer. DuckLake requires a **catalog database** that tracks table metadata, inlined row data, snapshot history, and file manifests. The catalog backend is a pure operational concern — the DuckLake schema is identical regardless of which backend is in use. Moving between stages is a connection-string change, not a data migration or a schema rewrite.

This document describes the three stages and the exact configuration change required for each transition.

---

## Stage Overview

```
┌─────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│   Stage 1           │    │   Stage 2             │    │   Stage 3            │
│   Embedded DuckDB   │───▶│   Quack server        │───▶│   PostgreSQL         │
│   (current)         │    │   (DuckDB 1.5.3+)     │    │                      │
│                     │    │   ⚠ beta in 1.5.3 —   │    │                      │
│   Single process    │    │   evaluate at 2.0 GA  │    │                      │
└─────────────────────┘    └──────────────────────┘    └──────────────────────┘
```

| | Stage 1 | Stage 2 | Stage 3 |
|---|---|---|---|
| **Catalog backend** | Embedded DuckDB file | Quack server (DuckDB extension) | PostgreSQL |
| **Processes** | 1 | Multiple | Multiple |
| **Ops overhead** | Zero | Low (one DuckDB process) | Medium (managed PG or self-hosted) |
| **DuckDB semantics** | ✅ | ✅ | ❌ (SQL dialect difference) |
| **HA / replication** | ❌ | ❌ (single Quack server) | ✅ (streaming replication) |
| **Status in 1.5.3** | GA | **Beta** | GA |
| **When to move here** | Default start | Second Joxette process needed | Quack is a bottleneck or HA required |

---

## Stage 1 — Embedded DuckDB (current)

Joxette embeds DuckDB directly via JDBC. The catalog is a single `.ducklake` file on local disk (or a mounted volume). All Kafka consumer threads share one JDBC `Connection`; DuckDB serializes concurrent writes internally. Reads are concurrent via separate `Statement` objects.

**Constraints:**
- One JVM process only. Running a second Joxette instance against the same catalog file will corrupt it.
- Catalog file must be accessible from the single host (local disk, NFS, EFS). Object storage is handled separately by DuckLake for Parquet files.

### Configuration

```yaml
# application.yml
joxette:
  catalog:
    path: "./data/joxette.ducklake"
    object-storage-path: "s3://my-bucket/joxette/data"
```

The DuckLake connection string used internally by `DuckLakeManager`:

```sql
ATTACH 'ducklake:./data/joxette.ducklake' AS lake (DATA_PATH 's3://my-bucket/joxette/data');
```

---

## Stage 2 — Quack Server (DuckDB 1.5.3+, Beta)

DuckDB 1.5.3 ships **Quack** as a core extension. Quack is a lightweight client–server protocol for DuckDB: a single DuckDB process listens on a TCP socket and accepts connections from multiple clients using the same DuckDB wire protocol. DuckLake 1.5.3 adds Quack as a supported catalog backend, meaning multiple Joxette instances can share one catalog.

> ⚠️ **Quack is in beta in DuckDB 1.5.3.** It is production-ready enough to evaluate but should not be adopted for critical workloads until DuckDB 2.0 GA, when Quack is expected to graduate. Revisit this decision at DuckDB 2.0 GA.

**Why Quack before PostgreSQL:**
- Keeps full DuckDB SQL semantics in the catalog (VARIANT, LIST, STRUCT, nested types).
- No additional database engine to operate — Quack runs as a sidecar DuckDB process.
- Zero schema changes: DuckLake's internal tables are identical to Stage 1.
- Connection change only: swap the `ATTACH` URI from a file path to a `quack://` endpoint.

**Constraints:**
- Single Quack server process — no built-in HA or replication. If the Quack process restarts, catalog reads/writes block until it is back.
- Quack is still beta: protocol changes between 1.5.3 and 2.0 are possible.
- Quack enforces DuckDB's single-writer rule internally; multiple Joxette writers are serialized server-side.

### Running the Quack server

Start a DuckDB Quack server sidecar (example using the DuckDB CLI):

```bash
duckdb -c "LOAD quack; SELECT quack_serve('./data/joxette-catalog.duckdb', host := '0.0.0.0', port := 5432);"
```

Or via the DuckDB Quack extension's recommended approach (see DuckDB docs for the authoritative invocation at your DuckDB version):

```bash
duckdb ./data/joxette-catalog.duckdb -c "INSTALL quack; LOAD quack; CALL quack_serve(port := 9999);"
```

Keep this process running as a systemd service, Docker sidecar, or Kubernetes container.

### Configuration change

Only the `catalog.path` value changes. The `object-storage-path` and all other settings are unchanged.

```yaml
# application.yml — Stage 2
joxette:
  catalog:
    # Was: path: "./data/joxette.ducklake"
    path: "quack://quack-host:9999/joxette-catalog"
    object-storage-path: "s3://my-bucket/joxette/data"
```

The DuckLake `ATTACH` statement becomes:

```sql
ATTACH 'ducklake:quack://quack-host:9999/joxette-catalog' AS lake (DATA_PATH 's3://my-bucket/joxette/data');
```

That is the **only change** required in `DuckLakeManager`. No Java code changes, no schema changes, no data migration.

### Migration steps (Stage 1 → Stage 2)

1. **Drain writes**: pause all Joxette topic recorders via `POST /topics/{topic}/pause` or stop the service cleanly.
2. **Copy catalog file**: copy the existing `.ducklake` DuckDB file to the host that will run the Quack server. This is the authoritative catalog — all DuckLake metadata lives here.
3. **Start Quack server**: point it at the copied catalog file.
4. **Update application.yml**: change `catalog.path` to `quack://quack-host:9999/joxette-catalog`.
5. **Start Joxette**: on startup `DuckLakeManager` will `ATTACH` via Quack instead of the local file. No schema initialization runs because the tables already exist.
6. **Resume recorders**: `POST /topics/{topic}/resume` or let the service auto-start per `RecordingCoordinator`.
7. **Verify**: check `/health` — catalog connectivity and inlined data size should be non-zero and stable.

---

## Stage 3 — PostgreSQL

If Quack becomes a throughput bottleneck, or if catalog HA/replication is required, migrate to PostgreSQL. DuckLake supports PostgreSQL as a catalog backend; its internal table schema is identical to Stage 1 and Stage 2 — only the connection string changes.

**When to consider this:**
- Multiple Joxette instances with high write concurrency overwhelm the single Quack process.
- Catalog HA is a hard requirement (primary + replica, automatic failover).
- Ops team already manages PostgreSQL and prefers it.

**Constraints:**
- PostgreSQL does not support DuckDB-native types (VARIANT, LIST, STRUCT) in its own query layer. DuckLake stores these as opaque bytes; catalog metadata is still queriable from the Joxette service via DuckDB, but you cannot directly `SELECT` from the PostgreSQL catalog tables and expect readable results.
- PostgreSQL requires provisioning and operational management.

### Configuration change

```yaml
# application.yml — Stage 3
joxette:
  catalog:
    # Was (Stage 1): path: "./data/joxette.ducklake"
    # Was (Stage 2): path: "quack://quack-host:9999/joxette-catalog"
    path: "postgresql://pg-host:5432/joxette_catalog?user=joxette&password=secret"
    object-storage-path: "s3://my-bucket/joxette/data"
```

The DuckLake `ATTACH` statement becomes:

```sql
ATTACH 'ducklake:postgresql://pg-host:5432/joxette_catalog?user=joxette&password=secret'
    AS lake (DATA_PATH 's3://my-bucket/joxette/data');
```

Again, **no Java code changes, no schema changes, no data migration** — only the URI in `DuckLakeManager.attachStatement()`.

### Migration steps (Stage 2 → Stage 3)

1. **Provision PostgreSQL**: create database `joxette_catalog`, user `joxette` with full privileges.
2. **Drain writes**: pause all topic recorders, quiesce the Quack server.
3. **Export catalog**: use DuckLake's built-in export (or `duckdb_to_pg` tooling) to copy the DuckLake catalog tables from the Quack-managed DuckDB file into PostgreSQL. DuckLake's catalog schema is documented at [ducklake.tech](https://ducklake.tech) — tables are standard SQL, no DuckDB-specific DDL.
4. **Update application.yml**: change `catalog.path` to the PostgreSQL JDBC URI.
5. **Start Joxette**: `DuckLakeManager` attaches to PostgreSQL. No re-initialization needed.
6. **Verify**: `/health`, spot-check a replay query against a recent entity cassette.
7. **Decommission Quack server** once stable.

---

## Summary: What Changes at Each Transition

| Transition | What changes | What stays the same |
|---|---|---|
| Stage 1 → Stage 2 | `catalog.path` URI (file path → `quack://`) | All Java code, all DuckLake schema, all Parquet data on object storage |
| Stage 2 → Stage 3 | `catalog.path` URI (`quack://` → `postgresql://`) + catalog data export | All Java code, all DuckLake schema, all Parquet data on object storage |

The key invariant: **DuckLake's catalog schema is backend-agnostic.** The tables DuckLake creates (snapshots, file manifests, inlined data, column statistics) are identical SQL DDL regardless of whether the backend is an embedded DuckDB file, a Quack server, or PostgreSQL. Joxette's application code never speaks directly to the catalog tables — it issues DuckLake SQL (`CREATE TABLE`, `INSERT`, `SELECT`) through DuckDB's JDBC driver, and DuckDB's DuckLake extension handles catalog persistence transparently.

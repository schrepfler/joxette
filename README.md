# Joxette 📼

> **Kafka Topic Cassette Recorder** — Record Kafka topics into replayable archives stored in DuckLake, backed by object storage.

Joxette records Kafka topics into **cassettes** — durable, queryable archives that can be replayed on demand. Cassettes can capture the raw topic stream in order (**general cassettes**) or aggregate messages by business entity across multiple topics (**entity cassettes**).

Storage is powered by [DuckLake](https://ducklake.select/), using its **data inlining** feature to buffer small writes in a local DuckDB catalog before flushing to Parquet on object storage (S3, GCS, Azure). This minimises S3 PUT costs and avoids the small-files problem without any application-level orchestration.

---

## Contents

- [Features](#features)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [REST API](#rest-api)
- [UI](#ui)
- [Development](#development)
- [Project Structure](#project-structure)

---

## Features

- 🎙️ **General cassettes** — raw topic stream preserved in arrival order, one DuckLake table per topic
- 🏷️ **Entity cassettes** — messages for a specific business entity (order, user, device…) aggregated across multiple topics, indexed for fast lookup
- 🔍 **Flexible replay** — paginated JSON, SSE streaming, or NDJSON for all cassettes
- 🗂️ **Cursor-based pagination** — logical cursors stable across physical storage changes (Parquet compaction, DuckLake inlining)
- 🗜️ **Compaction** — scheduled or on-demand Parquet file merging to optimise read performance
- 🏗️ **Bootstrap config** — seed topics and entity types from YAML on first start; REST API takes over afterwards
- 📊 **Observability** — Spring Actuator health endpoint, Prometheus metrics, Swagger UI at `/swagger-ui.html`
- 🗑️ **GDPR support** — delete all data for a specific entity ID

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  Joxette Service                 │
│                                                  │
│  ┌──────────┐   ┌───────────┐   ┌────────────┐  │
│  │  Kafka    │──▶│  Router   │──▶│  DuckLake  │  │
│  │ Consumer  │   │  (entity  │   │  Writer    │  │
│  │ (per      │   │  extract  │   │  (inline   │  │
│  │  topic)   │   │  + route) │   │  + flush)  │  │
│  └──────────┘   └───────────┘   └─────┬──────┘  │
│                                       │         │
│  ┌──────────┐                   ┌─────▼──────┐  │
│  │  REST    │                   │  DuckDB +  │  │
│  │  API     │──────────────────▶│  DuckLake  │  │
│  │ (replay, │                   │  (catalog  │  │
│  │ manage,  │                   │   + inline │  │
│  │ compact) │                   │   storage) │  │
│  └──────────┘                   └─────┬──────┘  │
│                                       │         │
│  ┌──────────┐                         │         │
│  │  Cron    │─── compact trigger ─────┘         │
│  │ Scheduler│                                   │
│  └──────────┘                                   │
└─────────────────────────────────────────────────┘
          │                          │
          ▼                          ▼
   ┌────────────┐           ┌──────────────┐
   │  DuckDB    │           │   Object     │
   │  Catalog   │           │   Storage    │
   │  (.ducklake)│          │   (S3/GCS/   │
   │            │           │    Azure)    │
   └────────────┘           └──────────────┘
```

Single process. DuckDB is embedded. Each Kafka consumer topic runs in its own [Jox](https://jox.softwaremill.com/) structured-concurrency scope, providing clean start/stop semantics and back-pressure via flow operators. All threads share one DuckDB JDBC connection; DuckDB serialises writes internally.

### Recording Pipeline

```
Jox KafkaSource (per topic)
    │
    ▼ .grouped(batchSize, batchTimeout)      ← batching for throughput
    │
    ▼ .map(batch → route(batch))             ← entity extraction + routing
    │
    ▼ .map(batch → write(batch))             ← DuckLake bulk insert
    │
    ▼ .map(result → commit(result))          ← Kafka offset commit
    │
    drain
```

Back-pressure is natural: slow DuckLake writes → batch buffer fills → Kafka consumption slows → consumer lag increases. Lag is the pressure valve.

---

## Tech Stack

| Component | Technology | Version |
|---|---|---|
| Language | Java (preview features enabled) | JDK 25 |
| Framework | Spring Boot | 3.5.x |
| Build | Maven | — |
| Concurrency / Flows | [Jox](https://jox.softwaremill.com/) (softwaremill) | 0.5.x / 1.1.x |
| Kafka | Apache Kafka clients (via Jox Kafka) | — |
| Database / Catalog | DuckDB JDBC | 1.5.1.0 |
| Lakehouse storage | DuckLake extension | — |
| SQL DSL | jOOQ (DDLDatabase codegen) | 3.19.x |
| API docs | SpringDoc OpenAPI / Swagger UI | 2.8.x |
| Object storage | Delegated to DuckDB httpfs + DuckLake | — |
| Testing | JUnit 5 + Testcontainers (Kafka) | 1.21.x |
| UI | React 19, TanStack Router/Query/Table, Tailwind CSS v4 | — |
| Dev Kafka | Apache Kafka (KRaft, native image) via Docker | — |
| Topic provisioning | [Jikkou](https://www.jikkou.io/) | — |

---

## Quick Start

### Prerequisites

- Java 25 (with `--enable-preview`)
- Maven 3.9+
- Docker + Docker Compose
- Node 20+ and [pnpm](https://pnpm.io/) (for the UI)

### 1. Start Kafka locally

```bash
docker compose up -d
```

This starts a single-node KRaft Kafka broker on `localhost:9092` and runs Jikkou to create the `events` topic.

### 2. Run the backend

```bash
# Set JAVA_25_HOME if not already in PATH
export JAVA_25_HOME=$(/usr/libexec/java_home -v 25)

mvn spring-boot:run
```

The service starts on **http://localhost:8080**.

On first start, Joxette initialises the DuckDB catalog, creates the DuckLake schema, and seeds the `events` topic from the bootstrap config in `application.yml`.

- Swagger UI: http://localhost:8080/swagger-ui.html
- API docs: http://localhost:8080/v3/api-docs
- Health: http://localhost:8080/actuator/health
- Metrics (Prometheus): http://localhost:8080/actuator/prometheus

### 3. Run the UI (optional)

```bash
cd ui
pnpm install
pnpm dev
```

The UI dev server starts on **http://localhost:5173** and proxies API calls to the backend.

---

## Configuration

All configuration lives in `src/main/resources/application.yml`. Override any value with environment variables (Spring's `JOXETTE_*` prefix convention) or a profile-specific file.

```yaml
joxette:

  # DuckLake catalog – the DuckDB file is the lakehouse catalog.
  # Leave object-storage-path blank for local dev (data stored next to catalog file).
  catalog:
    path: "./data/joxette.ducklake"
    object-storage-path: ""           # e.g. s3://my-bucket/joxette/data

  # Inline-buffering thresholds. DuckLake buffers data here before flushing to Parquet.
  inline:
    threshold-mb: 4
    threshold-records: 50000

  # Kafka connection
  kafka:
    bootstrap-servers: "localhost:9092"

  # Recording pipeline batching
  recording:
    batch-size: 10000
    batch-timeout-ms: 1000

  # Compaction (merges small Parquet files)
  compaction:
    schedule: "0 0 3 * * *"          # Spring cron: daily at 03:00
    entity:
      min-files-per-bucket: 10
      target-file-size-mb: 256
      lookback-days: 30
    general:
      enabled: false

  # CORS origins allowed to call the REST API
  cors:
    allowed-origins:
      - http://localhost:5173

  # Bootstrap seed – loaded only when config tables are empty on first start.
  bootstrap:
    topics:
      - topic: "events"
        mode: "general"
    entities: []
```

### Topic Modes

| Mode | Behaviour |
|---|---|
| `general` | Record raw stream only |
| `entity_only` | Extract entities and write to entity cassettes only |
| `both` | Record general stream **and** route to entity cassettes |

### Object Storage (Production)

Set `JOXETTE_CATALOG_OBJECT-STORAGE-PATH` to point DuckLake at your bucket. All S3/GCS/Azure interaction is handled by DuckDB's `httpfs` extension — no Java SDK required.

```bash
JOXETTE_CATALOG_OBJECT-STORAGE-PATH=s3://my-bucket/joxette/data
```

---

## REST API

Swagger UI is available at **http://localhost:8080/swagger-ui.html**.

### Topic Recording Management

| Method | Path | Description |
|---|---|---|
| `GET` | `/topics` | List all configured topic recordings |
| `POST` | `/topics` | Register a topic for recording |
| `GET` | `/topics/{topic}` | Config and stats for a topic |
| `PUT` | `/topics/{topic}` | Update topic config |
| `DELETE` | `/topics/{topic}` | Stop recording (data preserved) |
| `POST` | `/topics/{topic}/pause` | Pause consumption |
| `POST` | `/topics/{topic}/resume` | Resume consumption |

### Entity Type Management

| Method | Path | Description |
|---|---|---|
| `GET` | `/entities` | List all entity types with stats |
| `POST` | `/entities` | Register a new entity type |
| `GET` | `/entities/{entity_type}` | Config, source mappings, stats |
| `PUT` | `/entities/{entity_type}` | Update entity config |
| `DELETE` | `/entities/{entity_type}` | Remove entity type (data preserved) |
| `GET` | `/entities/{entity_type}/sources` | List topic → entity mappings |
| `POST` | `/entities/{entity_type}/sources` | Add a source topic mapping |
| `DELETE` | `/entities/{entity_type}/sources/{topic}` | Remove a source mapping |

### Cassette Replay

All replay endpoints support three response formats via the `Accept` header:

| Accept | Format |
|---|---|
| `application/json` | Paginated JSON (default) |
| `text/event-stream` | SSE streaming |
| `application/x-ndjson` | NDJSON streaming |

**General cassettes:**

| Method | Path | Key Query Params |
|---|---|---|
| `GET` | `/cassettes/topics/{topic}` | `from`, `to`, `partition`, `offset_from`, `limit`, `cursor` |

**Entity cassettes:**

| Method | Path | Description |
|---|---|---|
| `GET` | `/cassettes/entities/{entity_type}` | List known entity IDs |
| `GET` | `/cassettes/entities/{entity_type}/{entity_id}` | Replay entity history |
| `GET` | `/cassettes/entities/{entity_type}/{entity_id}/stats` | Message count, time range, topics |
| `GET` | `/cassettes/entities/{entity_type}/search` | Find entities matching criteria |

**Cassette lifecycle:**

| Method | Path | Description |
|---|---|---|
| `GET` | `/cassettes/topics/{topic}/stats` | File count, size, inlined vs. flushed |
| `POST` | `/cassettes/topics/{topic}/compact` | Trigger general cassette compaction |
| `POST` | `/cassettes/topics/{topic}/truncate` | Delete data before a timestamp |
| `POST` | `/cassettes/entities/{entity_type}/compact` | Trigger entity compaction |
| `POST` | `/cassettes/entities/{entity_type}/truncate` | Delete entity data before a timestamp |
| `POST` | `/cassettes/entities/{entity_type}/{entity_id}/delete` | Delete entity (GDPR) |
| `GET` | `/cassettes/snapshots` | List DuckLake snapshots |
| `POST` | `/cassettes/snapshots/{snapshot_id}/restore` | Restore a snapshot |

**Compaction & operations:**

| Method | Path | Description |
|---|---|---|
| `GET` | `/compaction/status` | Running/idle, last run, next scheduled |
| `POST` | `/compaction/trigger` | Trigger compaction (optional scope in body) |
| `GET` | `/compaction/history` | Past compaction runs with stats |
| `GET` | `/health` | Liveness, consumer lag, catalog size |

### Pagination

Cursor-based pagination encodes the last message's logical position (timestamp + partition + offset for general, or timestamp + source info for entity cassettes). Cursors are stable across compaction and inlining flushes.

```json
{
  "messages": [...],
  "cursor": "eyJ0c...",
  "has_more": true
}
```

---

## UI

The `ui/` directory contains a React 19 single-page application built with:

- **TanStack Router** — file-based SPA routing
- **TanStack Query** — server-state management
- **TanStack Table** — virtualised data grids
- **Tailwind CSS v4** — utility-first styling
- **Vite** — build tooling

### UI Routes

| Route | Description |
|---|---|
| `/` | Dashboard / home |
| `/topics` | Topic recording list and management |
| `/topics/{topic}` | Topic cassette replay |
| `/entities` | Entity type list |
| `/entities/{entityType}` | Entities within a type |
| `/entities/{entityType}/{entityId}` | Entity history replay |
| `/snapshots` | DuckLake snapshot browser |
| `/compaction` | Compaction status and history |
| `/health` | Service health |

### UI Development

```bash
cd ui
pnpm install
pnpm dev        # start dev server at http://localhost:5173
pnpm build      # production build
pnpm test       # run Vitest tests
```

---

## Development

### Running Tests

```bash
export JAVA_25_HOME=$(/usr/libexec/java_home -v 25)
mvn test
```

Integration tests use Testcontainers to spin up a real Kafka broker (Redpanda-compatible image). A `JAVA_25_HOME` environment variable is required because the Surefire plugin needs the Java 25 JVM to support preview-compiled classes.

### Managing Kafka Topics with Jikkou

Topic definitions live in `jikkou/topics.yml`. During `docker compose up`, Jikkou applies them automatically. To apply or reconcile manually:

```bash
# Apply (create / update)
jikkou apply --files jikkou/topics.yml

# Diff only
jikkou diff --files jikkou/topics.yml
```

### Code Generation (jOOQ)

jOOQ classes are generated from `src/main/resources/db/jooq-codegen-schema.sql` at build time via the Maven `generate-sources` phase. To regenerate after schema changes:

```bash
mvn generate-sources
```

Generated sources land in `target/generated-sources/jooq/`.

---

## Project Structure

```
joxette/
├── pom.xml                          # Maven build
├── docker-compose.yml               # Local Kafka (KRaft, apache/kafka-native)
├── jikkou/
│   ├── topics.yml                   # Kafka topic definitions
│   └── jikkou.yml                   # Jikkou CLI config
├── src/
│   ├── main/
│   │   ├── java/com/joxette/
│   │   │   ├── JoxetteApplication.java
│   │   │   ├── bootstrap/           # First-start YAML config loader
│   │   │   ├── config/              # Spring configuration beans & properties
│   │   │   ├── db/                  # DuckDB/DuckLake schema & connection
│   │   │   ├── management/          # Topic & entity CRUD controllers
│   │   │   ├── model/               # Domain model records
│   │   │   ├── recording/           # Jox Kafka consumer + batch writer
│   │   │   ├── replay/              # Cassette replay services & controllers
│   │   │   ├── compaction/          # Compaction service, scheduler, controller
│   │   │   └── repository/          # Config repository (jOOQ)
│   │   └── resources/
│   │       ├── application.yml
│   │       └── db/
│   │           ├── init.sql                 # DuckDB schema DDL
│   │           └── jooq-codegen-schema.sql  # jOOQ codegen input
│   └── test/
│       └── java/com/joxette/
│           ├── compaction/          # Compaction unit tests
│           ├── recording/           # Recorder unit tests
│           ├── replay/              # Replay service unit tests
│           ├── it/                  # Integration tests (Testcontainers)
│           └── support/             # DuckDB test support utilities
└── ui/                              # React 19 frontend (pnpm)
    ├── src/
    │   ├── api/                     # API client
    │   ├── components/              # Shared UI components
    │   ├── hooks/                   # Custom React hooks
    │   ├── routes/                  # TanStack Router file-based routes
    │   └── stores/                  # TanStack Store (global state)
    └── public/
```

---

## Design Notes

**Deduplication on read** — Kafka delivers at-least-once, so duplicates are possible. Joxette stores duplicates and deduplicates at query time using `(topic, partition, offset)` as the unique key. `QUALIFY` / `DISTINCT ON` in DuckDB replay queries handle this transparently.

**Logical cursors** — Replay cursors encode timestamps and offsets, not file paths or row numbers. This means compaction and DuckLake inlining flushes never invalidate a client's cursor.

**Single DuckDB connection** — DuckDB serialises concurrent writes internally. All Kafka consumer threads and the REST API share one JDBC connection. If multi-process writes are needed in future, the DuckLake catalog can be migrated to PostgreSQL with no code changes — only connection config differs.

**Storage delegation** — All object storage interaction (S3, GCS, Azure Blob) is handled by DuckDB's `httpfs` extension and DuckLake's storage management. There is no Java object-storage SDK in the dependency tree.

---

## License

See [LICENSE](LICENSE) for details.

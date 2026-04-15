# Tech Context

## Stack

| Layer | Technology | Version / Notes |
|---|---|---|
| Language | Java (pure, no Kotlin) | JDK 26 |
| Framework | Spring Boot | 4.0.5 |
| Build | Maven | `pom.xml` at root |
| Concurrency | Jox (softwaremill) | structured concurrency + Kafka module |
| Kafka client | `com.softwaremill.jox:kafka:0.5.3` | published to Maven Central 18 Mar 2026 |
| Database | DuckDB JDBC | `org.duckdb:duckdb_jdbc:1.5.1.0` |
| Storage format | DuckLake extension | Parquet on S3 via httpfs |
| Object storage | S3-compatible (AWS S3 or MinIO/RustFS locally) | config: `joxette.s3.*` |
| ORM / SQL builder | jOOQ | used in replay services for type-safe queries |
| UI | React + Vite + TypeScript | under `ui/` |
| UI routing | TanStack Router (file-based) | `ui/src/routes/` |
| UI data fetching | TanStack Query | mutations + queries |
| UI tables | TanStack Table | `@tanstack/react-table` |
| UI forms | TanStack Form | `@tanstack/react-form` |
| UI JSON viewer | `@visual-json/react` | used in topic replay records |
| UI package manager | pnpm | `pnpm-lock.yaml` |
| Testing | JUnit 5, Testcontainers, Awaitility | Kafka + DuckDB integration tests |
| API docs | SpringDoc / OpenAPI 3 | `/v3/api-docs`, `/swagger-ui.html` |

## Development Setup

### Backend
```bash
# Start dependencies (Kafka + MinIO)
docker compose up -d

# Run service
./mvnw spring-boot:run

# Run tests (unit only)
./mvnw test

# Run integration tests
./mvnw verify
```

### Frontend
```bash
cd ui
pnpm install
pnpm dev        # dev server on http://localhost:5173
pnpm build      # production build
```

### Docker Compose services
- **Kafka** (KRaft mode, no ZooKeeper) on `localhost:9092`
- **MinIO** (S3-compatible) on `localhost:9000` (console on `localhost:9001`)
  - Bucket: `joxette-data`
  - Credentials: `joxettedev` / `joxettedev123`
- **RustFS** may also be used (same S3 API)

### Jikkou (topic provisioning)
- `jikkou/topics.yml` — Kafka topic definitions
- `jikkou/jikkou.yml` — Jikkou config
- `jikkou apply --files jikkou/` to provision topics

## Configuration (`application.yml`)

```yaml
joxette:
  catalog:
    path: "./data/joxette.ducklake"
    object-storage-path: "s3://joxette-data/"
  kafka:
    bootstrap-servers: "localhost:9092"
  s3:
    endpoint: "http://localhost:9000"   # blank for real AWS
    access-key: "joxettedev"
    secret-key: "joxettedev123"
    region: "us-east-1"
  recording:
    batch-size: 10000
    batch-timeout-ms: 1000
  compaction:
    schedule: "0 0 3 * * *"            # Spring 6-field cron
```

## Integration Test Patterns

### Async polling — Awaitility (standard)
All integration tests that wait for asynchronous pipeline results (e.g. Kafka consumer →
DuckDB write) use **Awaitility** — not manual `while`/`Thread.sleep` loops.

Canonical form:
```java
await().atMost(Duration.ofSeconds(20))
       .pollInterval(Duration.ofMillis(100))
       .until(() -> {
           try {
               return DuckDBTestSupport.countRows(duckDB, table) >= expected;
           } catch (Exception ignored) {
               return false; // table not yet created by the recorder
           }
       });
```

Awaitility is a transitive dependency of `spring-boot-starter-test` — no extra `pom.xml` entry needed.

The `it` profile sets `batch-timeout-ms: 100` so the recording pipeline flushes quickly
enough that `atMost(20–30 s)` is comfortably safe.

### Kafka container image — `apache/kafka-native`
All integration tests and `TopicRecorderTest` use the official **Apache Kafka native image**
instead of the Confluent CP image:

```java
DockerImageName.parse("apache/kafka-native:4.0.2")
```

- **Why**: GraalVM-compiled native binary; starts in ~1–2 s vs ~10–15 s for the JVM-based
  Confluent image. Significantly reduces CI wall-clock time.
- **Current pinned version**: `4.0.2` (latest as of April 2026).
- **Testcontainers support**: use `org.testcontainers.kafka.KafkaContainer` (the newer module-aware
  class), **not** `org.testcontainers.containers.KafkaContainer`. The new class natively supports
  `apache/kafka-native` without any `.asCompatibleSubstituteFor(...)` shim:
  ```java
  import org.testcontainers.kafka.KafkaContainer;
  // ...
  static KafkaContainer kafka = new KafkaContainer(
          DockerImageName.parse("apache/kafka-native:4.0.2"));
  ```
- **Update policy**: pin an explicit tag; do not use `latest`.

## Important Technical Constraints

### DuckDB single-connection model
- One `Connection` bean (`DuckDBConfig`) shared across the application
- Writers use `duckConn.duplicate()` for their own virtual connections
- All multi-statement sequences use `synchronized(duckDB)` blocks
- **No multi-process writes** — only one JVM instance can write to the catalog file

### VARIANT vs JSON
- `SchemaManager.probeVariant()` tests VARIANT round-trip through DuckLake at startup
- Falls back to JSON if VARIANT fails (DuckDB 1.5 supports VARIANT, older may not)
- Flexible column (`metadata`) in all cassette tables uses whichever type is supported

### Headers column type
- Stored as `STRUCT(key VARCHAR, value BLOB)[]` — NOT JSON string
- `CassetteBatchWriter` must inline struct-array literals in SQL; cannot use JDBC `setString`
- Pattern: `[{'key': 'foo', 'value': '\x62\x61\x72'::BLOB}, ...]`

### Entity type name validation
- Must match `[a-z][a-z0-9_]*` (enforced by `SchemaManager.validateEntityType()` and `EntityReplayService.validateEntityType()`)
- Used directly in SQL table names — validation is the SQL injection guard

### Spring bean dependency order
- `DuckLakeManager` → `SchemaManager` (`dbSchemaManager`) → `ConfigRepository` (`managementConfigRepository`) → `MessageRouter`
- `@DependsOn` annotations enforce this order
- `RecordingStartupRunner` starts recorders after full context ready (`ApplicationRunner`)

## File Layout

```
src/main/java/com/joxette/
├── config/           JoxetteProperties, DuckDBConfig, KafkaConfig, WebConfig, SchedulingConfig
├── db/               DuckLakeManager, SchemaManager
├── management/       TopicController, EntityController, ConfigRepository,
│                     RecordingStartupRunner, TopicConfig, EntityTypeConfig,
│                     EntitySourceConfig, HealthController
├── recording/        RecordingCoordinator, TopicRecorder, CassetteBatchWriter,
│                     EntityCassetteBatchWriter
├── replay/           CassetteController, CassetteLifecycleService,
│                     TopicReplayService, EntityReplayService,
│                     MessageRouter, EntityIdExtractor, KnownEntitiesRepository,
│                     SseReplayHandler, + record types
└── compaction/       CompactionController, CompactionService, CompactionScheduler

ui/src/
├── api/client.ts     All API calls; types for all DTOs
├── routes/           File-based routing (TanStack Router)
│   ├── topics/       index.tsx (list), $topic.tsx (detail + replay)
│   ├── entities/     index.tsx (list + rebuild), $entityType/index.tsx (detail),
│   │                 $entityType/$entityId.tsx (entity replay)
│   ├── compaction/   index.tsx
│   ├── snapshots/    index.tsx (list + create local + export to S3)
│   └── health/       index.tsx
└── components/       Layout, Header, Footer, LoadingSpinner, ErrorMessage,
                      ConfirmDialog, Toast, ThemeToggle
```

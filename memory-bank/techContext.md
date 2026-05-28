# Tech Context

## Stack

| Layer | Technology | Version / Notes |
|---|---|---|
| Language | Java (pure, no Kotlin) | JDK 26 |
| Framework | Spring Boot | 4.0.5 |
| Build | Maven | `pom.xml` at root |
| Concurrency | Jox (softwaremill) | structured concurrency + Kafka module |
| Kafka client | `com.softwaremill.jox:kafka:0.5.3` | published to Maven Central 18 Mar 2026 |
| Database | DuckDB JDBC | `org.duckdb:duckdb_jdbc:1.5.3.0` |
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
    entity:
      row-group-memory-limit-mb: 256    # DuckDB 1.5.3+; SET write_buffer_row_group_memory_limit
  roles:
    recorder: true
    replay: true
    compaction: true
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

Awaitility (4.3.0) arrives transitively via `spring-boot-starter-test` but is now
declared explicitly in `joxette-service/pom.xml` for clarity. The hand-rolled
`while (deadline) { …; Thread.sleep(n) }` helpers in `TopicRecorderTest` and
`RebalanceIntegrationTest` were migrated to `await().atMost(...).untilAsserted(...)`;
the only remaining `Thread.sleep` calls are deterministic timestamp-ordering setup,
blocking-task stubs, and Kafka-admin retry-on-`marked for deletion` loops — none are
assertion guards.

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
- **Current pinned version**: `4.0.2` (latest as of April 2026; Kafka 4.x with KIP-848).
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
- **Confirmed**: 18-test evidence base (`VariantProbeTest`) verifies VARIANT round-trip through DuckLake Parquet on duckdb_jdbc 1.5.3.0 — decimal encoding fix + selection-vector indexing fix both included

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

The project is a Maven multi-module build. Modules are layered so that the
replay engine and its SPIs can be depended on without dragging in Spring,
DuckDB, or Kafka.

```
joxette/                              # parent POM: <modules>, dependencyManagement, pluginManagement
├── pom.xml
├── docker-compose.yml
├── jikkou/
├── docs/                             # PlantUML sources + rendered PNGs
├── memory-bank/                      # Claude memory bank
│
├── joxette-core/                     # Pure Java. No Spring, no DuckDB, no Kafka.
│   └── src/main/java/com/joxette/replay/
│       ├── CassetteRecord.java, EntityRecord.java, PagedResponse.java,
│       │                            ReplayProgress.java, ReplayToTopicRequest.java,
│       │                            ReplayTransformConfig.java, FieldSubstitution.java
│       ├── CassetteSource.java       SPI — read a general cassette
│       ├── EntityCassetteSource.java SPI — read an entity cassette
│       ├── MessageTransformer.java   restamp + JSONPath field substitution (no jOOQ)
│       ├── ReplayEngine.java         orchestrator: source → transform → sink
│       └── sink/
│           ├── RecordSink.java       SPI — blocking send
│           └── SinkException.java
│
├── joxette-kafka/                    # Depends on joxette-core + kafka-clients.
│   └── src/main/java/com/joxette/replay/sink/kafka/
│       └── KafkaRecordSink.java      RecordSink impl (owns byte/header/ts encoding)
│
├── joxette-service/                  # Spring Boot app. Depends on joxette-core + joxette-kafka.
│   └── src/main/java/com/joxette/
│       ├── JoxetteApplication.java
│       ├── config/                   JoxetteProperties, DuckDBConfig, KafkaConfig, WebConfig,
│       │                             SchedulingConfig, S3Config, BrokerConnectionFactory,
│       │                             OpenApiConfig, JacksonConfig
│       ├── db/                       DuckLakeManager, SchemaManager
│       ├── lifecycle/                BackgroundTaskRegistry (SmartLifecycle, phase MAX_VALUE-2048;
│       │                             unified submit/interrupt/join for ad-hoc VTs)
│       ├── management/               TopicController, EntityController, BrokerController,
│       │                             ConfigRepository, RecordingStartupRunner,
│       │                             HealthController, MatcherPreviewController,
│       │                             TransformPresetsController, ConfigController
│       ├── recording/                RecordingCoordinator, TopicRecorder,
│       │                             CassetteBatchWriter, EntityCassetteBatchWriter
│       ├── replay/                   CassetteController, CassetteLifecycleService,
│       │                             TopicReplayService (impl CassetteSource),
│       │                             EntityReplayService (impl EntityCassetteSource),
│       │                             MessageRouter, EntityIdExtractor,
│       │                             KnownEntitiesRepository, SseReplayHandler,
│       │                             ScheduledReplayService, + record types
│       │   ├── sink/kafka/           KafkaRecordSinkFactory (per-broker producer cache)
│       │   └── transform/            TransformPipeline, steps/, Predicate, GuardedStep,
│       │                             TransformPreset, ReplayMetadataInjector
│       └── compaction/               CompactionController, CompactionService,
│                                     CompactionScheduler, RetentionService, RetentionScheduler
│
├── joxette-test-kit/                 # Depends on joxette-core + joxette-kafka. No DuckDB, no Spring.
│   └── src/main/java/com/joxette/testkit/
│       ├── InMemoryCassetteSource.java        impl CassetteSource
│       ├── InMemoryEntityCassetteSource.java  impl EntityCassetteSource
│       ├── CapturingRecordSink.java           impl RecordSink (non-Kafka; captures sends)
│       └── ReplayEngineBuilder.java           fluent builder — wires a ReplayEngine in-process
│
└── ui/src/
    ├── api/client.ts                 All API calls; types for all DTOs
    ├── routes/                       File-based routing (TanStack Router)
    │   ├── topics/                   index.tsx (list), $topic.tsx (detail + replay)
    │   ├── entities/                 index.tsx (list + rebuild),
    │   │                             $entityType/index.tsx (detail),
    │   │                             $entityType/$entityId.tsx (entity replay)
    │   ├── compaction/               index.tsx
    │   ├── snapshots/                index.tsx (list + create local + export to S3)
    │   └── health/                   index.tsx
    └── components/                   Layout, Header, Footer, LoadingSpinner, ErrorMessage,
                                       ConfirmDialog, Toast, ThemeToggle
```

### Module boundaries (enforced by POMs)

| Module | May depend on | Must NOT depend on |
|---|---|---|
| `joxette-core` | slf4j-api, jackson-annotations, swagger-annotations-jakarta, json-path | Spring, DuckDB, jOOQ, Kafka |
| `joxette-kafka` | joxette-core, kafka-clients | Spring, DuckDB, jOOQ |
| `joxette-service` | joxette-core, joxette-kafka, Spring Boot, DuckDB, jOOQ, jox | — |
| `joxette-test-kit` | joxette-core, joxette-kafka | DuckDB, jOOQ, Spring |

The jOOQ codegen plugin and PlantUML `exec` plugin live only under `joxette-service`.
`swagger-annotations-jakarta` is pinned to `2.2.43` in the parent `dependencyManagement`
because the Spring Boot BOM still ships the older non-jakarta variant, and core DTOs use
`@Schema`.

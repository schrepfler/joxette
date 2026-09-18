# Replay & API Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close six concrete hardening gaps in Joxette's replay REST/SSE/NDJSON API — an unenforced DuckDB read-concurrency contract, no authentication on destructive endpoints, no API version prefix, unsigned replay cursors, lazily-validated transform pipelines that can fail mid-stream, and a silently-ignored `last_n` parameter contract.

**Architecture:** Task A is foundational: it resolves a real, load-bearing contradiction between CLAUDE.md's two threading-model claims by adding `synchronized(duckDB)` to the read path (`EntityReplayService`, `TopicReplayService`), matching the lock discipline already used by every other DB-touching class (`CompactionService`, `ConfigRepository`, etc.). Tasks B–F are independent, additive hardening layers on top of `CassetteController` and its collaborators: a Spring Security filter chain gated by a config property (B), a `WebMvcConfigurer` path-prefix (C), an HMAC signature wrapped around the existing base64(JSON) cursor codec (D), eager step-by-step validation inside `CassetteController.validated()` (E), and a shared exclusivity guard called from all three entity-replay handlers (F).

**Tech Stack:** Java 25, Spring Boot 4.0.5 (Spring MVC, virtual-thread executor), DuckDB JDBC 1.5.3.0, JUnit 5, Awaitility (async assertions — never raw Thread.sleep before an assertion), MockMvc/WebTestClient for controller tests

## Global Constraints

- Java 25 language features only (no Kotlin)
- Never use `Thread.sleep` before a test assertion — use `Awaitility.await().atMost(...).untilAsserted(...)`
- Prefer `@ParameterizedTest` (`@CsvSource` for scalars, `@MethodSource` for objects) over multiple near-identical `@Test` methods
- All new JDBC statement creation/execution on the shared `Connection` bean must be wrapped `synchronized(duckDB)`
- All errors must render as RFC 7807 `application/problem+json` via typed subclasses of `com.joxette.api.error.JoxetteException`, handled centrally by `GlobalExceptionHandler` — controllers must not catch-and-translate directly; streaming failures must emit the mirrored SSE `event: error` / NDJSON `{"_error":{...}}` terminal frame per `docs/error-handling.md`

## Task order

Task A must be done first and fully committed before any other task starts: it changes the constructor signature of `EntityReplayService` and `TopicReplayService`, and every other task in this plan either calls those constructors from a test or depends on the read path being correct. Tasks B, C, D, E, and F touch disjoint production files (`config/SecurityConfig.java`, `config/WebConfig.java`, `replay/TopicCursor.java`+`EntityCursor.java`, `replay/CassetteController.java`'s `validated()` method, `replay/CassetteController.java`'s entity-replay handlers) and may be executed in parallel by separate subagents once Task A is committed — with one caveat: **Tasks B and D both add fields to the same new `JoxetteProperties.Security` nested class** (B creates it with `apiKey`; D adds `cursorSigningKey` to it). Run B before D, or if run in parallel, whichever lands second must merge into the `Security` class the other created rather than overwriting it.

---

### Task A: Serialize DuckDB reads and resolve the CLAUDE.md threading contradiction

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/EntityReplayService.java` (imports line 19; field/constructor lines 108–112; `queryEntityEvents` lines 232–259 and 271–307; `fetchEntityPage` lines 535–570; `getEntityStats` lines 606–635)
- Modify: `joxette-service/src/main/java/com/joxette/replay/TopicReplayService.java` (imports line 15; field/constructor lines 90–94; `query` lines 176–203)
- Modify: `joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceTest.java` (line 52)
- Modify: `joxette-service/src/test/java/com/joxette/replay/TopicReplayServiceTest.java` (line 49)
- Modify: `joxette-service/src/test/java/com/joxette/replay/TopicReplayServiceTransformTest.java` (line 67)
- Modify: `joxette-service/src/test/java/com/joxette/replay/BatchReplayTest.java` (line 89)
- Modify: `joxette-service/src/main/java/com/joxette/config/DuckDBConfig.java` (javadoc lines 48–49)
- Modify: `joxette-service/src/main/java/com/joxette/recording/DuckLakeWriteChannel.java` (javadoc lines 42–43)
- Modify: `CLAUDE.md` (line 318, line 326)
- Test (create): `joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceConcurrencyTest.java`
- Test (create): `joxette-service/src/test/java/com/joxette/replay/TopicReplayServiceConcurrencyTest.java`

**Interfaces:**
- Consumes: `com.joxette.support.DuckDBTestSupport.newConnection()/createEntityTable(Connection,String)/createGeneralCassetteTable(Connection,String)/insertEntityRow(...)/insertCassetteRow(...)` (all pre-existing, unmodified)
- Produces: `EntityReplayService(DSLContext dsl, Connection duckDB)` (new constructor signature, was `EntityReplayService(DSLContext dsl)`); `TopicReplayService(DSLContext dsl, Connection duckDB)` (new constructor signature, was `TopicReplayService(DSLContext dsl)`)

> **Why this is foundational:** `DuckDBConfig.dslContext(Connection)` builds the shared `DSLContext` via `DSL.using(duckDbConnection, SQLDialect.DUCKDB)` — jOOQ does not pool separately, it wraps the exact same single `Connection` bean used everywhere else. `EntityReplayService`/`TopicReplayService` are the only two DB-touching classes in the codebase with **zero** `synchronized` blocks, while nine other classes (`ConfigRepository`, `CompactionService`, `RetentionService`, `CassetteLifecycleService`, `BrokerRepository`, `HealthController`, `CompactionLockManager`, `CatalogQueryController`) all wrap JDBC calls in `synchronized (duckDB)`. CLAUDE.md's own "Thread and Scope Responsibilities" table (line 326) and the "DuckDB Write Serialization" prose (line 318) claim reads "bypass the channel entirely... concurrent reads safe" — true only in the narrow sense that reads skip the *write channel*, but false about connection-level concurrency: DuckDB's JDBC driver wraps one native `duckdb_connection` handle that is not safe for concurrent `Statement` execution regardless of read/write, which is the exact rationale CLAUDE.md line 55 already gives for the write-path lock.

- [ ] **Step 1: Write failing tests**

Create `joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceConcurrencyTest.java`:

```java
package com.joxette.replay;

import com.joxette.support.DuckDBTestSupport;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stress test proving that concurrent reads through {@link EntityReplayService} do not
 * corrupt the shared DuckDB {@link Connection}. DuckDB's JDBC driver wraps a single native
 * {@code duckdb_connection} handle that is not thread-safe for concurrent {@code Statement}
 * execution regardless of read/write — see CLAUDE.md "DuckDB as Catalog".
 *
 * <p><b>This is a probabilistic regression test, not a deterministic one.</b> Without
 * {@code synchronized(duckDB)} around every query, concurrent access to the native handle
 * races unpredictably: it may throw a {@link java.sql.SQLException}, return a corrupted
 * row count, or (rarely) pass cleanly on a given run. Run this test 5 times before applying
 * the Step 3 fix — at least one of the five runs is expected to fail. After the fix, every
 * run is deterministic (no lock contention window remains to race on).
 */
class EntityReplayServiceConcurrencyTest {

    private static final String ENTITY_TYPE = "order";
    private static final int ROWS_PER_ENTITY = 40;
    private static final int THREAD_COUNT = 16;
    private static final int QUERIES_PER_THREAD = 25;

    private Connection duckDB;
    private EntityReplayService service;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createEntityTable(duckDB, ENTITY_TYPE);
        Instant base = Instant.parse("2025-01-01T00:00:00Z");
        for (int i = 0; i < ROWS_PER_ENTITY; i++) {
            DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE, "cust-1", 0, "OrderCreated",
                    "orders.events", 0, i, base.plusSeconds(i), base.plusSeconds(i),
                    "cust-1", ("{\"seq\":" + i + "}").getBytes());
        }
        service = new EntityReplayService(DSL.using(duckDB, SQLDialect.DUCKDB), duckDB);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    @Test
    void concurrentQueryEntityEvents_neverThrowsOrCorrupts() throws Exception {
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Integer> rowCounts = new ConcurrentLinkedQueue<>();

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int t = 0; t < THREAD_COUNT; t++) {
            tasks.add(() -> {
                for (int i = 0; i < QUERIES_PER_THREAD; i++) {
                    try {
                        PagedResponse<EntityRecord> page = service.queryEntityEvents(
                                ENTITY_TYPE, "cust-1", null, null, ROWS_PER_ENTITY, null);
                        rowCounts.add(page.data().size());
                    } catch (Throwable ex) {
                        failures.add(ex);
                    }
                }
                return null;
            });
        }

        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<Void>> futures = pool.invokeAll(tasks);
        for (Future<Void> f : futures) {
            f.get();
        }
        pool.shutdown();

        assertThat(failures).as("no thread should observe an exception from the shared DuckDB connection").isEmpty();
        assertThat(rowCounts).as("every concurrent query must see the full, uncorrupted row set")
                .allMatch(count -> count == ROWS_PER_ENTITY);
    }
}
```

Create `joxette-service/src/test/java/com/joxette/replay/TopicReplayServiceConcurrencyTest.java`:

```java
package com.joxette.replay;

import com.joxette.support.DuckDBTestSupport;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stress test mirroring {@link EntityReplayServiceConcurrencyTest} for the general-cassette
 * read path. See that class's javadoc for why this is a probabilistic (not deterministic)
 * regression test: run 5 times to observe a pre-fix failure.
 */
class TopicReplayServiceConcurrencyTest {

    private static final String TOPIC = "orders.events";
    private static final int ROWS = 40;
    private static final int THREAD_COUNT = 16;
    private static final int QUERIES_PER_THREAD = 25;

    private Connection duckDB;
    private TopicReplayService service;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC);
        Instant base = Instant.parse("2025-01-01T00:00:00Z");
        for (int i = 0; i < ROWS; i++) {
            DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, i, base.plusSeconds(i), base.plusSeconds(i),
                    "key-" + i, ("{\"seq\":" + i + "}").getBytes());
        }
        service = new TopicReplayService(DSL.using(duckDB, SQLDialect.DUCKDB), duckDB);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    @Test
    void concurrentQuery_neverThrowsOrCorrupts() throws Exception {
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Integer> rowCounts = new ConcurrentLinkedQueue<>();

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int t = 0; t < THREAD_COUNT; t++) {
            tasks.add(() -> {
                for (int i = 0; i < QUERIES_PER_THREAD; i++) {
                    try {
                        PagedResponse<CassetteRecord> page = service.query(
                                TOPIC, null, null, null, null, null, ROWS, null);
                        rowCounts.add(page.data().size());
                    } catch (Throwable ex) {
                        failures.add(ex);
                    }
                }
                return null;
            });
        }

        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<Void>> futures = pool.invokeAll(tasks);
        for (Future<Void> f : futures) {
            f.get();
        }
        pool.shutdown();

        assertThat(failures).as("no thread should observe an exception from the shared DuckDB connection").isEmpty();
        assertThat(rowCounts).as("every concurrent query must see the full, uncorrupted row set")
                .allMatch(count -> count == ROWS);
    }
}
```

- [ ] **Step 2: Run and confirm failure**

Run each test 5 times before touching production code:

```bash
for i in 1 2 3 4 5; do
  mvn -pl joxette-service -am test -Dtest=EntityReplayServiceConcurrencyTest -q
done
for i in 1 2 3 4 5; do
  mvn -pl joxette-service -am test -Dtest=TopicReplayServiceConcurrencyTest -q
done
```

The tests will **not yet compile** — `new EntityReplayService(DSL.using(duckDB, SQLDialect.DUCKDB), duckDB)` and `new TopicReplayService(DSL.using(duckDB, SQLDialect.DUCKDB), duckDB)` pass a second `duckDB` argument to the current one-argument constructors, so the build fails with:

```
[ERROR] constructor EntityReplayService in class com.joxette.replay.EntityReplayService cannot be applied to given types;
  required: org.jooq.DSLContext
  found:    org.jooq.DSLContext,java.sql.Connection
[ERROR] constructor TopicReplayService in class com.joxette.replay.TopicReplayService cannot be applied to given types;
  required: org.jooq.DSLContext
  found:    org.jooq.DSLContext,java.sql.Connection
```

This confirms the tests are wired correctly against the *target* signature. Once Step 3 adds the `Connection duckDB` parameter, the build will compile and — per the probabilistic-failure note above — is expected to fail at least once across 5 runs with either a `java.sql.SQLException` in `failures` or an `AssertionError` on `rowCounts` reporting a count other than 40, since the queries currently run with zero synchronization.

- [ ] **Step 3: Write minimal implementation**

In `joxette-service/src/main/java/com/joxette/replay/EntityReplayService.java`, add the import (before line 19 `import java.sql.SQLException;`):

```java
import java.sql.Connection;
```

Replace the field/constructor (lines 108–112):

```java
    private final DSLContext dsl;

    public EntityReplayService(DSLContext dsl) {
        this.dsl = dsl;
    }
```

with:

```java
    private final DSLContext dsl;
    private final Connection duckDB;

    public EntityReplayService(DSLContext dsl, Connection duckDB) {
        this.dsl = dsl;
        this.duckDB = duckDB;
    }
```

Replace the `lastN` tail-window block (lines 233–246):

```java
        if (lastN != null) {
            var tailBase = dsl
                    .select(F_ENTITY_ID, F_MESSAGE_TYPE, F_TOPIC, F_PARTITION, F_OFFSET,
                            F_TIMESTAMP, F_RECORDED_AT, F_KEY, F_VALUE, F_HEADERS)
                    .from(entityTable)
                    .where(cond);
            var qualified = qualifyClause != null ? tailBase.qualify(qualifyClause) : tailBase;
            List<EntityRecord> tail = qualified
                    .orderBy(F_TIMESTAMP.desc(), F_RECORDED_AT.desc(),
                             F_TOPIC.desc(), F_PARTITION.desc(), F_OFFSET.desc())
                    .limit(lastN)
                    .fetch(EntityReplayService::mapEntityRecord);
            // Reverse to chronological order
            java.util.Collections.reverse(tail);
```

with:

```java
        if (lastN != null) {
            List<EntityRecord> tail;
            synchronized (duckDB) {
                var tailBase = dsl
                        .select(F_ENTITY_ID, F_MESSAGE_TYPE, F_TOPIC, F_PARTITION, F_OFFSET,
                                F_TIMESTAMP, F_RECORDED_AT, F_KEY, F_VALUE, F_HEADERS)
                        .from(entityTable)
                        .where(cond);
                var qualified = qualifyClause != null ? tailBase.qualify(qualifyClause) : tailBase;
                tail = qualified
                        .orderBy(F_TIMESTAMP.desc(), F_RECORDED_AT.desc(),
                                 F_TOPIC.desc(), F_PARTITION.desc(), F_OFFSET.desc())
                        .limit(lastN)
                        .fetch(EntityReplayService::mapEntityRecord);
            }
            // Reverse to chronological order
            java.util.Collections.reverse(tail);
```

Replace the paginated query block (lines 271–307):

```java
        var baseSelect = dsl
                .select(F_ENTITY_ID, F_MESSAGE_TYPE, F_TOPIC, F_PARTITION, F_OFFSET,
                        F_TIMESTAMP, F_RECORDED_AT, F_KEY, F_VALUE, F_HEADERS)
                .from(entityTable)
                .where(cond);
        var selectBase = qualifyClause != null
                ? baseSelect.qualify(qualifyClause)
                        .orderBy(desc ? F_TIMESTAMP.desc()   : F_TIMESTAMP.asc(),
                                 desc ? F_RECORDED_AT.desc() : F_RECORDED_AT.asc(),
                                 desc ? F_TOPIC.desc()       : F_TOPIC.asc(),
                                 desc ? F_PARTITION.desc()   : F_PARTITION.asc(),
                                 desc ? F_OFFSET.desc()      : F_OFFSET.asc())
                : baseSelect
                        .orderBy(desc ? F_TIMESTAMP.desc()   : F_TIMESTAMP.asc(),
                                 desc ? F_RECORDED_AT.desc() : F_RECORDED_AT.asc(),
                                 desc ? F_TOPIC.desc()       : F_TOPIC.asc(),
                                 desc ? F_PARTITION.desc()   : F_PARTITION.asc(),
                                 desc ? F_OFFSET.desc()      : F_OFFSET.asc());

        List<EntityRecord> records;
        if (decoded != null) {
            // seekAfter is direction-aware in jOOQ: it inspects the ORDER BY
            // and emits the correct comparison for ASC / DESC. The cursor is
            // the same tuple; only the query direction changes.
            records = selectBase
                    .seekAfter(decoded.timestamp().atOffset(ZoneOffset.UTC),
                               decoded.recordedAt().atOffset(ZoneOffset.UTC),
                               decoded.sourceTopic(),
                               decoded.sourcePartition(),
                               decoded.sourceOffset())
                    .limit(limit + 1)
                    .fetch(EntityReplayService::mapEntityRecord);
        } else {
            records = selectBase
                    .limit(limit + 1)
                    .fetch(EntityReplayService::mapEntityRecord);
        }
```

with:

```java
        List<EntityRecord> records;
        synchronized (duckDB) {
            var baseSelect = dsl
                    .select(F_ENTITY_ID, F_MESSAGE_TYPE, F_TOPIC, F_PARTITION, F_OFFSET,
                            F_TIMESTAMP, F_RECORDED_AT, F_KEY, F_VALUE, F_HEADERS)
                    .from(entityTable)
                    .where(cond);
            var selectBase = qualifyClause != null
                    ? baseSelect.qualify(qualifyClause)
                            .orderBy(desc ? F_TIMESTAMP.desc()   : F_TIMESTAMP.asc(),
                                     desc ? F_RECORDED_AT.desc() : F_RECORDED_AT.asc(),
                                     desc ? F_TOPIC.desc()       : F_TOPIC.asc(),
                                     desc ? F_PARTITION.desc()   : F_PARTITION.asc(),
                                     desc ? F_OFFSET.desc()      : F_OFFSET.asc())
                    : baseSelect
                            .orderBy(desc ? F_TIMESTAMP.desc()   : F_TIMESTAMP.asc(),
                                     desc ? F_RECORDED_AT.desc() : F_RECORDED_AT.asc(),
                                     desc ? F_TOPIC.desc()       : F_TOPIC.asc(),
                                     desc ? F_PARTITION.desc()   : F_PARTITION.asc(),
                                     desc ? F_OFFSET.desc()      : F_OFFSET.asc());

            // seekAfter is direction-aware in jOOQ: it inspects the ORDER BY
            // and emits the correct comparison for ASC / DESC. The cursor is
            // the same tuple; only the query direction changes.
            if (decoded != null) {
                records = selectBase
                        .seekAfter(decoded.timestamp().atOffset(ZoneOffset.UTC),
                                   decoded.recordedAt().atOffset(ZoneOffset.UTC),
                                   decoded.sourceTopic(),
                                   decoded.sourcePartition(),
                                   decoded.sourceOffset())
                        .limit(limit + 1)
                        .fetch(EntityReplayService::mapEntityRecord);
            } else {
                records = selectBase
                        .limit(limit + 1)
                        .fetch(EntityReplayService::mapEntityRecord);
            }
        }
```

Replace `fetchEntityPage`'s body (lines 535–570, the `select`/`switch` — leave the method signature at line 532–534 and the `return TopicReplayService.buildPage(...)` at lines 572–576 unchanged):

```java
        var select = dsl
                .select(F_ENTITY_TYPE, F_ENTITY_ID, F_FIRST_SEEN, F_LAST_SEEN,
                        F_MESSAGE_COUNT, F_SOURCE_TOPICS, F_LAST_MESSAGE_TYPE)
                .from(KNOWN_ENTITIES)
                .where(where);

        List<EntityInfo> entities = switch (sortBy) {
```

with:

```java
        List<EntityInfo> entities;
        synchronized (duckDB) {
        var select = dsl
                .select(F_ENTITY_TYPE, F_ENTITY_ID, F_FIRST_SEEN, F_LAST_SEEN,
                        F_MESSAGE_COUNT, F_SOURCE_TOPICS, F_LAST_MESSAGE_TYPE)
                .from(KNOWN_ENTITIES)
                .where(where);

        entities = switch (sortBy) {
```

and add a closing `}` for the `synchronized` block immediately after the switch expression's terminating `};` (originally line 570, now shifted), i.e. the line reading `};` that closes the `switch` gets a `}` appended on the next line:

```java
        };
        }
```

Replace `getEntityStats`'s three query calls (lines 606–635):

```java
        var aggRecord = dsl.fetchOne(
                DSL.resultQuery(dedupCte
                        + "SELECT COUNT(*) AS cnt, MIN(ts) AS first_msg, MAX(ts) AS last_msg"
                        + " FROM deduped",
                        idParam));
        if (aggRecord != null) {
            count = ((Number) aggRecord.get("cnt")).longValue();
            var f = aggRecord.get("first_msg", java.time.OffsetDateTime.class);
            var l = aggRecord.get("last_msg",  java.time.OffsetDateTime.class);
            if (f != null) firstMsg = f.toInstant();
            if (l != null) lastMsg  = l.toInstant();
        }

        Map<String, Long> countByTopic = new LinkedHashMap<>();
        dsl.fetch(
                DSL.resultQuery(dedupCte
                        + "SELECT topic, COUNT(*) AS cnt FROM deduped"
                        + " GROUP BY topic ORDER BY topic",
                        idParam))
           .forEach(r -> countByTopic.put(
                   r.get("topic", String.class),
                   ((Number) r.get("cnt")).longValue()));

        Instant firstSeen = null;
        Instant lastSeen  = null;
        var regRecord = dsl
                .select(F_FIRST_SEEN, F_LAST_SEEN)
                .from(KNOWN_ENTITIES)
                .where(F_ENTITY_TYPE.eq(entityType).and(F_ENTITY_ID.eq(entityId)))
                .fetchOne();
        if (regRecord != null) {
            firstSeen = regRecord.get(F_FIRST_SEEN).toInstant();
            lastSeen  = regRecord.get(F_LAST_SEEN).toInstant();
        }
```

with:

```java
        Map<String, Long> countByTopic = new LinkedHashMap<>();
        Instant firstSeen = null;
        Instant lastSeen  = null;
        synchronized (duckDB) {
            var aggRecord = dsl.fetchOne(
                    DSL.resultQuery(dedupCte
                            + "SELECT COUNT(*) AS cnt, MIN(ts) AS first_msg, MAX(ts) AS last_msg"
                            + " FROM deduped",
                            idParam));
            if (aggRecord != null) {
                count = ((Number) aggRecord.get("cnt")).longValue();
                var f = aggRecord.get("first_msg", java.time.OffsetDateTime.class);
                var l = aggRecord.get("last_msg",  java.time.OffsetDateTime.class);
                if (f != null) firstMsg = f.toInstant();
                if (l != null) lastMsg  = l.toInstant();
            }

            dsl.fetch(
                    DSL.resultQuery(dedupCte
                            + "SELECT topic, COUNT(*) AS cnt FROM deduped"
                            + " GROUP BY topic ORDER BY topic",
                            idParam))
               .forEach(r -> countByTopic.put(
                       r.get("topic", String.class),
                       ((Number) r.get("cnt")).longValue()));

            var regRecord = dsl
                    .select(F_FIRST_SEEN, F_LAST_SEEN)
                    .from(KNOWN_ENTITIES)
                    .where(F_ENTITY_TYPE.eq(entityType).and(F_ENTITY_ID.eq(entityId)))
                    .fetchOne();
            if (regRecord != null) {
                firstSeen = regRecord.get(F_FIRST_SEEN).toInstant();
                lastSeen  = regRecord.get(F_LAST_SEEN).toInstant();
            }
        }
```

(`count`, `firstMsg`, `lastMsg` remain declared as `long count = 0;` / `Instant firstMsg = null;` / `Instant lastMsg = null;` above this block, unchanged.)

In `joxette-service/src/main/java/com/joxette/replay/TopicReplayService.java`, add the import (before line 15 `import java.sql.Array;`, i.e. insert as a new line so both are present):

```java
import java.sql.Connection;
```

Replace the field/constructor (lines 90–94):

```java
    private final DSLContext dsl;

    public TopicReplayService(DSLContext dsl) {
        this.dsl = dsl;
    }
```

with:

```java
    private final DSLContext dsl;
    private final Connection duckDB;

    public TopicReplayService(DSLContext dsl, Connection duckDB) {
        this.dsl = dsl;
        this.duckDB = duckDB;
    }
```

Replace the query block (lines 175–203, from `boolean desc = ...` through the `if (decoded != null) { ... }` fetch):

```java
        boolean desc = order == Order.DESC;
        var selectBase = dsl
                .select(F_PARTITION, F_OFFSET, F_TIMESTAMP, F_RECORDED_AT, F_KEY, F_VALUE, F_HEADERS, F_MESSAGE_TYPE)
                .from(table)
                .where(cond)
                .qualify(QUALIFY_DEDUP)
                .orderBy(desc ? F_TIMESTAMP.desc() : F_TIMESTAMP.asc(),
                         desc ? F_PARTITION.desc() : F_PARTITION.asc(),
                         desc ? F_OFFSET.desc()    : F_OFFSET.asc());

        final String topicFinal = topic;
        List<CassetteRecord> records;
        if (decoded != null) {
            // jOOQ's seekAfter is direction-aware: it inspects the ORDER BY
            // clause and generates `WHERE (cols) > (vals)` for ASC or
            // `WHERE (cols) < (vals)` for DESC.  So a single code path
            // serves both directions correctly as long as the ORDER BY
            // matches.
            records = selectBase
                    .seekAfter(decoded.timestamp().atOffset(ZoneOffset.UTC),
                               decoded.partition(),
                               decoded.offset())
                    .limit(limit + 1)
                    .fetch(r -> mapRecord(topicFinal, r));
        } else {
            records = selectBase
                    .limit(limit + 1)
                    .fetch(r -> mapRecord(topicFinal, r));
        }
```

with:

```java
        boolean desc = order == Order.DESC;
        final String topicFinal = topic;
        List<CassetteRecord> records;
        synchronized (duckDB) {
            var selectBase = dsl
                    .select(F_PARTITION, F_OFFSET, F_TIMESTAMP, F_RECORDED_AT, F_KEY, F_VALUE, F_HEADERS, F_MESSAGE_TYPE)
                    .from(table)
                    .where(cond)
                    .qualify(QUALIFY_DEDUP)
                    .orderBy(desc ? F_TIMESTAMP.desc() : F_TIMESTAMP.asc(),
                             desc ? F_PARTITION.desc() : F_PARTITION.asc(),
                             desc ? F_OFFSET.desc()    : F_OFFSET.asc());

            // jOOQ's seekAfter is direction-aware: it inspects the ORDER BY
            // clause and generates `WHERE (cols) > (vals)` for ASC or
            // `WHERE (cols) < (vals)` for DESC.  So a single code path
            // serves both directions correctly as long as the ORDER BY
            // matches.
            if (decoded != null) {
                records = selectBase
                        .seekAfter(decoded.timestamp().atOffset(ZoneOffset.UTC),
                                   decoded.partition(),
                                   decoded.offset())
                        .limit(limit + 1)
                        .fetch(r -> mapRecord(topicFinal, r));
            } else {
                records = selectBase
                        .limit(limit + 1)
                        .fetch(r -> mapRecord(topicFinal, r));
            }
        }
```

Update the four existing test call sites to pass the new `Connection` argument:

In `joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceTest.java` line 52, replace:
```java
        service = new EntityReplayService(DSL.using(duckDB, SQLDialect.DUCKDB));
```
with:
```java
        service = new EntityReplayService(DSL.using(duckDB, SQLDialect.DUCKDB), duckDB);
```

In `joxette-service/src/test/java/com/joxette/replay/TopicReplayServiceTest.java` line 49, replace:
```java
        service = new TopicReplayService(DSL.using(duckDB, SQLDialect.DUCKDB));
```
with:
```java
        service = new TopicReplayService(DSL.using(duckDB, SQLDialect.DUCKDB), duckDB);
```

In `joxette-service/src/test/java/com/joxette/replay/TopicReplayServiceTransformTest.java` line 67, replace:
```java
        service = new TopicReplayService(dsl);
```
with:
```java
        service = new TopicReplayService(dsl, duckDB);
```

In `joxette-service/src/test/java/com/joxette/replay/BatchReplayTest.java` line 89, replace:
```java
        EntityReplayService entityService =
                new EntityReplayService(DSL.using(conn, SQLDialect.DUCKDB));
```
with:
```java
        EntityReplayService entityService =
                new EntityReplayService(DSL.using(conn, SQLDialect.DUCKDB), conn);
```

Fix the stale javadoc in `joxette-service/src/main/java/com/joxette/config/DuckDBConfig.java` (lines 48–49), replacing:

```java
 * <p>DuckDB serialises writes internally; no external locking is required.
 * Multiple concurrent reads are safe via separate {@code Statement} objects.
 */
```

with:

```java
 * <p>DuckDB serialises writes internally, and reads must be serialised too: the JDBC
 * driver wraps a single native {@code duckdb_connection} handle that is not safe for
 * concurrent {@code Statement} execution regardless of whether the statements are reads
 * or writes. Every DB-touching class (read and write paths alike) wraps its statement
 * execution in {@code synchronized(duckDB)} against this same bean.
 */
```

Fix the stale javadoc in `joxette-service/src/main/java/com/joxette/recording/DuckLakeWriteChannel.java` (lines 42–43), replacing:

```java
 * <p>Reads (replay queries) are NOT routed here — they use separate
 * {@code Statement} objects on the shared connection and proceed concurrently.
```

with:

```java
 * <p>Reads (replay queries) are NOT routed through this write channel — they never
 * enqueue onto {@code Channel<WriteBatch>}. They still execute a separate
 * {@code Statement} on the shared connection, but that execution is wrapped in
 * {@code synchronized(duckDB)} exactly like the drain loop here, so reads and writes
 * (and reads and other reads) never run concurrently against the native handle.
```

Fix `CLAUDE.md` line 318, replacing:

```
Reads (replay API) bypass the channel entirely: each request opens a separate `Statement` on the shared `Connection`. DuckDB permits concurrent reads.
```

with:

```
Reads (replay API) bypass the *write channel* (they never enqueue onto `Channel<WriteBatch>`), but each request's `Statement` execution on the shared `Connection` is still wrapped in `synchronized(duckDB)`, exactly like the write path. DuckDB's native `duckdb_connection` handle does not permit concurrent `Statement` execution regardless of read/write.
```

Fix `CLAUDE.md` line 326 (the "DuckDB reads (replay)" table row), replacing:

```
| DuckDB reads (replay) | Virtual thread per HTTP request | Unbounded (VT) | Separate `Statement` per request; concurrent reads safe |
```

with:

```
| DuckDB reads (replay) | Virtual thread per HTTP request | Unbounded (VT) | Separate `Statement` per request; serialized via `synchronized(duckDB)` alongside writes — concurrent reads are NOT safe on DuckDB's shared native handle |
```

- [ ] **Step 4: Run and confirm pass**

```bash
mvn -pl joxette-service -am test -Dtest=EntityReplayServiceTest,TopicReplayServiceTest,TopicReplayServiceTransformTest,BatchReplayTest,EntityReplayServiceConcurrencyTest,TopicReplayServiceConcurrencyTest -q
```

All six test classes compile and pass. Run the two concurrency tests 5 more times each to confirm the previously-observed intermittent failure no longer reproduces:

```bash
for i in 1 2 3 4 5; do mvn -pl joxette-service -am test -Dtest=EntityReplayServiceConcurrencyTest -q; done
for i in 1 2 3 4 5; do mvn -pl joxette-service -am test -Dtest=TopicReplayServiceConcurrencyTest -q; done
```

- [ ] **Step 5: Commit**

```
fix(replay): serialize DuckDB reads via synchronized(duckDB) to prevent concurrent native handle corruption

EntityReplayService and TopicReplayService were the only DB-touching classes with
zero synchronization, contradicting both CLAUDE.md's own stated rationale for the
write-path lock and the pattern used by every other class (ConfigRepository,
CompactionService, etc). Wrap every read-path Statement execution in
synchronized(duckDB) and correct the two stale "concurrent reads are safe" claims
in CLAUDE.md and two javadoc comments.
```

---

### Task B: Minimal API-key authentication for state-changing endpoints

**Files:**
- Modify: `joxette-service/pom.xml` (add dependency after the actuator block, lines 51–55)
- Modify: `joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java` (add `Security` nested class after `ObjectStore`, line 827; register field near line 24; add getter/setter near line 864)
- Create: `joxette-service/src/main/java/com/joxette/config/SecurityConfig.java`
- Create: `joxette-service/src/main/java/com/joxette/api/error/UnauthorizedException.java`
- Modify: `joxette-service/src/main/java/com/joxette/api/error/ErrorTypes.java` (add `UNAUTHORIZED` constant after line 17)
- Modify: `joxette-service/src/main/java/com/joxette/api/error/ErrorCodes.java` (add `UNAUTHORIZED` constant after line 18)
- Test (create): `joxette-service/src/test/java/com/joxette/it/ApiKeyAuthenticationIT.java`

**Interfaces:**
- Consumes: `com.joxette.config.JoxetteProperties.getSecurity(): Security` (new); `com.joxette.api.error.JoxetteException(HttpStatus, URI, String, String, String)` constructor (pre-existing, used by the new `UnauthorizedException`)
- Produces: `SecurityFilterChain securityFilterChain(HttpSecurity, JoxetteProperties, ObjectMapper)` bean; `UnauthorizedException.missingOrInvalidApiKey(): UnauthorizedException`; new config property `joxette.security.api-key`

Before writing code, confirm the exact dependency coordinates with `mcs`:

```bash
mcs search org.springframework.boot:spring-boot-starter-security -l 5
```

This returns `org.springframework.boot:spring-boot-starter-security:4.1.0` (matching the `<spring-boot-dependencies.version>4.1.0</spring-boot-dependencies.version>` BOM already imported at root `pom.xml` line 43) among other versions — no explicit `<version>` is needed in `joxette-service/pom.xml` since the parent's `spring-boot-dependencies` BOM manages it, exactly like the existing `spring-boot-starter-web`/`spring-boot-starter-actuator` entries.

- [ ] **Step 1: Write failing test**

Create `joxette-service/src/test/java/com/joxette/it/ApiKeyAuthenticationIT.java`:

```java
package com.joxette.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.ErrorCodes;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end confirmation that mutating endpoints require {@code X-API-Key} when
 * {@code joxette.security.api-key} is set, and that GET requests are never gated.
 * Uses the same embedded Kafka + DuckDB wiring as the other IT tests
 * (see {@code com.joxette.it.ProblemDetailContractIT}).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class ApiKeyAuthenticationIT {

    private static final String API_KEY = "test-only-secret-key";

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("joxette.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("joxette.security.api-key", () -> API_KEY);
    }

    @LocalServerPort
    private int port;

    private final ObjectMapper mapper = new ObjectMapper();

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private RestTemplate nonThrowingRestTemplate() {
        RestTemplate rt = new RestTemplate();
        rt.setErrorHandler(new DefaultResponseErrorHandler() {
            @Override public boolean hasError(ClientHttpResponse response) { return false; }
        });
        return rt;
    }

    @Test
    void postWithoutApiKey_returns401ProblemJson() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String body = "{\"type\":\"customer\",\"buckets\":256}";

        ResponseEntity<String> response = nonThrowingRestTemplate().exchange(
                url("/entities"), HttpMethod.POST, new HttpEntity<>(body, headers), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(response.getHeaders().getContentType().toString()).contains("application/problem+json");
        JsonNode node = mapper.readTree(response.getBody());
        assertThat(node.get("errorCode").asText()).isEqualTo(ErrorCodes.UNAUTHORIZED);
        assertThat(node.get("status").asInt()).isEqualTo(401);
        assertThat(node.get("path").asText()).isEqualTo("/entities");
    }

    @Test
    void postWithCorrectApiKey_reachesController() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-API-Key", API_KEY);
        String body = "{\"type\":\"widget\",\"buckets\":256}";

        ResponseEntity<String> response = nonThrowingRestTemplate().exchange(
                url("/entities"), HttpMethod.POST, new HttpEntity<>(body, headers), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    }

    @Test
    void getWithoutApiKey_succeedsUnauthenticated() {
        ResponseEntity<String> response = nonThrowingRestTemplate().getForEntity(url("/entities"), String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}
```

- [ ] **Step 2: Run and confirm failure**

```bash
mvn -pl joxette-service -am test -Dtest=ApiKeyAuthenticationIT -q
```

Expected failure: `postWithoutApiKey_returns401ProblemJson` fails with `expected: 401 UNAUTHORIZED but was: 201 CREATED` (or a `NullPointerException` on `response.getHeaders().getContentType()` if the body wasn't a `ProblemDetail`) — today, `POST /entities` succeeds unconditionally regardless of headers, because no `SecurityFilterChain` exists anywhere in the codebase (confirmed via `grep -rln "SecurityFilterChain\|EnableWebSecurity" joxette-service/src/main/java` returning no hits) and `spring-boot-starter-security` is not on the classpath.

- [ ] **Step 3: Write minimal implementation**

Add to `joxette-service/pom.xml`, immediately after the actuator dependency block (after line 55):

```xml
        <!-- Spring Security: minimal API-key auth for mutating endpoints -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-security</artifactId>
        </dependency>
```

Add `ErrorTypes.UNAUTHORIZED` to `joxette-service/src/main/java/com/joxette/api/error/ErrorTypes.java`, after line 17 (`FORBIDDEN`):

```java
    public static final URI UNAUTHORIZED         = URI.create("https://joxette.dev/problems/unauthorized");
```

Add `ErrorCodes.UNAUTHORIZED` to `joxette-service/src/main/java/com/joxette/api/error/ErrorCodes.java`, after line 18 (`FORBIDDEN`):

```java
    public static final String UNAUTHORIZED         = "ERR_UNAUTHORIZED";
```

Create `joxette-service/src/main/java/com/joxette/api/error/UnauthorizedException.java`:

```java
package com.joxette.api.error;

import org.springframework.http.HttpStatus;

public class UnauthorizedException extends JoxetteException {

    public UnauthorizedException(String detail) {
        super(HttpStatus.UNAUTHORIZED, ErrorTypes.UNAUTHORIZED, "Unauthorized", detail, ErrorCodes.UNAUTHORIZED);
    }

    public static UnauthorizedException missingOrInvalidApiKey() {
        return new UnauthorizedException("Missing or invalid X-API-Key header");
    }
}
```

In `joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java`, add a new nested class after `ObjectStore` closes (after line 827), before the "Root getters/setters" comment:

```java
    // -----------------------------------------------------------------------
    // Security: API-key auth (mutating endpoints) and cursor signing
    // -----------------------------------------------------------------------

    public static class Security {
        /**
         * Shared-secret API key required via the {@code X-API-Key} header on every
         * mutating request (POST/PUT/DELETE/PATCH). GET/HEAD requests are always
         * unauthenticated. Leave blank to disable authentication entirely (default) —
         * a WARN is logged at startup when blank so this is not left unset outside
         * local development.
         */
        private String apiKey = "";

        public String getApiKey() { return apiKey; }
        public void setApiKey(String apiKey) { this.apiKey = apiKey; }
    }
```

Register the field near line 24 (after `private ObjectStore objectStore = new ObjectStore();`):

```java
    private Security security = new Security();
```

Add the getter/setter near line 864 (after `getObjectStore`/`setObjectStore`):

```java
    public Security getSecurity() { return security; }
    public void setSecurity(Security security) { this.security = security; }
```

Create `joxette-service/src/main/java/com/joxette/config/SecurityConfig.java`:

```java
package com.joxette.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.UnauthorizedException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.http.ProblemDetail;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * Minimal API-key authentication for state-changing endpoints.
 *
 * <p>GET/HEAD requests are always permitted unauthenticated. POST/PUT/DELETE/PATCH
 * requests require a matching {@code X-API-Key} header when {@code joxette.security.api-key}
 * is set. When the property is blank or unset, mutating endpoints are also permitted
 * unauthenticated — a startup WARN log flags this so it is not accidentally left that way
 * outside local development.
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http, JoxetteProperties properties,
                                                     ObjectMapper objectMapper) throws Exception {
        String apiKey = properties.getSecurity().getApiKey();
        if (apiKey == null || apiKey.isBlank()) {
            log.warn("joxette.security.api-key is not set — POST/PUT/DELETE/PATCH endpoints are " +
                    "UNAUTHENTICATED. Set joxette.security.api-key for any non-local deployment.");
        }
        http
            .csrf(csrf -> csrf.disable())
            .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                    .requestMatchers(HttpMethod.GET, "/**").permitAll()
                    .requestMatchers(HttpMethod.HEAD, "/**").permitAll()
                    .anyRequest().authenticated())
            .exceptionHandling(e -> e.authenticationEntryPoint(new ApiKeyAuthenticationEntryPoint(objectMapper)))
            .addFilterBefore(new ApiKeyAuthenticationFilter(apiKey), UsernamePasswordAuthenticationFilter.class);
        return http.build();
    }

    /**
     * Authenticates mutating requests (POST/PUT/DELETE/PATCH) against the configured
     * {@code X-API-Key} header. GET/HEAD and, when {@code apiKey} is blank, every request
     * is granted an authenticated anonymous principal so {@code anyRequest().authenticated()}
     * passes without requiring a header.
     */
    static final class ApiKeyAuthenticationFilter extends OncePerRequestFilter {

        private static final String HEADER = "X-API-Key";

        private final String apiKey;

        ApiKeyAuthenticationFilter(String apiKey) {
            this.apiKey = apiKey;
        }

        @Override
        protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                         FilterChain chain) throws ServletException, IOException {
            String method = request.getMethod();
            boolean mutating = "POST".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)
                    || "DELETE".equalsIgnoreCase(method) || "PATCH".equalsIgnoreCase(method);

            if (!mutating || apiKey == null || apiKey.isBlank()) {
                SecurityContextHolder.getContext().setAuthentication(
                        new UsernamePasswordAuthenticationToken("anonymous", null, List.of()));
                chain.doFilter(request, response);
                return;
            }

            if (apiKey.equals(request.getHeader(HEADER))) {
                SecurityContextHolder.getContext().setAuthentication(
                        new UsernamePasswordAuthenticationToken("api-key-client", null,
                                List.of(new SimpleGrantedAuthority("ROLE_API_CLIENT"))));
            }
            chain.doFilter(request, response);
        }
    }

    /**
     * Renders the same RFC 7807 {@code application/problem+json} shape as
     * {@link com.joxette.api.error.GlobalExceptionHandler} for requests rejected before
     * reaching the DispatcherServlet. Spring Security's filter chain runs upstream of
     * {@code @RestControllerAdvice}, so {@link UnauthorizedException} cannot be thrown and
     * caught there — this entry point duplicates the same field construction instead.
     */
    static final class ApiKeyAuthenticationEntryPoint implements AuthenticationEntryPoint {

        private final ObjectMapper objectMapper;

        ApiKeyAuthenticationEntryPoint(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void commence(HttpServletRequest request, HttpServletResponse response,
                              AuthenticationException authException) throws IOException {
            UnauthorizedException ex = UnauthorizedException.missingOrInvalidApiKey();
            ProblemDetail body = ProblemDetail.forStatusAndDetail(ex.status(), ex.detail());
            body.setType(ex.type());
            body.setTitle(ex.title());
            body.setProperty("timestamp", Instant.now().toString());
            body.setProperty("path", request.getRequestURI());
            body.setProperty("errorCode", ex.errorCode());
            response.setStatus(ex.status().value());
            response.setContentType("application/problem+json");
            objectMapper.writeValue(response.getOutputStream(), body);
        }
    }
}
```

- [ ] **Step 4: Run and confirm pass**

```bash
mvn -pl joxette-service -am test -Dtest=ApiKeyAuthenticationIT -q
```

Also run the full existing IT suite to confirm no regressions from adding Spring Security to the classpath (existing ITs never set `joxette.security.api-key`, so mutating requests in those tests remain unauthenticated exactly as before):

```bash
mvn -pl joxette-service -am test -Dtest=ProblemDetailContractIT,SpringDocIT -q
```

- [ ] **Step 5: Commit**

```
feat(security): add minimal API-key authentication for mutating endpoints

Destructive endpoints (entity delete, truncate, snapshot restore, compaction
trigger, etc) were reachable by anyone hitting the port. Add
spring-boot-starter-security with a SecurityFilterChain that permits all
GET/HEAD unauthenticated and requires a configured X-API-Key header on
POST/PUT/DELETE/PATCH. Empty/unset joxette.security.api-key disables
authentication with a startup WARN, keeping local dev unaffected.
```

---

### Task C: API versioning via a global `/v1` path prefix

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/config/WebConfig.java` (add `configurePathMatch` override)
- Test (create): `joxette-service/src/test/java/com/joxette/it/ApiVersioningIT.java`

**Interfaces:**
- Consumes: `org.springframework.web.method.HandlerTypePredicate.forBasePackage(String...): Predicate<Class<?>>` (Spring Framework API, verified against `/websites/spring_io_spring-framework_current_javadoc-api` via context7); `org.springframework.web.servlet.config.annotation.PathMatchConfigurer.addPathPrefix(String, Predicate<Class<?>>): PathMatchConfigurer` (verified same source)
- Produces: every `com.joxette.*` `@RestController` endpoint now additionally responds under `/v1/**`; the un-prefixed path returns 404; `/actuator/**` remains unprefixed and unaffected (Spring Boot Actuator's handler classes live in `org.springframework.boot.actuate.*`, outside the `com.joxette` base package the predicate matches, so no explicit exclusion is needed)

- [ ] **Step 1: Write failing test**

Create `joxette-service/src/test/java/com/joxette/it/ApiVersioningIT.java`:

```java
package com.joxette.it;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Confirms every {@code com.joxette} controller endpoint is now served under {@code /v1},
 * the un-prefixed path 404s, and {@code /actuator/health} remains unprefixed.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class ApiVersioningIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("joxette.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @LocalServerPort
    private int port;

    private RestTemplate restTemplate;
    private RestTemplate nonThrowingRestTemplate;
    private String baseUrl;

    @BeforeEach
    void setUp() {
        restTemplate = new RestTemplate();
        nonThrowingRestTemplate = new RestTemplate();
        nonThrowingRestTemplate.setErrorHandler(new DefaultResponseErrorHandler() {
            @Override public boolean hasError(ClientHttpResponse response) { return false; }
        });
        baseUrl = "http://localhost:" + port;
    }

    @Test
    void prefixedTopicsEndpoint_isReachable() {
        ResponseEntity<String> response = restTemplate.getForEntity(baseUrl + "/v1/topics", String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void unprefixedTopicsEndpoint_returns404() {
        ResponseEntity<String> response = nonThrowingRestTemplate.getForEntity(baseUrl + "/topics", String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void actuatorHealth_remainsUnprefixed() {
        ResponseEntity<String> response = restTemplate.getForEntity(baseUrl + "/actuator/health", String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}
```

- [ ] **Step 2: Run and confirm failure**

```bash
mvn -pl joxette-service -am test -Dtest=ApiVersioningIT -q
```

Expected failure: `prefixedTopicsEndpoint_isReachable` fails with `expected: 200 OK but was: 404 NOT_FOUND` — today `/v1/topics` matches no handler (every controller is mapped at its bare path, confirmed via `grep -rn "@RequestMapping" joxette-service/src/main/java/com/joxette` showing `/cassettes`, `/topics`, `/entities`, etc. with no prefix), while `unprefixedTopicsEndpoint_returns404` and `actuatorHealth_remainsUnprefixed` already pass (no regression risk from those two).

- [ ] **Step 3: Write minimal implementation**

In `joxette-service/src/main/java/com/joxette/config/WebConfig.java`, add imports after line 14 (`import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;`):

```java
import org.springframework.web.method.HandlerTypePredicate;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
```

Add the override after `addCorsMappings` (after line 46, before `addFormatters` at line 48):

```java
    @Override
    public void configurePathMatch(PathMatchConfigurer configurer) {
        configurer.addPathPrefix("/v1", HandlerTypePredicate.forBasePackage("com.joxette"));
    }
```

- [ ] **Step 4: Run and confirm pass**

```bash
mvn -pl joxette-service -am test -Dtest=ApiVersioningIT -q
```

Also re-run `SpringDocIT` — it regenerates `docs/openapi.json` from the live `/v3/api-docs` document and will now capture the `/v1`-prefixed paths automatically:

```bash
mvn -pl joxette-service -am test -Dtest=SpringDocIT -q
git diff docs/openapi.json | head -50
```

**Follow-up (explicitly out of scope for this task):** `docs/openapi.json`'s `servers[0].url` (currently `"http://localhost:59649"`, springdoc-generated from the live server, not hand-maintained) and the `paths` keys will pick up `/v1` automatically the next time `SpringDocIT` runs and commits the regenerated file — no manual JSON editing is needed, but committing that regenerated `docs/openapi.json` diff is a separate follow-up commit, not part of this task's Step 5.

- [ ] **Step 5: Commit**

```
feat(api): version REST endpoints under /v1 path prefix

Add a WebMvcConfigurer#configurePathMatch override that applies an addPathPrefix("/v1", ...)
scoped to HandlerTypePredicate.forBasePackage("com.joxette"), so every com.joxette
controller is now served under /v1 while /actuator/** (a different base package)
is unaffected. docs/openapi.json regeneration via SpringDocIT is a follow-up.
```

---

### Task D: Tamper-resistant replay cursors

**Files:**
- Create: `joxette-service/src/main/java/com/joxette/replay/CursorSignature.java`
- Create: `joxette-service/src/main/java/com/joxette/replay/CursorSigningKey.java`
- Modify: `joxette-service/src/main/java/com/joxette/replay/TopicCursor.java` (`encode`/`decode`, lines 34–45 and 47–60)
- Modify: `joxette-service/src/main/java/com/joxette/replay/EntityCursor.java` (`encode`/`decode`, lines 42–55 and 57–72)
- Modify: `joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java` (add `cursorSigningKey` field/getter/setter to the `Security` nested class created in Task B)
- Test (create): `joxette-service/src/test/java/com/joxette/replay/TopicCursorTest.java`
- Test (create): `joxette-service/src/test/java/com/joxette/replay/EntityCursorTest.java`

**Interfaces:**
- Consumes: `com.joxette.api.error.InvalidCursorException(String, Throwable)` constructor and `InvalidCursorException.malformed(Throwable): InvalidCursorException` static factory (both pre-existing, unmodified)
- Produces: `CursorSignature.sign(String payload): String` (package-private, `com.joxette.replay`); `CursorSignature.verify(String signed): String` (package-private, throws `InvalidCursorException`); `CursorSignature.install(byte[] keyBytes): void` (package-private); `TopicCursor.encode()`/`decode(String)` and `EntityCursor.encode()`/`decode(String)` unchanged public signatures, now HMAC-signed

> **Note:** This task shares `JoxetteProperties.Security` with Task B. If Task B has already run, add `cursorSigningKey` to the existing `Security` class (shown below as a diff against Task B's version). If Task D runs first, create `Security` fresh with only `cursorSigningKey` — Task B will then add `apiKey` to it.

- [ ] **Step 1: Write failing test**

Create `joxette-service/src/test/java/com/joxette/replay/TopicCursorTest.java`:

```java
package com.joxette.replay;

import com.joxette.api.error.InvalidCursorException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicCursorTest {

    private static final TopicCursor SAMPLE =
            new TopicCursor(Instant.parse("2025-06-01T10:00:00Z"), 3, 4567L);

    @Test
    void encode_thenDecode_roundTripsExactly() {
        TopicCursor decoded = TopicCursor.decode(SAMPLE.encode());
        assertThat(decoded).isEqualTo(SAMPLE);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidCursors")
    void decode_rejectsTamperedOrMalformedCursors(String label, Function<String, String> corrupt) {
        String valid = SAMPLE.encode();
        String corrupted = corrupt.apply(valid);
        assertThatThrownBy(() -> TopicCursor.decode(corrupted))
                .as(label)
                .isInstanceOf(InvalidCursorException.class);
    }

    static Stream<Arguments> invalidCursors() {
        return Stream.of(
                Arguments.of("tampered payload (flipped byte)", (Function<String, String>) valid -> {
                    int sep = valid.lastIndexOf('.');
                    String payload = valid.substring(0, sep);
                    String signature = valid.substring(sep + 1);
                    char[] chars = payload.toCharArray();
                    chars[0] = (char) (chars[0] ^ 1);
                    return new String(chars) + "." + signature;
                }),
                Arguments.of("wrong signature", (Function<String, String>) valid -> {
                    int sep = valid.lastIndexOf('.');
                    return valid.substring(0, sep) + ".wrong0000000000000000000000000";
                }),
                Arguments.of("malformed base64 payload (correctly signed, garbage payload)",
                        (Function<String, String>) valid -> CursorSignature.sign("not-valid-base64!!!"))
        );
    }
}
```

Create `joxette-service/src/test/java/com/joxette/replay/EntityCursorTest.java`:

```java
package com.joxette.replay;

import com.joxette.api.error.InvalidCursorException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EntityCursorTest {

    private static final EntityCursor SAMPLE = new EntityCursor(
            Instant.parse("2025-06-01T10:00:00Z"), Instant.parse("2025-06-01T10:00:00.456Z"),
            "orders.events", 1, 8800L);

    @Test
    void encode_thenDecode_roundTripsExactly() {
        EntityCursor decoded = EntityCursor.decode(SAMPLE.encode());
        assertThat(decoded).isEqualTo(SAMPLE);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidCursors")
    void decode_rejectsTamperedOrMalformedCursors(String label, Function<String, String> corrupt) {
        String valid = SAMPLE.encode();
        String corrupted = corrupt.apply(valid);
        assertThatThrownBy(() -> EntityCursor.decode(corrupted))
                .as(label)
                .isInstanceOf(InvalidCursorException.class);
    }

    static Stream<Arguments> invalidCursors() {
        return Stream.of(
                Arguments.of("tampered payload (flipped byte)", (Function<String, String>) valid -> {
                    int sep = valid.lastIndexOf('.');
                    String payload = valid.substring(0, sep);
                    String signature = valid.substring(sep + 1);
                    char[] chars = payload.toCharArray();
                    chars[0] = (char) (chars[0] ^ 1);
                    return new String(chars) + "." + signature;
                }),
                Arguments.of("wrong signature", (Function<String, String>) valid -> {
                    int sep = valid.lastIndexOf('.');
                    return valid.substring(0, sep) + ".wrong0000000000000000000000000";
                }),
                Arguments.of("malformed base64 payload (correctly signed, garbage payload)",
                        (Function<String, String>) valid -> CursorSignature.sign("not-valid-base64!!!"))
        );
    }
}
```

- [ ] **Step 2: Run and confirm failure**

```bash
mvn -pl joxette-service -am test -Dtest=TopicCursorTest,EntityCursorTest -q
```

The build fails to compile: `CursorSignature` does not yet exist (`cannot find symbol: class CursorSignature`). This confirms the test is wired against the target design.

- [ ] **Step 3: Write minimal implementation**

Create `joxette-service/src/main/java/com/joxette/replay/CursorSignature.java`:

```java
package com.joxette.replay;

import com.joxette.api.error.InvalidCursorException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HMAC-SHA256 signing/verification for {@link TopicCursor} and {@link EntityCursor}.
 *
 * <p>Both cursor records use static {@code encode}/{@code decode} methods called from many
 * sites in {@link TopicReplayService} and {@link EntityReplayService}; rather than threading
 * a signing key through every one of them, {@link CursorSigningKey} installs the effective
 * key here once at Spring context startup, exactly like {@code TopicCursor}'s own
 * {@code MAPPER} field is a static constant today. Package-private: only the two cursor
 * records use it.
 */
final class CursorSignature {

    private static final String ALGORITHM = "HmacSHA256";
    // Used only when no CursorSigningKey bean has installed a real key — e.g. a plain
    // unit test constructing TopicCursor/EntityCursor without the Spring context.
    private static final byte[] FALLBACK_KEY =
            "test-only-unconfigured-cursor-key".getBytes(StandardCharsets.UTF_8);
    private static final AtomicReference<byte[]> KEY = new AtomicReference<>();

    private CursorSignature() {}

    static void install(byte[] keyBytes) {
        KEY.set(keyBytes);
    }

    /** Appends {@code .<base64url-hmac>} to {@code payload}. */
    static String sign(String payload) {
        return payload + "." + hmac(effectiveKey(), payload);
    }

    /**
     * Splits {@code signed} into payload and signature at the last {@code '.'}, recomputes
     * the HMAC, and returns the payload if it matches in constant time.
     *
     * @throws InvalidCursorException if the input has no signature separator, or the
     *         recomputed HMAC does not match
     */
    static String verify(String signed) {
        int sep = signed.lastIndexOf('.');
        if (sep < 0) {
            throw InvalidCursorException.malformed(
                    new IllegalArgumentException("cursor is missing its signature"));
        }
        String payload   = signed.substring(0, sep);
        String signature = signed.substring(sep + 1);
        String expected = hmac(effectiveKey(), payload);
        if (!MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8),
                signature.getBytes(StandardCharsets.UTF_8))) {
            throw InvalidCursorException.malformed(
                    new IllegalArgumentException("cursor signature does not match"));
        }
        return payload;
    }

    private static byte[] effectiveKey() {
        byte[] key = KEY.get();
        return key != null ? key : FALLBACK_KEY;
    }

    private static String hmac(byte[] key, String payload) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(key, ALGORITHM));
            byte[] raw = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        }
    }
}
```

Create `joxette-service/src/main/java/com/joxette/replay/CursorSigningKey.java`:

```java
package com.joxette.replay;

import com.joxette.config.JoxetteProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

/**
 * Installs the effective HMAC signing key for {@link TopicCursor}/{@link EntityCursor} into
 * {@link CursorSignature} once at startup.
 *
 * <p>When {@code joxette.security.cursor-signing-key} is unset, a random 256-bit key is
 * generated for this process only. Cursors signed with a random key do not survive a
 * service restart — any client holding a {@code nextCursor} from before the restart gets
 * {@link com.joxette.api.error.InvalidCursorException} (HTTP 400) on the next page request.
 * Set the property explicitly for any deployment where cursors must survive a restart or
 * where multiple instances must accept each other's cursors.
 */
@Component
public class CursorSigningKey {

    private static final Logger log = LoggerFactory.getLogger(CursorSigningKey.class);

    public CursorSigningKey(JoxetteProperties properties) {
        String configured = properties.getSecurity().getCursorSigningKey();
        byte[] keyBytes;
        if (configured == null || configured.isBlank()) {
            keyBytes = new byte[32];
            new SecureRandom().nextBytes(keyBytes);
            log.warn("joxette.security.cursor-signing-key is not set — using a random " +
                    "per-process key. Cursors issued before a restart will fail with " +
                    "ERR_INVALID_CURSOR after restart. Set joxette.security.cursor-signing-key " +
                    "for stable cursors across restarts or multiple instances.");
        } else {
            keyBytes = configured.getBytes(StandardCharsets.UTF_8);
        }
        CursorSignature.install(keyBytes);
    }
}
```

Add `cursorSigningKey` to the `Security` nested class in `joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java` (created by Task B), replacing:

```java
    public static class Security {
        /**
         * Shared-secret API key required via the {@code X-API-Key} header on every
         * mutating request (POST/PUT/DELETE/PATCH). GET/HEAD requests are always
         * unauthenticated. Leave blank to disable authentication entirely (default) —
         * a WARN is logged at startup when blank so this is not left unset outside
         * local development.
         */
        private String apiKey = "";

        public String getApiKey() { return apiKey; }
        public void setApiKey(String apiKey) { this.apiKey = apiKey; }
    }
```

with:

```java
    public static class Security {
        /**
         * Shared-secret API key required via the {@code X-API-Key} header on every
         * mutating request (POST/PUT/DELETE/PATCH). GET/HEAD requests are always
         * unauthenticated. Leave blank to disable authentication entirely (default) —
         * a WARN is logged at startup when blank so this is not left unset outside
         * local development.
         */
        private String apiKey = "";

        /**
         * HMAC-SHA256 secret used to sign replay cursors ({@code nextCursor} values).
         * Leave blank to generate a random per-process key at startup (default) — cursors
         * will not survive a restart in that case, and a WARN is logged. Set explicitly for
         * any deployment where cursors must survive a restart or where multiple instances
         * must accept each other's cursors.
         */
        private String cursorSigningKey = "";

        public String getApiKey() { return apiKey; }
        public void setApiKey(String apiKey) { this.apiKey = apiKey; }

        public String getCursorSigningKey() { return cursorSigningKey; }
        public void setCursorSigningKey(String cursorSigningKey) { this.cursorSigningKey = cursorSigningKey; }
    }
```

Modify `joxette-service/src/main/java/com/joxette/replay/TopicCursor.java`, replacing `encode()` (lines 34–45):

```java
    public String encode() {
        try {
            byte[] json = MAPPER.createObjectNode()
                    .put("ts", timestamp.toString())
                    .put("p", partition)
                    .put("o", offset)
                    .toString().getBytes();
            return Base64.getUrlEncoder().withoutPadding().encodeToString(json);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to encode TopicCursor", e);
        }
    }
```

with:

```java
    public String encode() {
        try {
            byte[] json = MAPPER.createObjectNode()
                    .put("ts", timestamp.toString())
                    .put("p", partition)
                    .put("o", offset)
                    .toString().getBytes();
            String payload = Base64.getUrlEncoder().withoutPadding().encodeToString(json);
            return CursorSignature.sign(payload);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to encode TopicCursor", e);
        }
    }
```

and `decode(String)` (lines 47–60):

```java
    public static TopicCursor decode(String encoded) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(encoded);
            var node = MAPPER.readTree(bytes);
            return new TopicCursor(
                    Instant.parse(node.get("ts").asText()),
                    node.get("p").asInt(),
                    node.get("o").asLong()
            );
        } catch (Exception e) {
            throw new com.joxette.api.error.InvalidCursorException(
                    "Invalid topic cursor: " + encoded, e);
        }
    }
```

with:

```java
    public static TopicCursor decode(String encoded) {
        try {
            String payload = CursorSignature.verify(encoded);
            byte[] bytes = Base64.getUrlDecoder().decode(payload);
            var node = MAPPER.readTree(bytes);
            return new TopicCursor(
                    Instant.parse(node.get("ts").asText()),
                    node.get("p").asInt(),
                    node.get("o").asLong()
            );
        } catch (com.joxette.api.error.InvalidCursorException e) {
            throw e;
        } catch (Exception e) {
            throw new com.joxette.api.error.InvalidCursorException(
                    "Invalid topic cursor: " + encoded, e);
        }
    }
```

Modify `joxette-service/src/main/java/com/joxette/replay/EntityCursor.java`, replacing `encode()` (lines 42–55):

```java
    public String encode() {
        try {
            byte[] json = MAPPER.createObjectNode()
                    .put("ts", timestamp.toString())
                    .put("ra", recordedAt.toString())
                    .put("t", sourceTopic)
                    .put("p", sourcePartition)
                    .put("o", sourceOffset)
                    .toString().getBytes();
            return Base64.getUrlEncoder().withoutPadding().encodeToString(json);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to encode EntityCursor", e);
        }
    }
```

with:

```java
    public String encode() {
        try {
            byte[] json = MAPPER.createObjectNode()
                    .put("ts", timestamp.toString())
                    .put("ra", recordedAt.toString())
                    .put("t", sourceTopic)
                    .put("p", sourcePartition)
                    .put("o", sourceOffset)
                    .toString().getBytes();
            String payload = Base64.getUrlEncoder().withoutPadding().encodeToString(json);
            return CursorSignature.sign(payload);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to encode EntityCursor", e);
        }
    }
```

and `decode(String)` (lines 57–72):

```java
    public static EntityCursor decode(String encoded) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(encoded);
            var node = MAPPER.readTree(bytes);
            return new EntityCursor(
                    Instant.parse(node.get("ts").asText()),
                    Instant.parse(node.get("ra").asText()),
                    node.get("t").asText(),
                    node.get("p").asInt(),
                    node.get("o").asLong()
            );
        } catch (Exception e) {
            throw new com.joxette.api.error.InvalidCursorException(
                    "Invalid entity cursor: " + encoded, e);
        }
    }
```

with:

```java
    public static EntityCursor decode(String encoded) {
        try {
            String payload = CursorSignature.verify(encoded);
            byte[] bytes = Base64.getUrlDecoder().decode(payload);
            var node = MAPPER.readTree(bytes);
            return new EntityCursor(
                    Instant.parse(node.get("ts").asText()),
                    Instant.parse(node.get("ra").asText()),
                    node.get("t").asText(),
                    node.get("p").asInt(),
                    node.get("o").asLong()
            );
        } catch (com.joxette.api.error.InvalidCursorException e) {
            throw e;
        } catch (Exception e) {
            throw new com.joxette.api.error.InvalidCursorException(
                    "Invalid entity cursor: " + encoded, e);
        }
    }
```

- [ ] **Step 4: Run and confirm pass**

```bash
mvn -pl joxette-service -am test -Dtest=TopicCursorTest,EntityCursorTest,TopicReplayServiceTest,EntityReplayServiceTest -q
```

`TopicReplayServiceTest`/`EntityReplayServiceTest` are included because both exercise cursor round-trips through `TopicReplayService.query`/`EntityReplayService.queryEntityEvents` (`seekAfter`/`nextCursor`), and both construct their service under plain JUnit (no Spring context, so `CursorSignature` falls back to `FALLBACK_KEY` — still self-consistent for sign/verify within the same test JVM run).

- [ ] **Step 5: Commit**

```
fix(replay): sign replay cursors with HMAC-SHA256 to prevent tampering

TopicCursor/EntityCursor were plain unsigned base64(JSON), despite
docs/error-handling.md documenting ERR_INVALID_CURSOR as covering "wrong
signature, expired, malformed". Add CursorSignature (HMAC-SHA256, key installed
at startup by CursorSigningKey from joxette.security.cursor-signing-key, random
per-process fallback with a WARN) and verify on every decode.
```

---

### Task E: Eager validation for `TransformPipeline` steps before streaming starts

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/CassetteController.java` (imports after line 38; `validated()` method, lines 2791–2818)
- Test (create): `joxette-service/src/test/java/com/joxette/replay/CassetteControllerTransformValidationTest.java`

**Interfaces:**
- Consumes: `com.jayway.jsonpath.JsonPath.compile(String)` (pre-existing, already used for `FilterDropStep`); `RenameFieldStep.source()`, `MergePatchStep.target()`, `AddComputedFieldStep.target()/expression()`, `CopyToHeaderStep.source()`, `RedirectTopicStep.topic()`, `ConditionalStep.condition()/thenSteps()/elseSteps()`, `GapTransformStep.select()`, `GapSelector.after()/before()`, `MessagePattern.predicate()/quantifier()`, `Predicate.Leaf.field()`, `Predicate.And.predicates()`, `Predicate.Or.predicates()`, `Predicate.Not.predicate()` (all pre-existing record accessors, unmodified)
- Produces: `CassetteController.validated(List<TransformStep>): List<TransformStep>` now eagerly validates every step type instead of only `FilterDropStep`; throws `com.joxette.api.error.ValidationException` (unchanged exception type, broadened trigger conditions)

> **Two distinct, already-existing path syntaxes govern what "malformed" means here** — verified by reading `JsonStepHelper.parentAndLeaf` (lines 133–150) and `TransformPipeline.extractField` (lines 511–535):
> 1. **JsonStepHelper paths** (`RenameFieldStep.source`, `MergePatchStep.target`, `AddComputedFieldStep.target`, and `AddComputedFieldStep.expression` when not `REPLAY_SEQUENCE`/`NOW_EPOCH_MS`/`NOW_ISO`) — must start with `"$."`, or `parentAndLeaf` throws `IllegalArgumentException` (today silently caught and no-op'd at apply time).
> 2. **`$.value` JSONPath fragments** (`FilterDropStep`/`ConditionalStep` predicate leaves, `CopyToHeaderStep.source`, `${...}` placeholders inside `RedirectTopicStep.topic`, and predicates nested inside `GapTransformStep.select`) — when the path starts with `"$.value"`, the suffix must compile via the real `com.jayway.jsonpath.JsonPath.compile`, exactly what `validated()` already does for `FilterDropStep` today.

- [ ] **Step 1: Write failing test**

Create `joxette-service/src/test/java/com/joxette/replay/CassetteControllerTransformValidationTest.java`:

```java
package com.joxette.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.GlobalExceptionHandler;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.management.KafkaTopicAdmin;
import com.joxette.recording.CassetteRecordingBus;
import com.joxette.replay.sink.kafka.KafkaRecordSinkFactory;
import com.joxette.replay.transform.ReplayMetadataInjector;
import com.joxette.replay.transform.TransformPresetRepository;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CassetteControllerTransformValidationTest {

    @Mock TopicReplayService        topicService;
    @Mock EntityReplayService       entityService;
    @Mock SseReplayHandler          sseHandler;
    @Mock CassetteLifecycleService  lifecycle;
    @Mock KafkaRecordSinkFactory    sinkFactory;
    @Mock ScheduledReplayService    scheduledReplayService;
    @Mock ReplayMetadataInjector    metadataInjector;
    @Mock TransformPresetRepository presetRepository;
    @Mock SequenceMatchService      sequenceMatchService;
    @Mock FieldSuggestionsService   fieldSuggestionsService;
    @Mock CassetteRecordingBus      recordingBus;
    @Mock KafkaTopicAdmin           kafkaTopicAdmin;
    @Mock ConfigRepository          configRepository;
    @SuppressWarnings("unchecked")
    @Mock ActorRef<ReplayCoordinatorActor.Cmd> replayCoordinator;
    @SuppressWarnings("unchecked")
    @Mock ActorSystem<Void>         actorSystem;

    private MockMvc mvc;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        JoxetteProperties properties = new JoxetteProperties();

        CassetteController controller = new CassetteController(
                topicService, entityService, sseHandler, lifecycle, sinkFactory,
                scheduledReplayService, metadataInjector, presetRepository, properties,
                mapper, sequenceMatchService, null, null,
                fieldSuggestionsService, recordingBus, kafkaTopicAdmin, configRepository,
                replayCoordinator, actorSystem,
                Executors.newVirtualThreadPerTaskExecutor(),
                new StateFoldService(mapper),
                new DiffService(mapper),
                new TimelineService(),
                new PortraitService());

        FormattingConversionService conversion = new FormattingConversionService();
        conversion.addConverter(String.class, Order.class, Order::parse);

        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .setConversionService(conversion)
                .build();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedTransformSteps")
    void malformedTransformStep_returns400BeforeStreaming(String label, String transformJson) throws Exception {
        mvc.perform(get("/cassettes/topics/orders").param("transform", transformJson))
           .andExpect(status().isBadRequest())
           .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));
    }

    static Stream<Arguments> malformedTransformSteps() {
        return Stream.of(
            Arguments.of("rename_field: source missing '$.' prefix",
                "[{\"type\":\"rename_field\",\"source\":\"value.orderId\",\"new_name\":\"order_id\"}]"),
            Arguments.of("merge_patch: target missing '$.' prefix",
                "[{\"type\":\"merge_patch\",\"target\":\"value\",\"patch\":{}}]"),
            Arguments.of("add_computed_field: target missing '$.' prefix",
                "[{\"type\":\"add_computed_field\",\"target\":\"value.seq\",\"expression\":\"REPLAY_SEQUENCE\"}]"),
            Arguments.of("copy_to_header: source has invalid JSONPath syntax",
                "[{\"type\":\"copy_to_header\",\"source\":\"$.value.[[[\",\"headerKey\":\"x-id\"}]"),
            Arguments.of("redirect_topic: template placeholder has invalid JSONPath syntax",
                "[{\"type\":\"redirect_topic\",\"topic\":\"prefix-${$.value.[[[}\"}]"),
            Arguments.of("conditional: condition field has invalid JSONPath syntax",
                "[{\"type\":\"conditional\",\"condition\":{\"field\":\"$.value.[[[\",\"operator\":\"EQ\",\"value\":\"x\"},\"then_steps\":[]}]"),
            Arguments.of("gap_transform: nested predicate has invalid JSONPath syntax",
                "[{\"type\":\"gap_transform\",\"select\":{\"after\":{\"predicate\":{\"field\":\"$.value.[[[\",\"operator\":\"EQ\",\"value\":\"x\"},\"quantifier\":\"first\"},\"min_duration_ms\":3000},\"operation\":{\"op\":\"cut\"}}]")
        );
    }
}
```

- [ ] **Step 2: Run and confirm failure**

```bash
mvn -pl joxette-service -am test -Dtest=CassetteControllerTransformValidationTest -q
```

Expected: all 7 parameterized cases fail with `expected: 400 but was: 200` — today `validated()` only inspects `step instanceof FilterDropStep`, so all 7 malformed step types (none of which is `FilterDropStep`) pass through unvalidated, `topicService.query(...)` (an unstubbed Mockito mock) returns `null`, and `ResponseEntity.ok(null)` yields `200 OK`.

- [ ] **Step 3: Write minimal implementation**

In `joxette-service/src/main/java/com/joxette/replay/CassetteController.java`, add imports after line 38 (`import com.joxette.replay.transform.steps.FilterDropStep;`):

```java
import com.joxette.replay.transform.Predicate;
import com.joxette.replay.transform.gap.GapSelector;
import com.joxette.replay.transform.gap.MessagePattern;
import com.joxette.replay.transform.steps.AddComputedFieldStep;
import com.joxette.replay.transform.steps.ConditionalStep;
import com.joxette.replay.transform.steps.CopyToHeaderStep;
import com.joxette.replay.transform.steps.GapTransformStep;
import com.joxette.replay.transform.steps.MergePatchStep;
import com.joxette.replay.transform.steps.RedirectTopicStep;
import com.joxette.replay.transform.steps.RenameFieldStep;
```

Replace `validated()` (lines 2791–2818):

```java
    /**
     * Validates a step list: enforces the max-step cap and pre-compiles any JSONPath
     * expressions found in {@link FilterDropStep}s that reference {@code $.value.*} paths.
     */
    private List<TransformStep> validated(List<TransformStep> steps) {
        int max = properties.getReplay().getMaxTransformSteps();
        if (steps.size() > max) {
            throw new ValidationException(
                    "Transform pipeline exceeds the maximum of " + max + " steps");
        }
        for (TransformStep step : steps) {
            if (step instanceof FilterDropStep fds) {
                String field = fds.field();
                if (field != null && field.startsWith("$.value.")) {
                    // Compile the extracted JSONPath portion to catch syntax errors early
                    String jsonPath = "$" + field.substring("$.value".length());
                    try {
                        JsonPath.compile(jsonPath);
                    } catch (InvalidPathException e) {
                        throw new ValidationException(
                                "Invalid JSONPath in filter_drop field '" + field + "': "
                                        + e.getMessage());
                    }
                }
                }
            }
        return steps;
    }
```

with:

```java
    /**
     * Validates a step list: enforces the max-step cap and eagerly checks every step's
     * field-path / template syntax so malformed input fails fast with 400 before any
     * stream starts, instead of silently no-op'ing (JsonStepHelper-style paths) or being
     * caught only when a matching record happens to execute the step
     * ({@code $.value.*} JSONPath fragments).
     */
    private List<TransformStep> validated(List<TransformStep> steps) {
        int max = properties.getReplay().getMaxTransformSteps();
        if (steps.size() > max) {
            throw new ValidationException(
                    "Transform pipeline exceeds the maximum of " + max + " steps");
        }
        for (TransformStep step : steps) {
            validateStep(step);
        }
        return steps;
    }

    private void validateStep(TransformStep step) {
        if (step instanceof FilterDropStep fds) {
            validatePredicateJsonPaths(fds.predicate());
        } else if (step instanceof ConditionalStep cs) {
            validatePredicateJsonPaths(cs.condition());
            for (TransformStep s : cs.thenSteps()) validateStep(s);
            for (TransformStep s : cs.elseSteps()) validateStep(s);
        } else if (step instanceof RenameFieldStep rfs) {
            validateJsonStepHelperPath("rename_field.source", rfs.source());
        } else if (step instanceof MergePatchStep mps) {
            validateJsonStepHelperPath("merge_patch.target", mps.target());
        } else if (step instanceof AddComputedFieldStep acfs) {
            validateJsonStepHelperPath("add_computed_field.target", acfs.target());
            String expr = acfs.expression();
            if (!"REPLAY_SEQUENCE".equals(expr) && !"NOW_EPOCH_MS".equals(expr) && !"NOW_ISO".equals(expr)) {
                validateJsonStepHelperPath("add_computed_field.expression", expr);
            }
        } else if (step instanceof CopyToHeaderStep cths) {
            validateValueJsonPath("copy_to_header.source", cths.source());
        } else if (step instanceof RedirectTopicStep rts) {
            validateTemplatePlaceholders("redirect_topic.topic", rts.topic());
        } else if (step instanceof GapTransformStep gts) {
            validateGapSelector(gts.select());
        }
    }

    /** JsonStepHelper-style paths (see {@code JsonStepHelper#parentAndLeaf}): must start with {@code "$."}. */
    private void validateJsonStepHelperPath(String stepField, String path) {
        if (path != null && !path.startsWith("$.")) {
            throw new ValidationException(
                    "Invalid path in " + stepField + " '" + path + "': must start with '$.'");
        }
    }

    /** {@code $.value.*} fragments (see {@code TransformPipeline#extractField}): must compile as JSONPath. */
    private void validateValueJsonPath(String stepField, String path) {
        if (path != null && path.startsWith("$.value")) {
            String jsonPath = "$" + path.substring("$.value".length());
            try {
                JsonPath.compile(jsonPath);
            } catch (InvalidPathException e) {
                throw new ValidationException(
                        "Invalid JSONPath in " + stepField + " '" + path + "': " + e.getMessage());
            }
        }
    }

    private static final java.util.regex.Pattern TEMPLATE_VAR =
            java.util.regex.Pattern.compile("\\$\\{([^}]+)\\}");

    private void validateTemplatePlaceholders(String stepField, String template) {
        if (template == null || !template.contains("${")) return;
        java.util.regex.Matcher m = TEMPLATE_VAR.matcher(template);
        while (m.find()) {
            validateValueJsonPath(stepField, m.group(1));
        }
    }

    private void validatePredicateJsonPaths(Predicate predicate) {
        if (predicate == null) return;
        if (predicate instanceof Predicate.Leaf leaf) {
            validateValueJsonPath("predicate.field", leaf.field());
        } else if (predicate instanceof Predicate.And and) {
            and.predicates().forEach(this::validatePredicateJsonPaths);
        } else if (predicate instanceof Predicate.Or or) {
            or.predicates().forEach(this::validatePredicateJsonPaths);
        } else if (predicate instanceof Predicate.Not not) {
            validatePredicateJsonPaths(not.predicate());
        }
    }

    private void validateGapSelector(GapSelector selector) {
        if (selector == null) return;
        validateMessagePattern(selector.after());
        validateMessagePattern(selector.before());
    }

    private void validateMessagePattern(MessagePattern pattern) {
        if (pattern == null) return;
        validatePredicateJsonPaths(pattern.predicate());
        if (pattern.quantifier() instanceof MessagePattern.Quantifier.FirstAfter fa) {
            validateMessagePattern(fa.after());
        }
    }
```

- [ ] **Step 4: Run and confirm pass**

```bash
mvn -pl joxette-service -am test -Dtest=CassetteControllerTransformValidationTest -q
```

Also run the existing transform-pipeline tests to confirm no regressions on well-formed steps:

```bash
mvn -pl joxette-service -am test -Dtest=TopicReplayServiceTransformTest -q
```

- [ ] **Step 5: Commit**

```
fix(replay): eagerly validate transform pipeline steps before streaming starts

CassetteController.validated() only pre-compiled JSONPath for FilterDropStep,
letting a malformed rename_field/merge_patch/add_computed_field/copy_to_header/
redirect_topic/conditional/gap_transform step return 200 and start SSE/NDJSON
streaming before failing mid-stream. Extend validation to every step type using
the two syntax rules their apply()/extractField() paths actually enforce
(JsonStepHelper "$."-prefix vs Jayway JsonPath compile for $.value.* fragments),
recursing into ConditionalStep branches and GapTransformStep's nested predicates.
```

---

### Task F: Reject invalid `last_n` + `from`/`to`/`cursor` combinations

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/CassetteController.java` (`getEntityJson` line 799 insertion point; `getEntitySse` line 913 insertion point; `getEntityNdjson` line 1045 insertion point; new helper method)
- Test (create): `joxette-service/src/test/java/com/joxette/replay/CassetteControllerLastNExclusivityTest.java`

**Interfaces:**
- Consumes: none new
- Produces: `CassetteController.validateLastNExclusivity(Integer lastN, Instant from, Instant to, String cursor): void` and its 3-arg overload `validateLastNExclusivity(Integer lastN, Instant from, Instant to): void` (both new, private) — throw `com.joxette.api.error.ValidationException` when `lastN != null` and any of `from`/`to`/`cursor` is also non-null

> **Verified against `EntityReplayService.queryEntityEvents` (lines 232–259):** when `lastN != null`, the method returns at line 258 without ever reading `from`, `to`, or `cursor` — they are silently discarded, not rejected. The JSON handler (`getEntityJson`, line 834) passes all four through together; the SSE/NDJSON handlers (`getEntitySse` line 957, `getEntityNdjson` line 1080) each hardcode `from`/`to` to `null` inside their own `lastN != null` branch before calling `queryEntityEvents`, silently overriding whatever the client sent — this task rejects the combination at the controller boundary instead of silently overriding it in either place. Neither the SSE nor the NDJSON handler has a `cursor` parameter, so their validation call omits it.

- [ ] **Step 1: Write failing test**

Create `joxette-service/src/test/java/com/joxette/replay/CassetteControllerLastNExclusivityTest.java`:

```java
package com.joxette.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.GlobalExceptionHandler;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.management.KafkaTopicAdmin;
import com.joxette.recording.CassetteRecordingBus;
import com.joxette.replay.sink.kafka.KafkaRecordSinkFactory;
import com.joxette.replay.transform.ReplayMetadataInjector;
import com.joxette.replay.transform.TransformPresetRepository;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CassetteControllerLastNExclusivityTest {

    @Mock TopicReplayService        topicService;
    @Mock EntityReplayService       entityService;
    @Mock SseReplayHandler          sseHandler;
    @Mock CassetteLifecycleService  lifecycle;
    @Mock KafkaRecordSinkFactory    sinkFactory;
    @Mock ScheduledReplayService    scheduledReplayService;
    @Mock ReplayMetadataInjector    metadataInjector;
    @Mock TransformPresetRepository presetRepository;
    @Mock SequenceMatchService      sequenceMatchService;
    @Mock FieldSuggestionsService   fieldSuggestionsService;
    @Mock CassetteRecordingBus      recordingBus;
    @Mock KafkaTopicAdmin           kafkaTopicAdmin;
    @Mock ConfigRepository          configRepository;
    @SuppressWarnings("unchecked")
    @Mock ActorRef<ReplayCoordinatorActor.Cmd> replayCoordinator;
    @SuppressWarnings("unchecked")
    @Mock ActorSystem<Void>         actorSystem;

    private MockMvc mvc;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        JoxetteProperties properties = new JoxetteProperties();

        CassetteController controller = new CassetteController(
                topicService, entityService, sseHandler, lifecycle, sinkFactory,
                scheduledReplayService, metadataInjector, presetRepository, properties,
                mapper, sequenceMatchService, null, null,
                fieldSuggestionsService, recordingBus, kafkaTopicAdmin, configRepository,
                replayCoordinator, actorSystem,
                Executors.newVirtualThreadPerTaskExecutor(),
                new StateFoldService(mapper),
                new DiffService(mapper),
                new TimelineService(),
                new PortraitService());

        FormattingConversionService conversion = new FormattingConversionService();
        conversion.addConverter(String.class, Order.class, Order::parse);

        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .setConversionService(conversion)
                .build();
    }

    // -------------------------------------------------------------------------
    // JSON
    // -------------------------------------------------------------------------

    @Test
    void json_lastNWithFrom_returns400() throws Exception {
        mvc.perform(get("/cassettes/entities/order/cust-1")
                .param("last_n", "5")
                .param("from", "2025-01-01T00:00:00Z"))
           .andExpect(status().isBadRequest())
           .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));
    }

    @Test
    void json_lastNWithCursor_returns400() throws Exception {
        mvc.perform(get("/cassettes/entities/order/cust-1")
                .param("last_n", "5")
                .param("cursor", "some-cursor"))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));
    }

    @Test
    void json_lastNAlone_succeeds() throws Exception {
        when(entityService.queryEntityEvents(anyString(), anyString(), any(), any(), anyInt(), any(),
                any(), anyString(), any(), any(), any(), any()))
                .thenReturn(new PagedResponse<>(List.of(), null, false, null, null));

        mvc.perform(get("/cassettes/entities/order/cust-1").param("last_n", "5"))
           .andExpect(status().isOk());
    }

    // -------------------------------------------------------------------------
    // SSE
    // -------------------------------------------------------------------------

    @Test
    void sse_lastNWithTo_returns400() throws Exception {
        mvc.perform(get("/cassettes/entities/order/cust-1")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .param("last_n", "5")
                .param("to", "2025-01-01T00:00:00Z"))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));
    }

    @Test
    void sse_lastNAlone_doesNotThrow() throws Exception {
        when(sseHandler.<EntityRecord>streamSse(any(), any(), any())).thenReturn(new SseEmitter());
        when(entityService.queryEntityEvents(anyString(), anyString(), any(), any(), anyInt(), any(),
                any(), anyString(), any(), any(), any(), any()))
                .thenReturn(new PagedResponse<>(List.of(), null, false, null, null));

        mvc.perform(get("/cassettes/entities/order/cust-1")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .param("last_n", "5"))
           .andExpect(request().asyncStarted());
    }

    // -------------------------------------------------------------------------
    // NDJSON
    // -------------------------------------------------------------------------

    @Test
    void ndjson_lastNWithFromAndTo_returns400() throws Exception {
        mvc.perform(get("/cassettes/entities/order/cust-1")
                .accept(MediaType.parseMediaType("application/x-ndjson"))
                .param("last_n", "5")
                .param("from", "2025-01-01T00:00:00Z")
                .param("to", "2025-01-02T00:00:00Z"))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));
    }

    @Test
    void ndjson_lastNAlone_doesNotThrow() throws Exception {
        when(sseHandler.<EntityRecord>streamNdjson(any(), any()))
                .thenReturn(outputStream -> {});
        when(entityService.queryEntityEvents(anyString(), anyString(), any(), any(), anyInt(), any(),
                any(), anyString(), any(), any(), any(), any()))
                .thenReturn(new PagedResponse<>(List.of(), null, false, null, null));

        mvc.perform(get("/cassettes/entities/order/cust-1")
                .accept(MediaType.parseMediaType("application/x-ndjson"))
                .param("last_n", "5"))
           .andExpect(request().asyncStarted());
    }
}
```

- [ ] **Step 2: Run and confirm failure**

```bash
mvn -pl joxette-service -am test -Dtest=CassetteControllerLastNExclusivityTest -q
```

Expected failures: `json_lastNWithFrom_returns400`, `json_lastNWithCursor_returns400`, `sse_lastNWithTo_returns400`, and `ndjson_lastNWithFromAndTo_returns400` all fail with `expected: 400 but was: 200` — today none of the three handlers validates the combination; `from`/`to`/`cursor` are either silently discarded inside `EntityReplayService.queryEntityEvents`'s `lastN != null` branch (JSON path) or explicitly overridden to `null` before the call (SSE/NDJSON paths). The three `*_lastNAlone_*` tests already pass (no regression risk).

- [ ] **Step 3: Write minimal implementation**

In `joxette-service/src/main/java/com/joxette/replay/CassetteController.java`, add the shared helper near `validated()` (e.g. immediately before it, so both live in the same "request validation helpers" region):

```java
    private void validateLastNExclusivity(Integer lastN, Instant from, Instant to, String cursor) {
        if (lastN == null) return;
        if (from != null || to != null || cursor != null) {
            throw new ValidationException(
                    "last_n is mutually exclusive with from, to, and cursor");
        }
    }

    private void validateLastNExclusivity(Integer lastN, Instant from, Instant to) {
        validateLastNExclusivity(lastN, from, to, null);
    }
```

In `getEntityJson`, insert as the first statement of the method body (immediately before line 799 `Instant scheduledAt = resolveScheduledAt(startAt, startDelayMs);`):

```java
        validateLastNExclusivity(lastN, from, to, cursor);
```

In `getEntitySse`, insert as the first statement of the method body (immediately before line 913 `rejectFollowWithUpperBound(follow, to, null);`):

```java
        validateLastNExclusivity(lastN, from, to);
```

In `getEntityNdjson`, insert as the first statement of the method body (immediately before line 1045 `rejectFollowWithUpperBound(follow, to, null);`):

```java
        validateLastNExclusivity(lastN, from, to);
```

- [ ] **Step 4: Run and confirm pass**

```bash
mvn -pl joxette-service -am test -Dtest=CassetteControllerLastNExclusivityTest -q
```

Also run the existing entity-replay tests to confirm the plain `last_n` and plain `from`/`to` paths (never combined) are unaffected:

```bash
mvn -pl joxette-service -am test -Dtest=BatchReplayTest,EntityReplayServiceTest -q
```

- [ ] **Step 5: Commit**

```
fix(replay): reject last_n combined with from/to/cursor instead of silently overriding

docs/openapi.json and the controller's own @Parameter javadoc both document
last_n as mutually exclusive with from/to/cursor, but EntityReplayService.
queryEntityEvents silently discarded them, and the SSE/NDJSON handlers each
independently hardcoded from/to to null in their own last_n branch. Add a
shared validateLastNExclusivity() called first in getEntityJson/getEntitySse/
getEntityNdjson so the illegal combination now returns 400 instead of quietly
doing something other than what the client asked for.
```

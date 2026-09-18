# Cassette Dataset Summary Panel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a persistent leaderboard-style "dataset summary" sidebar to topic and entity cassette pages, showing true (backend-computed) cardinality breakdowns by partition/source-topic and message type, with click-to-filter/highlight interaction.

**Architecture:** Two new backend `GET .../summary` endpoints (one per cassette type) compute deduplicated top-20-plus-"other" `GROUP BY` breakdowns via jOOQ against the shared DuckDB connection, mirroring existing read patterns in `TopicReplayService`/`EntityReplayService`. A new `DatasetSummaryPanel` React component fetches this via TanStack Query and renders alongside the existing tab content on four routes (two detail pages, two dedicated timeline pages); clicking a row either sets an existing/new server-side filter or dims non-matching records already loaded in `CassetteTimeline`/the records table, depending on what filter plumbing already exists on that route.

**Tech Stack:** Java 25, Spring Boot 4, jOOQ 3.21.7 against embedded DuckDB; React 19, TanStack Query, TanStack Router, Vitest + Testing Library.

## Global Constraints

- Backend: all DuckDB reads run inside `synchronized (duckDB)`, wrapped in `TopicReplayService.withObjectStoreRetry(...)` for transient object-store I/O retry — no new locking pattern.
- Backend: no `ResourceNotFoundException` on unknown topic/entity for this endpoint family — an unknown table surfaces via whatever `GlobalExceptionHandler` already does for `getTopicStats`/`getEntityStats` (do not add a new existence check).
- Backend: summary counts must dedupe the same way normal reads do (QUALIFY `ROW_NUMBER()` over `(kafka_partition, kafka_offset)` for topics, `(topic, kafka_partition, kafka_offset)` for entities, keeping latest `recorded_at`) — do not count raw undeduplicated rows.
- Frontend: style the panel against the real tokens in `ui/src/design/tokens.css` (`--accent`, `--ink-secondary`, `--ink-tertiary`, `--surface-raised`, `--rule`, `--rule-strong`, `--font-mono`) — `DESIGN.md`'s oxblood/Fraunces description does not match the live tokens and must not be used.
- Frontend: no shared test-utils file exists in this codebase; each test file inlines its own `QueryClient`/`QueryClientProvider` wrap and `vi.mock('../api/client', ...)` — do not add a shared helper for one component's tests.
- Every task ends in a real `git commit`.

---

## Task 1: Backend DTOs — `ValueCount`, `CassetteSummary`

**Files:**
- Create: `joxette-service/src/main/java/com/joxette/replay/ValueCount.java`
- Create: `joxette-service/src/main/java/com/joxette/replay/CassetteSummary.java`

**Interfaces:**
- Produces: `com.joxette.replay.ValueCount(String value, int count)`, `com.joxette.replay.CassetteSummary(long totalRecords, Instant from, Instant to, Map<String, List<ValueCount>> dimensions)` — consumed by Tasks 2, 3, 4.

These are plain data DTOs (no business logic), so no test is written for them — they're exercised indirectly by Task 2/3's service tests. `dimensions` is a plain `Map<String, List<ValueCount>>` rather than a separate `DimensionBreakdown` wrapper type: Jackson serializes a `Map` directly to the `{"partition": [...], "messageType": [...]}` shape the frontend needs, so a wrapper type would add a layer with no behavior.

- [ ] **Step 1: Create `ValueCount.java`**

```java
package com.joxette.replay;

import io.swagger.v3.oas.annotations.media.Schema;

/** One (value, count) pair in a {@link CassetteSummary} dimension breakdown. */
public record ValueCount(
    @Schema(description = "The dimension value, null if the underlying field was absent on matching records, "
                         + "or the literal \"__other__\" for the folded remainder beyond the top 20 values.",
            example = "3")
    String value,
    @Schema(description = "Number of deduplicated records with this value", example = "40213")
    long count
) {}
```

- [ ] **Step 2: Create `CassetteSummary.java`**

```java
package com.joxette.replay;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Dimension-cardinality breakdown for a cassette, scoped to an optional
 * time window. Counts are computed after the same at-least-once
 * deduplication normal replay reads apply.
 */
@Schema(description = "Dimension-cardinality breakdown for a cassette, scoped to an optional time window. "
                     + "Each dimension's value list is capped at the top 20 by count; any remainder is folded "
                     + "into a single {\"value\": \"__other__\"} entry.",
        example = """
            {
              "totalRecords": 128456,
              "from": "2026-08-24T00:00:00Z",
              "to": "2026-08-24T12:00:00Z",
              "dimensions": {
                "partition": [
                  {"value": "3", "count": 40213},
                  {"value": "1", "count": 38010}
                ],
                "messageType": [
                  {"value": "OrderCreated", "count": 88012},
                  {"value": null, "count": 1200}
                ]
              }
            }""")
public record CassetteSummary(
    @Schema(description = "Total deduplicated records in the scoped window", example = "128456")
    long totalRecords,
    @Schema(description = "Start of the scoped time window, or null if unbounded", example = "2026-08-24T00:00:00Z")
    Instant from,
    @Schema(description = "End of the scoped time window, or null if unbounded", example = "2026-08-24T12:00:00Z")
    Instant to,
    @Schema(description = "Per-dimension top-20 value counts, keyed by dimension name "
                         + "(\"partition\"+\"messageType\" for topics, \"sourceTopic\"+\"messageType\" for entities)")
    Map<String, List<ValueCount>> dimensions
) {}
```

- [ ] **Step 3: Compile check**

Run: `cd joxette-service && mvn -q -pl . compile`
Expected: `BUILD SUCCESS` (no test run needed yet — these are inert DTOs).

- [ ] **Step 4: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/replay/ValueCount.java joxette-service/src/main/java/com/joxette/replay/CassetteSummary.java
git commit -m "feat(replay): add CassetteSummary/ValueCount DTOs for dataset summary endpoints"
```

---

## Task 2: `TopicReplayService.getTopicSummary()`

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/TopicReplayService.java`
- Test: `joxette-service/src/test/java/com/joxette/replay/TopicReplayServiceTest.java`

**Interfaces:**
- Consumes: `ValueCount`, `CassetteSummary` (Task 1); existing `F_PARTITION`, `F_MESSAGE_TYPE`, `F_TIMESTAMP`, `QUALIFY_DEDUP`, `tableFor(String)`, `withObjectStoreRetry(String, Supplier<T>)` (all already defined in this file).
- Produces: `public CassetteSummary getTopicSummary(String topic, Instant from, Instant to) throws SQLException` — consumed by Task 4 (`CassetteController`).

- [ ] **Step 1: Write the failing tests**

Add to `TopicReplayServiceTest.java`, in a new section after the existing tests (before the closing `}` of the class):

```java
    // -------------------------------------------------------------------------
    // getTopicSummary
    // -------------------------------------------------------------------------

    @Test
    void getTopicSummary_breaksDownByPartitionAndMessageType() throws Exception {
        Instant ts = Instant.parse("2024-01-01T10:00:00Z");
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 0L, ts, Instant.now(), "k0", b("v0"), "OrderCreated");
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 1L, ts.plusSeconds(1), Instant.now(), "k1", b("v1"), "OrderCreated");
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 1, 0L, ts.plusSeconds(2), Instant.now(), "k2", b("v2"), "OrderCancelled");

        CassetteSummary summary = service.getTopicSummary(TOPIC, null, null);

        assertThat(summary.totalRecords()).isEqualTo(3);
        assertThat(summary.dimensions().get("partition"))
                .containsExactlyInAnyOrder(new ValueCount("0", 2), new ValueCount("1", 1));
        assertThat(summary.dimensions().get("messageType"))
                .containsExactlyInAnyOrder(new ValueCount("OrderCreated", 2), new ValueCount("OrderCancelled", 1));
    }

    @Test
    void getTopicSummary_dedupesSameOffsetRecordedTwice() throws Exception {
        Instant ts = Instant.parse("2024-01-01T10:00:00Z");
        // Same (partition, offset) inserted twice with different recorded_at — simulates
        // an at-least-once redelivery. Only the most-recently-recorded row should count.
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 0L, ts, Instant.now().minusSeconds(10), "k0", b("v0"), "A");
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 0L, ts, Instant.now(), "k0", b("v0"), "A");

        CassetteSummary summary = service.getTopicSummary(TOPIC, null, null);

        assertThat(summary.totalRecords()).isEqualTo(1);
        assertThat(summary.dimensions().get("partition")).containsExactly(new ValueCount("0", 1));
    }

    @Test
    void getTopicSummary_scopesToFromToWindow() throws Exception {
        Instant inWindow = Instant.parse("2024-01-01T10:00:00Z");
        Instant outOfWindow = Instant.parse("2024-01-02T10:00:00Z");
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 0L, inWindow, Instant.now(), "k0", b("v0"), "A");
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 1L, outOfWindow, Instant.now(), "k1", b("v1"), "A");

        CassetteSummary summary = service.getTopicSummary(TOPIC,
                Instant.parse("2024-01-01T00:00:00Z"), Instant.parse("2024-01-01T23:59:59Z"));

        assertThat(summary.totalRecords()).isEqualTo(1);
    }

    @Test
    void getTopicSummary_foldsBeyondTop20IntoOtherBucket() throws Exception {
        Instant ts = Instant.parse("2024-01-01T10:00:00Z");
        // 25 distinct message types, one record each — only 20 should appear by name,
        // the remaining 5 folded into a single "__other__" entry.
        for (int i = 0; i < 25; i++) {
            DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, i, ts.plusSeconds(i), Instant.now(),
                    "k" + i, b("v" + i), "type" + i);
        }

        CassetteSummary summary = service.getTopicSummary(TOPIC, null, null);

        List<ValueCount> messageTypes = summary.dimensions().get("messageType");
        assertThat(messageTypes).hasSize(21); // 20 named + 1 "other"
        assertThat(messageTypes).filteredOn(vc -> "__other__".equals(vc.value()))
                .singleElement()
                .satisfies(vc -> assertThat(vc.count()).isEqualTo(5));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd joxette-service && mvn -q -Dtest=TopicReplayServiceTest#getTopicSummary_breaksDownByPartitionAndMessageType,TopicReplayServiceTest#getTopicSummary_dedupesSameOffsetRecordedTwice,TopicReplayServiceTest#getTopicSummary_scopesToFromToWindow,TopicReplayServiceTest#getTopicSummary_foldsBeyondTop20IntoOtherBucket test`
Expected: compile error (`getTopicSummary` does not exist on `TopicReplayService`).

- [ ] **Step 3: Implement `getTopicSummary`**

Add to `TopicReplayService.java`, after the `tableFor`/`normalizeTopicName` helpers (after line ~444, before the "Record mapping" section comment):

```java
    // -------------------------------------------------------------------------
    // Cassette summary
    // -------------------------------------------------------------------------

    private static final int SUMMARY_TOP_N = 20;

    /**
     * Returns a dimension-cardinality breakdown (by partition and message type)
     * for this topic's general cassette, optionally scoped to a time window.
     * Deduplicates the same way {@link #query} does (QUALIFY {@code ROW_NUMBER()}
     * over {@code (kafka_partition, kafka_offset)}, keeping the most recently
     * recorded row) before counting, so at-least-once Kafka duplicates aren't
     * double-counted.
     */
    public CassetteSummary getTopicSummary(String topic, Instant from, Instant to) throws SQLException {
        Table<?> table = tableFor(topic);
        Condition cond = DSL.noCondition();
        if (from != null) cond = cond.and(F_TIMESTAMP.ge(from.atOffset(ZoneOffset.UTC)));
        if (to != null)   cond = cond.and(F_TIMESTAMP.le(to.atOffset(ZoneOffset.UTC)));
        final Condition finalCond = cond;

        return withObjectStoreRetry("summary:" + topic, () -> {
            synchronized (duckDB) {
                var deduped = dsl.select(F_PARTITION, F_MESSAGE_TYPE)
                        .from(table)
                        .where(finalCond)
                        .qualify(QUALIFY_DEDUP)
                        .asTable("deduped");
                Field<Integer> dPartition   = deduped.field(F_PARTITION);
                Field<String>  dMessageType = deduped.field(F_MESSAGE_TYPE);

                int totalRecords = dsl.fetchCount(deduped);

                var partitionRows = dsl.select(dPartition.cast(String.class), DSL.count())
                        .from(deduped)
                        .groupBy(dPartition)
                        .orderBy(DSL.count().desc())
                        .limit(SUMMARY_TOP_N + 1)
                        .fetch();

                var messageTypeRows = dsl.select(dMessageType, DSL.count())
                        .from(deduped)
                        .groupBy(dMessageType)
                        .orderBy(DSL.count().desc())
                        .limit(SUMMARY_TOP_N + 1)
                        .fetch();

                return new CassetteSummary(totalRecords, from, to, Map.of(
                        "partition", toValueCounts(partitionRows, totalRecords),
                        "messageType", toValueCounts(messageTypeRows, totalRecords)));
            }
        });
    }

    /**
     * Converts a {@code SELECT value, COUNT(*) ... LIMIT TOP_N+1} result into a
     * capped {@link ValueCount} list, folding anything beyond {@link #SUMMARY_TOP_N}
     * into a single {@code "__other__"} entry so a high-cardinality dimension
     * doesn't blow up the response.
     */
    static List<ValueCount> toValueCounts(Result<Record2<String, Integer>> rows, long total) {
        List<ValueCount> out = new ArrayList<>();
        long topSum = 0;
        int n = Math.min(rows.size(), SUMMARY_TOP_N);
        for (int i = 0; i < n; i++) {
            String value = rows.get(i).value1();
            int count = rows.get(i).value2();
            out.add(new ValueCount(value, count));
            topSum += count;
        }
        if (rows.size() > SUMMARY_TOP_N) {
            out.add(new ValueCount("__other__", total - topSum));
        }
        return out;
    }
```

Add these imports (alongside the existing `import org.jooq...` block near the top of the file):

```java
import org.jooq.Record2;
import org.jooq.Result;
```

and (alongside the existing `import java.util...` block):

```java
import java.util.Map;
```

(`toValueCounts` is package-visible `static`, not `private`, so Task 3 can reuse it from `EntityReplayService` — mirroring how `withObjectStoreRetry` is already shared the same way, per the file's own "Package-visible helpers (used by EntityReplayService)" comment.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd joxette-service && mvn -q -Dtest=TopicReplayServiceTest#getTopicSummary_breaksDownByPartitionAndMessageType,TopicReplayServiceTest#getTopicSummary_dedupesSameOffsetRecordedTwice,TopicReplayServiceTest#getTopicSummary_scopesToFromToWindow,TopicReplayServiceTest#getTopicSummary_foldsBeyondTop20IntoOtherBucket test`
Expected: `BUILD SUCCESS`, 4 tests passed.

- [ ] **Step 5: Run the full test class to confirm no regression**

Run: `cd joxette-service && mvn -q -Dtest=TopicReplayServiceTest test`
Expected: `BUILD SUCCESS`, all tests passed.

- [ ] **Step 6: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/replay/TopicReplayService.java joxette-service/src/test/java/com/joxette/replay/TopicReplayServiceTest.java
git commit -m "feat(replay): add TopicReplayService.getTopicSummary dimension breakdown"
```

---

## Task 3: `EntityReplayService.getEntitySummary()`

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/EntityReplayService.java`
- Test: `joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceTest.java`

**Interfaces:**
- Consumes: `ValueCount`, `CassetteSummary` (Task 1); `TopicReplayService.toValueCounts(Result<Record2<String,Integer>>, long)`, `TopicReplayService.withObjectStoreRetry(String, Supplier<T>)` (Task 2, package-visible); existing `F_TOPIC`, `F_MESSAGE_TYPE`, `F_TIMESTAMP`, `QUALIFY_DEDUP`, `entityTable(String)`, `validateEntityType(String)` (already in this file).
- Produces: `public CassetteSummary getEntitySummary(String entityType, Instant from, Instant to) throws SQLException` — consumed by Task 4.

- [ ] **Step 1: Write the failing tests**

Add to `EntityReplayServiceTest.java`, in a new section:

```java
    // -------------------------------------------------------------------------
    // getEntitySummary
    // -------------------------------------------------------------------------

    @Test
    void getEntitySummary_breaksDownBySourceTopicAndMessageType() throws Exception {
        Instant ts = Instant.parse("2024-01-01T10:00:00Z");
        DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE, "order-1", 0, "Created",
                "orders.events", 0, 0L, ts, Instant.now(), "k0", b("v0"));
        DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE, "order-1", 0, "Paid",
                "payments.events", 0, 0L, ts.plusSeconds(1), Instant.now(), "k1", b("v1"));
        DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE, "order-2", 0, "Created",
                "orders.events", 0, 1L, ts.plusSeconds(2), Instant.now(), "k2", b("v2"));

        CassetteSummary summary = service.getEntitySummary(ENTITY_TYPE, null, null);

        assertThat(summary.totalRecords()).isEqualTo(3);
        assertThat(summary.dimensions().get("sourceTopic"))
                .containsExactlyInAnyOrder(new ValueCount("orders.events", 2), new ValueCount("payments.events", 1));
        assertThat(summary.dimensions().get("messageType"))
                .containsExactlyInAnyOrder(new ValueCount("Created", 2), new ValueCount("Paid", 1));
    }

    @Test
    void getEntitySummary_dedupesSameSourceOffsetRecordedTwice() throws Exception {
        Instant ts = Instant.parse("2024-01-01T10:00:00Z");
        DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE, "order-1", 0, "Created",
                "orders.events", 0, 0L, ts, Instant.now().minusSeconds(10), "k0", b("v0"));
        DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE, "order-1", 0, "Created",
                "orders.events", 0, 0L, ts, Instant.now(), "k0", b("v0"));

        CassetteSummary summary = service.getEntitySummary(ENTITY_TYPE, null, null);

        assertThat(summary.totalRecords()).isEqualTo(1);
    }

    @Test
    void getEntitySummary_rejectsInvalidEntityType() {
        assertThatThrownBy(() -> service.getEntitySummary("Not Valid!", null, null))
                .isInstanceOf(com.joxette.api.error.ValidationException.class);
    }
```

Add `b(...)` helper (this test class doesn't have one yet — `TopicReplayServiceTest`'s `b()` is `private`, not reusable across classes):

```java
    private static byte[] b(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd joxette-service && mvn -q -Dtest=EntityReplayServiceTest#getEntitySummary_breaksDownBySourceTopicAndMessageType,EntityReplayServiceTest#getEntitySummary_dedupesSameSourceOffsetRecordedTwice,EntityReplayServiceTest#getEntitySummary_rejectsInvalidEntityType test`
Expected: compile error (`getEntitySummary` does not exist on `EntityReplayService`).

- [ ] **Step 3: Implement `getEntitySummary`**

Add to `EntityReplayService.java`, after the `getEntityStats` method (after its closing `}`, before the "Helpers" section):

```java
    // -------------------------------------------------------------------------
    // Cassette summary
    // -------------------------------------------------------------------------

    private static final int SUMMARY_TOP_N = 20;

    /**
     * Returns a dimension-cardinality breakdown (by source topic and message
     * type) for this entity type's cassette, optionally scoped to a time
     * window. Deduplicates the same way {@link #getEntityStats} does (QUALIFY
     * {@code ROW_NUMBER()} over {@code (topic, kafka_partition, kafka_offset)},
     * keeping the most recently recorded row) before counting.
     */
    public CassetteSummary getEntitySummary(String entityType, Instant from, Instant to) throws SQLException {
        validateEntityType(entityType);
        Table<?> table = entityTable(entityType);
        Condition cond = DSL.noCondition();
        if (from != null) cond = cond.and(F_TIMESTAMP.ge(from.atOffset(ZoneOffset.UTC)));
        if (to != null)   cond = cond.and(F_TIMESTAMP.le(to.atOffset(ZoneOffset.UTC)));
        final Condition finalCond = cond;

        return TopicReplayService.withObjectStoreRetry("entitySummary:" + entityType, () -> {
            synchronized (duckDB) {
                var deduped = dsl.select(F_TOPIC, F_MESSAGE_TYPE)
                        .from(table)
                        .where(finalCond)
                        .qualify(QUALIFY_DEDUP)
                        .asTable("deduped");
                Field<String> dTopic       = deduped.field(F_TOPIC);
                Field<String> dMessageType = deduped.field(F_MESSAGE_TYPE);

                int totalRecords = dsl.fetchCount(deduped);

                var topicRows = dsl.select(dTopic, DSL.count())
                        .from(deduped)
                        .groupBy(dTopic)
                        .orderBy(DSL.count().desc())
                        .limit(SUMMARY_TOP_N + 1)
                        .fetch();

                var messageTypeRows = dsl.select(dMessageType, DSL.count())
                        .from(deduped)
                        .groupBy(dMessageType)
                        .orderBy(DSL.count().desc())
                        .limit(SUMMARY_TOP_N + 1)
                        .fetch();

                return new CassetteSummary(totalRecords, from, to, Map.of(
                        "sourceTopic", TopicReplayService.toValueCounts(topicRows, totalRecords),
                        "messageType", TopicReplayService.toValueCounts(messageTypeRows, totalRecords)));
            }
        });
    }
```

Add this import (alongside the existing `import java.util...` block near the top of the file):

```java
import java.util.Map;
```

(`org.jooq.Record2`/`org.jooq.Result` may already be needed as imports here too — add them next to the file's existing `org.jooq.*` imports if not already present.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd joxette-service && mvn -q -Dtest=EntityReplayServiceTest#getEntitySummary_breaksDownBySourceTopicAndMessageType,EntityReplayServiceTest#getEntitySummary_dedupesSameSourceOffsetRecordedTwice,EntityReplayServiceTest#getEntitySummary_rejectsInvalidEntityType test`
Expected: `BUILD SUCCESS`, 3 tests passed.

- [ ] **Step 5: Run the full test class to confirm no regression**

Run: `cd joxette-service && mvn -q -Dtest=EntityReplayServiceTest test`
Expected: `BUILD SUCCESS`, all tests passed.

- [ ] **Step 6: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/replay/EntityReplayService.java joxette-service/src/test/java/com/joxette/replay/EntityReplayServiceTest.java
git commit -m "feat(replay): add EntityReplayService.getEntitySummary dimension breakdown"
```

---

## Task 4: `CassetteController` summary endpoints

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/CassetteController.java`

**Interfaces:**
- Consumes: `topicService.getTopicSummary(String, Instant, Instant)` (Task 2), `entityService.getEntitySummary(String, Instant, Instant)` (Task 3) — both already fields on this controller (`topicService`, `entityService`).
- Produces: `GET /cassettes/topics/{topic}/summary`, `GET /cassettes/entities/{entityType}/{entityId}/summary` — consumed by Task 5 (frontend `cassettesApi`).

No dedicated controller test: these are thin delegating handlers with no new branching logic (matching how `/stats` itself has no dedicated controller-level test — the generic `SQLException`/`Exception` → ProblemDetail mapping in `GlobalExceptionHandler` already covers this endpoint for free).

- [ ] **Step 1: Add the topic summary handler**

Insert into `CassetteController.java`, immediately after the `getTopicStats` handler (after its closing `}`, around line 1556):

```java
    @Operation(
        operationId = "getTopicSummary",
        summary = "Topic cassette dimension summary",
        description = "Returns a deduplicated dimension-cardinality breakdown (by partition and message type) "
                     + "for the topic's general cassette, optionally scoped to a time window. Each dimension is "
                     + "capped at the top 20 values by count, with the remainder folded into an \"__other__\" entry."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Dataset summary",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = CassetteSummary.class),
                examples = @ExampleObject(name = "summary", value = """
                    {
                      "totalRecords": 128456,
                      "from": null,
                      "to": null,
                      "dimensions": {
                        "partition": [{"value": "3", "count": 40213}, {"value": "1", "count": 38010}],
                        "messageType": [{"value": "OrderCreated", "count": 88012}, {"value": null, "count": 1200}]
                      }
                    }"""))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/topics/{topic}/summary", produces = MediaType.APPLICATION_JSON_VALUE)
    public CassetteSummary getTopicSummary(
            @Parameter(description = "Kafka topic name", required = true, example = "orders")
            @PathVariable String topic,
            @Parameter(description = "Include only records with timestamp >= this value (ISO-8601 instant)")
            @RequestParam(required = false) Instant from,
            @Parameter(description = "Include only records with timestamp <= this value (ISO-8601 instant)")
            @RequestParam(required = false) Instant to
    ) throws SQLException {
        return topicService.getTopicSummary(topic, from, to);
    }
```

- [ ] **Step 2: Add the entity summary handler**

Insert into `CassetteController.java`, immediately after the `getEntityStats` handler (after its closing `}`, around line 1174):

```java
    @Operation(
        operationId = "getEntitySummary",
        summary = "Entity cassette dimension summary",
        description = "Returns a deduplicated dimension-cardinality breakdown (by source topic and message type) "
                     + "for the entity type's cassette, optionally scoped to a time window. Each dimension is "
                     + "capped at the top 20 values by count, with the remainder folded into an \"__other__\" entry."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Dataset summary",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(implementation = CassetteSummary.class),
                examples = @ExampleObject(name = "summary", value = """
                    {
                      "totalRecords": 17,
                      "from": null,
                      "to": null,
                      "dimensions": {
                        "sourceTopic": [{"value": "customer-events", "count": 12}, {"value": "customer-orders", "count": 5}],
                        "messageType": [{"value": "Created", "count": 10}, {"value": "Updated", "count": 7}]
                      }
                    }"""))),
        @ApiResponse(responseCode = "400", description = "Invalid entity type name",
            content = @Content(schema = @Schema(type = "string"))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/entities/{entityType}/{entityId}/summary",
                produces = MediaType.APPLICATION_JSON_VALUE)
    public CassetteSummary getEntitySummary(
            @Parameter(description = "Entity type name (must match `[a-z][a-z0-9_]*`)", required = true, example = "customer")
            @PathVariable String entityType,
            @Parameter(description = "Entity identifier (unused — summary is per-type, not per-instance; present for URL symmetry with sibling endpoints)", required = true, example = "cust-042")
            @PathVariable String entityId,
            @Parameter(description = "Include only records with timestamp >= this value (ISO-8601 instant)")
            @RequestParam(required = false) Instant from,
            @Parameter(description = "Include only records with timestamp <= this value (ISO-8601 instant)")
            @RequestParam(required = false) Instant to
    ) throws SQLException {
        return entityService.getEntitySummary(entityType, from, to);
    }
```

**Note on the entity path shape:** this mirrors `/entities/{entityType}/{entityId}/stats`'s URL pattern for consistency with sibling endpoints, but the summary is actually per-*entity-type* (the whole cassette table), not per-instance — `entityId` is accepted and ignored. This matches the design spec's endpoint shape (`GET /cassettes/entities/{entityType}/{entityId}/summary`) and the frontend's `cassettesApi.getEntitySummary(entityType, entityId, ...)` call, both written assuming this URL. If this asymmetry (accepting an unused path segment) turns out to bother a future reader, moving the route to `/entities/{entityType}/summary` is a one-line change on both sides — not done here to keep this task's diff minimal and match the already-approved spec exactly.

- [ ] **Step 3: Compile check**

Run: `cd joxette-service && mvn -q compile`
Expected: `BUILD SUCCESS`.

- [ ] **Step 4: Manual smoke test**

Run the service locally (`mvn spring-boot:run` from `joxette-service/`, against a local DuckLake catalog with at least one recorded topic), then:

```bash
curl -s "http://localhost:8080/cassettes/topics/<a-real-topic>/summary" | jq .
```

Expected: a `200` JSON body matching the `CassetteSummary` shape, with real `dimensions.partition`/`dimensions.messageType` counts.

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/replay/CassetteController.java
git commit -m "feat(replay): expose GET .../summary endpoints for topic and entity cassettes"
```

---

## Task 5: Frontend API client — types, `cassettesApi` methods, `EntityRecordsParams.messageTypes`

**Files:**
- Modify: `ui/src/api/client.ts`

**Interfaces:**
- Produces: `ValueCount`, `CassetteSummary`, `CassetteSummaryParams` types; `cassettesApi.getTopicSummary(topic, params?)`, `cassettesApi.getEntitySummary(entityType, entityId, params?)` — consumed by Task 7 (`DatasetSummaryPanel`). `EntityRecordsParams.messageTypes?: string[]` — consumed by Task 10.

- [ ] **Step 1: Add the summary types**

Insert into `ui/src/api/client.ts`, immediately after the `EntityRecordsParams` type (after line 497):

```ts
export interface ValueCount {
  value: string | null
  count: number
}

export interface CassetteSummary {
  totalRecords: number
  from: string | null
  to: string | null
  dimensions: Record<string, ValueCount[]>
}

export type CassetteSummaryParams = QueryParams & {
  from?: string
  to?: string
}
```

- [ ] **Step 2: Add `messageTypes` to `EntityRecordsParams`**

Modify the existing type (line 491-497):

```ts
export type EntityRecordsParams = QueryParams & {
  from?: string
  to?: string
  limit?: number
  cursor?: string
  order?: Order
  messageTypes?: string[]
}
```

- [ ] **Step 3: Wire `messageTypes` through `getEntityRecords`**

Modify the existing method (line 514-515) from:

```ts
  getEntityRecords: (entityType: string, entityId: string, params?: EntityRecordsParams) =>
    request<PagedResponse<EntityRecord>>(`/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}${buildQuery(params ?? {})}`),
```

to:

```ts
  getEntityRecords: (entityType: string, entityId: string, params?: EntityRecordsParams) => {
    const { messageTypes, ...rest } = params ?? {}
    const messageTypesParam = messageTypes && messageTypes.length > 0 ? messageTypes.join(',') : undefined
    return request<PagedResponse<EntityRecord>>(
      `/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}`
      + buildQuery({ ...rest, message_types: messageTypesParam }),
    )
  },
```

(The backend binds `message_types` as a Spring `List<String>` `@RequestParam`, which accepts a single comma-separated value the same way repeated params would — confirmed via `CassetteController.java:790`.)

- [ ] **Step 4: Add the summary methods to `cassettesApi`**

Insert immediately after `getTopicStats` (after line 503):

```ts
  getTopicSummary: (topic: string, params?: CassetteSummaryParams) =>
    request<CassetteSummary>(`/cassettes/topics/${encodeURIComponent(topic)}/summary${buildQuery(params ?? {})}`),
```

Insert immediately after `getEntityStats` (after line 517):

```ts
  getEntitySummary: (entityType: string, entityId: string, params?: CassetteSummaryParams) =>
    request<CassetteSummary>(`/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}/summary${buildQuery(params ?? {})}`),
```

- [ ] **Step 5: Type-check**

Run: `cd ui && pnpm exec tsc --noEmit`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add ui/src/api/client.ts
git commit -m "feat(ui): add cassette summary API client methods and entity message-type filter"
```

---

## Task 6: `CassetteTimeline` — `highlightPredicate` prop

**Files:**
- Modify: `ui/src/components/CassetteTimeline.tsx`

**Interfaces:**
- Produces: `CassetteTimelineProps.highlightPredicate?: (record: TimelineRecord) => boolean` — consumed by Tasks 9 and 11 (the two timeline routes).

Additive-only change: when the prop is omitted, rendering is byte-for-byte identical to today (verified by the "no dimming" branch below always matching the current unconditional `color + '99'` expression when `highlightPredicate` is `undefined`). No new automated test — canvas pixel output isn't meaningfully unit-testable in this codebase (no existing precedent for it), and manual verification is covered in Tasks 9/11.

- [ ] **Step 1: Add the prop to the exported `CassetteTimelineProps` interface**

Modify `ui/src/components/CassetteTimeline.tsx`, adding to the interface (after `onGroupByModeChange`, around line 66):

```ts
  onGroupByModeChange?: (mode: GroupByMode) => void
  /** When set, markers for records where this returns false render dimmed. Purely visual — does not filter what's loaded. */
  highlightPredicate?: (record: TimelineRecord) => boolean
```

- [ ] **Step 2: Thread it through the `CassetteTimeline` component's destructured props**

Modify the function signature (line 737-747):

```ts
export function CassetteTimeline({
  records,
  onLoadBefore,
  onLoadAfter,
  hasMore,
  loading,
  title,
  extraControls,
  supportsMessageType,
  onGroupByModeChange,
  highlightPredicate,
}: CassetteTimelineProps) {
```

- [ ] **Step 3: Pass it to `TimelineCanvas`**

Modify the `<TimelineCanvas>` call (line 911-917):

```tsx
        <TimelineCanvas
          records={effectiveRecords}
          selectedIdx={selectedIdx}
          colorKeys={colorKeys}
          onSelect={handleSelect}
          fitKey={fitKey}
          highlightPredicate={highlightPredicate}
        />
```

- [ ] **Step 4: Add the prop to `TimelineCanvasProps` and destructure it**

Modify (line 366-375):

```ts
interface TimelineCanvasProps {
  records: TimelineRecord[]
  selectedIdx: number
  colorKeys: string[]
  onSelect: (idx: number) => void
  onViewChange?: (vs: ViewState) => void
  fitKey?: number  // increment to trigger fit-to-window
  highlightPredicate?: (record: TimelineRecord) => boolean
}

function TimelineCanvas({ records, selectedIdx, colorKeys, onSelect, fitKey, highlightPredicate }: TimelineCanvasProps) {
```

- [ ] **Step 5: Dim non-matching markers in the draw loop**

Modify the marker loop (line 472-513) — change the `fillStyle` line and the `useCallback` dependency array:

```ts
    // Markers
    records.forEach((r, i) => {
      const ms = timestamps[i]
      if (ms == null) return
      const x = vs.originPx + ms / vs.msPerPx
      if (x < -20 || x > w + 20) return

      const color = colorForKey(r.colorKey, colorKeys, theme.chartCat)
      const isSelected = i === selectedIdx
      const isDimmed = !isSelected && highlightPredicate != null && !highlightPredicate(r)
      const radius = isSelected ? SELECTED_RADIUS : MARKER_RADIUS

      // Shadow for selected
      if (isSelected) {
        ctx.shadowColor = color
        ctx.shadowBlur = 10
      }

      ctx.beginPath()
      ctx.arc(x, MARKER_Y, radius, 0, Math.PI * 2)
      ctx.fillStyle = isSelected ? color : (isDimmed ? color + '33' : color + '99')
      ctx.fill()
      // Unselected markers get a subtle ring in the theme's rule color
      // (rather than a hardcoded white ring, which read wrong against the
      // dark --surface-sunken canvas background).
      ctx.strokeStyle = isSelected ? color : theme.ruleStrong
      ctx.lineWidth = isSelected ? 2.5 : 1.5
      ctx.stroke()

      ctx.shadowBlur = 0

      // Label on selected
      if (isSelected) {
        ctx.fillStyle = theme.inkPrimary
        ctx.font = 'bold 11px system-ui'
        ctx.textAlign = 'center'
        const label = r.colorKey
        ctx.fillText(label, x, MARKER_Y - SELECTED_RADIUS - 6)
      }
    })

    ctx.restore()
  }, [records, timestamps, selectedIdx, colorKeys, minMs, maxMs, highlightPredicate])
```

(Only the `fillStyle` line and the dependency array change; everything else in this block is unchanged and shown here only for exact placement.)

- [ ] **Step 6: Type-check**

Run: `cd ui && pnpm exec tsc --noEmit`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add ui/src/components/CassetteTimeline.tsx
git commit -m "feat(ui): add optional highlightPredicate dimming to CassetteTimeline markers"
```

---

## Task 7: `DatasetSummaryPanel` component

**Files:**
- Create: `ui/src/components/DatasetSummaryPanel.tsx`
- Test: `ui/src/components/DatasetSummaryPanel.test.tsx`

**Interfaces:**
- Consumes: `cassettesApi.getTopicSummary`/`getEntitySummary`, `CassetteSummary`, `ValueCount` (Task 5); `Tabular` (`ui/src/design/primitives/Tabular.tsx`, existing); `ErrorMessage` (`ui/src/components/ErrorMessage.tsx`, existing).
- Produces: `export interface DatasetSummarySelection { dimension: string; value: string | null }`; `export function DatasetSummaryPanel(props: DatasetSummaryPanelProps)` where

```ts
interface DatasetSummaryPanelProps {
  kind: 'topic' | 'entity'
  topic?: string
  entityType?: string
  entityId?: string
  from?: string
  to?: string
  selected: DatasetSummarySelection | null
  onSelect: (dimension: string, value: string | null) => void
}
```

— consumed by Tasks 8, 9, 10, 11.

- [ ] **Step 1: Write the failing test**

Create `ui/src/components/DatasetSummaryPanel.test.tsx`:

```tsx
// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, cleanup, fireEvent } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

vi.mock('../api/client', () => ({
  cassettesApi: {
    getTopicSummary: vi.fn(),
    getEntitySummary: vi.fn(),
  },
}))

import { cassettesApi } from '../api/client'
import { DatasetSummaryPanel } from './DatasetSummaryPanel'

afterEach(() => cleanup())

function renderPanel(overrides: Partial<React.ComponentProps<typeof DatasetSummaryPanel>> = {}) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const onSelect = overrides.onSelect ?? vi.fn()
  return {
    onSelect,
    ...render(
      <QueryClientProvider client={qc}>
        <DatasetSummaryPanel
          kind="topic"
          topic="orders.events"
          selected={null}
          onSelect={onSelect}
          {...overrides}
        />
      </QueryClientProvider>,
    ),
  }
}

describe('DatasetSummaryPanel', () => {
  it('renders dimension leaderboards from the summary response', async () => {
    vi.mocked(cassettesApi.getTopicSummary).mockResolvedValue({
      totalRecords: 100,
      from: null,
      to: null,
      dimensions: {
        partition: [{ value: '0', count: 60 }, { value: '1', count: 40 }],
        messageType: [{ value: 'OrderCreated', count: 100 }],
      },
    })

    renderPanel()

    expect(await screen.findByText('0')).toBeTruthy()
    expect(screen.getByText('60')).toBeTruthy()
    expect(screen.getByText('OrderCreated')).toBeTruthy()
  })

  it('calls onSelect with the clicked dimension and value', async () => {
    vi.mocked(cassettesApi.getTopicSummary).mockResolvedValue({
      totalRecords: 60,
      from: null,
      to: null,
      dimensions: {
        partition: [{ value: '0', count: 60 }],
        messageType: [],
      },
    })
    const { onSelect } = renderPanel()

    fireEvent.click(await screen.findByText('0'))

    expect(onSelect).toHaveBeenCalledWith('partition', '0')
  })

  it('shows an error message when the summary request fails', async () => {
    vi.mocked(cassettesApi.getTopicSummary).mockRejectedValue(new Error('boom'))

    renderPanel()

    expect(await screen.findByRole('alert')).toBeTruthy()
  })
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd ui && pnpm test -- DatasetSummaryPanel`
Expected: FAIL — `Failed to resolve import "./DatasetSummaryPanel"`.

- [ ] **Step 3: Implement `DatasetSummaryPanel`**

Create `ui/src/components/DatasetSummaryPanel.tsx`:

```tsx
import { useQuery } from '@tanstack/react-query'
import { cassettesApi, type CassetteSummary } from '../api/client'
import { Tabular } from '../design/primitives/Tabular'
import { ErrorMessage } from './ErrorMessage'

export interface DatasetSummarySelection {
  dimension: string
  value: string | null
}

interface DatasetSummaryPanelProps {
  kind: 'topic' | 'entity'
  topic?: string
  entityType?: string
  entityId?: string
  from?: string
  to?: string
  selected: DatasetSummarySelection | null
  onSelect: (dimension: string, value: string | null) => void
}

const DIMENSION_LABELS: Record<string, string> = {
  partition: 'By partition',
  sourceTopic: 'By source topic',
  messageType: 'By message type',
}

export function DatasetSummaryPanel({ kind, topic, entityType, entityId, from, to, selected, onSelect }: DatasetSummaryPanelProps) {
  const id = kind === 'topic' ? topic : `${entityType}/${entityId}`

  const query = useQuery({
    queryKey: ['cassettes', kind, id, 'summary', { from, to }],
    queryFn: () =>
      kind === 'topic'
        ? cassettesApi.getTopicSummary(topic!, { from, to })
        : cassettesApi.getEntitySummary(entityType!, entityId!, { from, to }),
    staleTime: 30_000,
  })

  if (query.isLoading) {
    return (
      <aside style={{ width: 240, flexShrink: 0 }} aria-busy="true" aria-label="Dataset summary loading">
        {[0, 1].map(i => (
          <div key={i} style={{ height: 120, borderRadius: 6, background: 'var(--surface-raised)', marginBottom: 16 }} />
        ))}
      </aside>
    )
  }

  if (query.error || !query.data) {
    return (
      <aside style={{ width: 240, flexShrink: 0 }}>
        <ErrorMessage message={(query.error as Error)?.message ?? 'Failed to load summary'} />
      </aside>
    )
  }

  const summary: CassetteSummary = query.data
  const dimensionKeys = Object.keys(summary.dimensions)

  return (
    <aside style={{ width: 240, flexShrink: 0, display: 'flex', flexDirection: 'column', gap: 20 }} aria-label="Dataset summary">
      {dimensionKeys.map(dimKey => {
        const rows = summary.dimensions[dimKey] ?? []
        return (
          <div key={dimKey}>
            <div
              style={{
                fontSize: 11,
                fontWeight: 600,
                textTransform: 'uppercase',
                letterSpacing: '0.06em',
                color: 'var(--ink-secondary)',
                marginBottom: 8,
              }}
            >
              {DIMENSION_LABELS[dimKey] ?? dimKey}
            </div>
            {rows.length === 0 ? (
              <div style={{ fontSize: 12, color: 'var(--ink-tertiary)' }}>No data</div>
            ) : (
              <ul style={{ listStyle: 'none', margin: 0, padding: 0, display: 'flex', flexDirection: 'column', gap: 2 }}>
                {rows.map(row => {
                  const isSelected = selected?.dimension === dimKey && selected?.value === row.value
                  return (
                    <li key={row.value ?? '__null__'}>
                      <button
                        type="button"
                        onClick={() => onSelect(dimKey, row.value)}
                        style={{
                          display: 'flex',
                          justifyContent: 'space-between',
                          alignItems: 'center',
                          width: '100%',
                          padding: '4px 6px',
                          background: isSelected ? 'var(--accent)' : 'transparent',
                          color: isSelected ? 'var(--accent-ink)' : 'var(--ink-primary)',
                          border: 'none',
                          borderRadius: 4,
                          cursor: 'pointer',
                          fontSize: 13,
                          textAlign: 'left',
                        }}
                      >
                        <span>{row.value ?? '(none)'}</span>
                        <Tabular size="xs" style={{ color: 'inherit' }}>{row.count.toLocaleString()}</Tabular>
                      </button>
                    </li>
                  )
                })}
              </ul>
            )}
          </div>
        )
      })}
    </aside>
  )
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd ui && pnpm test -- DatasetSummaryPanel`
Expected: PASS, 3 tests.

- [ ] **Step 5: Type-check**

Run: `cd ui && pnpm exec tsc --noEmit`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add ui/src/components/DatasetSummaryPanel.tsx ui/src/components/DatasetSummaryPanel.test.tsx
git commit -m "feat(ui): add DatasetSummaryPanel component"
```

---

## Task 8: Wire `DatasetSummaryPanel` into `topics/$topic.tsx`

**Files:**
- Modify: `ui/src/routes/topics/$topic.tsx`

**Interfaces:**
- Consumes: `DatasetSummaryPanel`, `DatasetSummarySelection` (Task 7).

Partition clicks set the existing `partitionRaw` filter (real server refetch); message-type clicks are client-side highlight only, applied to the barcode/records view already rendered in this route (there is no `CassetteTimeline` mounted on this page — see Task 9 for the dedicated timeline route). No new automated test — this is JSX/state wiring with no new logic beyond what Task 7's component test and the existing route already cover; verify manually per Step 4.

- [ ] **Step 1: Add selection state and a toggle handler**

Modify `ui/src/routes/topics/$topic.tsx`, adding after the existing filter-state declarations (after line 169, `_replayPipelineFragments`):

```ts
  const [summarySelection, setSummarySelection] = useState<DatasetSummarySelection | null>(null)

  const handleSummarySelect = useCallback((dimension: string, value: string | null) => {
    setSummarySelection(prev => (prev?.dimension === dimension && prev?.value === value ? null : { dimension, value }))
    if (dimension === 'partition') {
      setPartitionRaw(prev => {
        const isToggleOff = summarySelection?.dimension === 'partition' && summarySelection?.value === value
        return isToggleOff ? '' : (value ?? '')
      })
    }
  }, [summarySelection])
```

Add the import (alongside the other component imports near the top of the file):

```ts
import { DatasetSummaryPanel, type DatasetSummarySelection } from '../../components/DatasetSummaryPanel'
```

(`useCallback` should already be imported from `react` in this file — if not already present in the existing `import { ... } from 'react'` line, add it there.)

- [ ] **Step 2: Wrap the view-mode-bar-and-tabs block in a flex row with the panel**

Modify `ui/src/routes/topics/$topic.tsx` around the View mode bar section (lines 684-697) — insert the flex-row opening right after the existing `</div>` that closes the ViewModeBar wrapper (end of line 697):

```tsx
          {/* ── View mode bar ────────────────────────────────────────── */}
          <div style={{ marginBottom: 16 }}>
            <ViewModeBar
              modes={[
                { id: 'records',  label: 'Records',  icon: '☰' },
                { id: 'sol',      label: 'SOL',       icon: '⌥' },
                { id: 'timeline', label: 'Timeline',  icon: '⏱' },
                { id: 'barcode',  label: 'Barcode',   icon: '▦' },
                { id: 'sequence', label: 'Sequence',  icon: '⛓' },
              ]}
              active={activeTab}
              onChange={setActiveTab}
            />
          </div>

          <div style={{ display: 'flex', gap: 'var(--space-6)', alignItems: 'flex-start' }}>
          <div style={{ flex: '1 1 0', minWidth: 0 }}>

          {activeTab === 'sol' && (
```

Then, right before line 903's closing `</div>` (the one that closes the root `maxWidth: 1180` div), close the new wrapper and render the panel:

```tsx
          )}

          </div>
          <DatasetSummaryPanel
            kind="topic"
            topic={topic}
            from={from || undefined}
            to={to || undefined}
            selected={summarySelection}
            onSelect={handleSummarySelect}
          />
          </div>
        </div>
      )}
```

(The last four lines above — `</div>`, `</div>`, `)}`, `</Layout>`'s sibling structure — must line up with what's already there; only the two new `<div>...</div>` wrapper lines and the `<DatasetSummaryPanel>` block are additions. The existing `{activeTab === 'records' && (...)}` block's own closing `)}` — currently the line right before the root `</div>` — stays exactly where it is; the new wrapper close and panel go between that `)}` and the root `</div>`.)

- [ ] **Step 3: Type-check**

Run: `cd ui && pnpm exec tsc --noEmit`
Expected: no errors.

- [ ] **Step 4: Manual verification**

Run: `cd ui && pnpm dev`, open a topic detail page in the browser.

Expected:
- The summary panel renders to the right of the Records/SOL/Barcode/Sequence tab content, showing partition and message-type breakdowns.
- Clicking a partition row highlights it and updates the "Partition" filter input's value; the records table refetches to match.
- Clicking a message-type row highlights it in the panel (client-side only — the records table/barcode view do not need to visibly change for this task, since Task 8 doesn't wire message-type into barcode dimming; that's out of scope per the design spec's Interaction section, which only requires `CassetteTimeline`, not the barcode view, to dim).
- Clicking a selected row again clears the selection and (for partition) clears the filter.

- [ ] **Step 5: Commit**

```bash
git add ui/src/routes/topics/\$topic.tsx
git commit -m "feat(ui): wire DatasetSummaryPanel into the topic detail page"
```

---

## Task 9: Wire `DatasetSummaryPanel` into `topics/$topic_.timeline.tsx`

**Files:**
- Modify: `ui/src/routes/topics/$topic_.timeline.tsx`

**Interfaces:**
- Consumes: `DatasetSummaryPanel`, `DatasetSummarySelection` (Task 7); `CassetteTimeline`'s `highlightPredicate` prop (Task 6).

This route has no `from`/`to` or partition filter state at all today (it loads the whole cassette by scrolling). So unlike Task 8, **every** dimension selection here is client-side highlight only via `highlightPredicate` — there is no server filter to wire into on this page.

- [ ] **Step 1: Add selection state and a `highlightPredicate`**

Modify `ui/src/routes/topics/$topic_.timeline.tsx`, inside `TopicTimelinePage`, after the existing state declarations (after line 134, `initialLoaded`):

```ts
  const [summarySelection, setSummarySelection] = useState<DatasetSummarySelection | null>(null)

  const handleSummarySelect = useCallback((dimension: string, value: string | null) => {
    setSummarySelection(prev => (prev?.dimension === dimension && prev?.value === value ? null : { dimension, value }))
  }, [])

  const highlightPredicate = useMemo(() => {
    if (!summarySelection) return undefined
    const { dimension, value } = summarySelection
    return (r: TimelineRecord) => {
      if (dimension === 'partition') return r.meta.partition === value
      if (dimension === 'messageType') return r.meta.type === value || (value === null && r.meta.type === undefined)
      return true
    }
  }, [summarySelection])
```

Add the import:

```ts
import { DatasetSummaryPanel, type DatasetSummarySelection } from '../../components/DatasetSummaryPanel'
```

(`toTimelineRecord` in this file already puts partition into `meta.partition` — see line 116 — but does **not** currently put `message_type` into `meta`; add it there too, since the highlight predicate above depends on it. Modify `toTimelineRecord` (lines 110-124):)

```ts
function toTimelineRecord(r: CassetteRecord): TimelineRecord {
  return {
    timestamp: r.timestamp,
    colorKey: `partition ${r.partition}`,
    value: r.value,
    meta: {
      partition: String(r.partition),
      offset: String(r.offset),
      ...(r.messageType ? { type: r.messageType } : {}),
      ...(r.key ? { key: r.key } : {}),
      recorded: r.recordedAt.slice(0, 19).replace('T', ' '),
    },
    headers: r.headers,
    sourceTopic: r.topic,
  }
}
```

(Only the new `...(r.messageType ? { type: r.messageType } : {}),` line is added; everything else in this function is unchanged.)

- [ ] **Step 2: Render the panel alongside the timeline**

Modify the JSX (lines 208-221) from:

```tsx
            <div style={{ flex: '1 1 0', minHeight: 0, border: '1px solid #e2e8f0', borderRadius: 8, overflow: 'hidden' }}>
              <CassetteTimeline
                records={timelineRecords}
                onLoadAfter={handleLoadAfter}
                hasMore={hasMoreAfter}
                loading={loading}
                title={topic}
                extraControls={
                  hasMoreAfter && !loading
                    ? <button style={secondaryBtn} onClick={handleLoadAfter}>Load next page</button>
                    : undefined
                }
              />
            </div>
```

to:

```tsx
            <div style={{ flex: '1 1 0', minHeight: 0, display: 'flex', gap: 12 }}>
              <div style={{ flex: '1 1 0', minWidth: 0, border: '1px solid #e2e8f0', borderRadius: 8, overflow: 'hidden' }}>
                <CassetteTimeline
                  records={timelineRecords}
                  onLoadAfter={handleLoadAfter}
                  hasMore={hasMoreAfter}
                  loading={loading}
                  title={topic}
                  highlightPredicate={highlightPredicate}
                  extraControls={
                    hasMoreAfter && !loading
                      ? <button style={secondaryBtn} onClick={handleLoadAfter}>Load next page</button>
                      : undefined
                  }
                />
              </div>
              <DatasetSummaryPanel
                kind="topic"
                topic={topic}
                selected={summarySelection}
                onSelect={handleSummarySelect}
              />
            </div>
```

(`useMemo` should already be imported from `react` in this file per line 2 — confirm it's there; it is.)

- [ ] **Step 3: Type-check**

Run: `cd ui && pnpm exec tsc --noEmit`
Expected: no errors.

- [ ] **Step 4: Manual verification**

Run: `cd ui && pnpm dev`, open a topic's Timeline page, click "Load Messages".

Expected: the summary panel renders beside the timeline canvas; clicking a partition or message-type row dims non-matching markers on the canvas (lower opacity), clicking the same row again undims everything.

- [ ] **Step 5: Commit**

```bash
git add ui/src/routes/topics/\$topic_.timeline.tsx
git commit -m "feat(ui): wire DatasetSummaryPanel into the topic timeline page"
```

---

## Task 10: Wire `DatasetSummaryPanel` into `entities/$entityType/$entityId.tsx`

**Files:**
- Modify: `ui/src/routes/entities/$entityType/$entityId.tsx`

**Interfaces:**
- Consumes: `DatasetSummaryPanel`, `DatasetSummarySelection` (Task 7); `EntityRecordsParams.messageTypes` (Task 5).

Message-type clicks set a new `messageTypesRaw` filter that feeds `EntityRecordsParams.messageTypes` (real server refetch, mirroring `partitionRaw` in the topic route); source-topic clicks are client-side highlight only (no server filter exists for it).

- [ ] **Step 1: Add selection state, a message-type filter, and a toggle handler**

Modify `ui/src/routes/entities/$entityType/$entityId.tsx`, after the existing `from`/`to` state declarations (after line 122, right after `const to = useDebounce(toRaw, 300)`):

```ts
  const [messageTypesRaw, setMessageTypesRaw] = useState<string[]>([])
  const [summarySelection, setSummarySelection] = useState<DatasetSummarySelection | null>(null)

  const handleSummarySelect = useCallback((dimension: string, value: string | null) => {
    setSummarySelection(prev => (prev?.dimension === dimension && prev?.value === value ? null : { dimension, value }))
    if (dimension === 'messageType') {
      setMessageTypesRaw(prev => {
        const isToggleOff = summarySelection?.dimension === 'messageType' && summarySelection?.value === value
        return isToggleOff || value === null ? [] : [value]
      })
    }
  }, [summarySelection])
```

Add the import:

```ts
import { DatasetSummaryPanel, type DatasetSummarySelection } from '../../../components/DatasetSummaryPanel'
```

- [ ] **Step 2: Thread `messageTypesRaw` into `recordsQuery`**

Modify the existing query (lines 157-166):

```ts
  const recordsQuery = useQuery({
    queryKey: ['cassettes', 'entities', entityType, entityId, 'records', { from, to, cursor, order, messageTypesRaw }],
    queryFn: () => cassettesApi.getEntityRecords(entityType, entityId, {
      from: from || undefined,
      to: to || undefined,
      cursor,
      limit: 50,
      order,
      messageTypes: messageTypesRaw.length > 0 ? messageTypesRaw : undefined,
    }),
  })
```

- [ ] **Step 3: Render the panel alongside the tab content**

Modify the JSX around the view mode switcher (lines 449-469) — insert the flex-row opening right after the `</div>` that closes it (end of line 469):

```tsx
      {/* View mode switcher */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
        <ViewModeBar
          aria-label="Entity view"
          modes={[
            { id: 'records',  label: 'Records',  icon: '☰' },
            { id: 'diff',     label: 'Diff',     icon: '±' },
            { id: 'state',    label: 'State',    icon: '◉' },
            { id: 'portrait', label: 'Portrait', icon: '👤' },
            { id: 'sol',      label: 'SOL',       icon: '⌥' },
            { id: 'timeline', label: 'Timeline',  icon: '⏱' },
            { id: 'barcode',  label: 'Barcode',   icon: '▦' },
            { id: 'sequence', label: 'Sequence',  icon: '⛓' },
          ]}
          active={activeTab}
          onChange={setActiveTab}
        />
        {activeTab === 'barcode' && (
          <BarcodeXModeToggle value={barcodeXMode} onChange={setBarcodeXMode} />
        )}
      </div>

      <div style={{ display: 'flex', gap: '1.5rem', alignItems: 'flex-start' }}>
      <div style={{ flex: '1 1 0', minWidth: 0 }}>

      {/* SOL tab */}
      {activeTab === 'sol' && (
```

Then, right after line 780's `)}` (which closes `{activeTab === 'records' && (...)}` — the last tab block, immediately before the `{deleteStep === 1 && ...}` dialog block), close the wrapper and render the panel:

```tsx
      )}

      </div>
      <DatasetSummaryPanel
        kind="entity"
        entityType={entityType}
        entityId={entityId}
        from={from || undefined}
        to={to || undefined}
        selected={summarySelection}
        onSelect={handleSummarySelect}
      />
      </div>

      {deleteStep === 1 && (
```

- [ ] **Step 4: Type-check**

Run: `cd ui && pnpm exec tsc --noEmit`
Expected: no errors.

- [ ] **Step 5: Manual verification**

Run: `cd ui && pnpm dev`, open an entity detail page.

Expected: the summary panel renders beside the tab content, showing source-topic and message-type breakdowns; clicking a message-type row filters the records table (real refetch); clicking a source-topic row only highlights it in the panel (no visible change elsewhere on this route, since the barcode/records views aren't wired to source-topic dimming — out of scope, same reasoning as Task 8's message-type case).

- [ ] **Step 6: Commit**

```bash
git add ui/src/routes/entities/\$entityType/\$entityId.tsx
git commit -m "feat(ui): wire DatasetSummaryPanel into the entity detail page"
```

---

## Task 11: Wire `DatasetSummaryPanel` into `entities/$entityType/$entityId_.timeline.tsx`

**Files:**
- Modify: `ui/src/routes/entities/$entityType/$entityId_.timeline.tsx`

**Interfaces:**
- Consumes: `DatasetSummaryPanel`, `DatasetSummarySelection` (Task 7); `CassetteTimeline`'s `highlightPredicate` prop (Task 6).

Like Task 9, this route has no filter state at all — every dimension selection here is client-side highlight only.

- [ ] **Step 1: Add selection state and a `highlightPredicate`**

Modify `ui/src/routes/entities/$entityType/$entityId_.timeline.tsx`, inside `EntityTimelinePage`, after the existing state declarations (after line 110, `groupByKind`):

```ts
  const [summarySelection, setSummarySelection] = useState<DatasetSummarySelection | null>(null)

  const handleSummarySelect = useCallback((dimension: string, value: string | null) => {
    setSummarySelection(prev => (prev?.dimension === dimension && prev?.value === value ? null : { dimension, value }))
  }, [])

  const highlightPredicate = useMemo(() => {
    if (!summarySelection) return undefined
    const { dimension, value } = summarySelection
    return (r: TimelineRecord) => {
      if (dimension === 'sourceTopic') return r.sourceTopic === value
      if (dimension === 'messageType') return r.meta.type === value || (value === null && r.meta.type === undefined)
      return true
    }
  }, [summarySelection])
```

Add the import:

```ts
import { DatasetSummaryPanel, type DatasetSummarySelection } from '../../../components/DatasetSummaryPanel'
```

(`toTimelineRecord` in this file already puts `message_type` into `meta.type` — see line 92 (`...(r.messageType ? { type: r.messageType } : {}),`) — and `r.topic` into `sourceTopic` — see line 97. No change needed there, unlike Task 9.)

- [ ] **Step 2: Render the panel alongside the timeline**

Modify the JSX (lines 192-209) from:

```tsx
            <div style={{ flex: '1 1 0', minHeight: 0, border: '1px solid #e2e8f0', borderRadius: 8, overflow: 'hidden' }}>
              <CassetteTimeline
                records={timelineRecords}
                hasMore={false}
                loading={loading}
                title={`${entityType} / ${entityId}`}
                supportsMessageType={true}
                onGroupByModeChange={mode => setGroupByKind(mode.kind)}
                extraControls={
                  loading
                    ? <span style={{ fontSize: 12, color: '#718096' }}>
                        Loading… {loadedCount} messages
                        <button style={{ ...secondaryBtn, marginLeft: 8 }} onClick={handleCancel}>Cancel</button>
                      </span>
                    : undefined
                }
              />
            </div>
```

to:

```tsx
            <div style={{ flex: '1 1 0', minHeight: 0, display: 'flex', gap: 12 }}>
              <div style={{ flex: '1 1 0', minWidth: 0, border: '1px solid #e2e8f0', borderRadius: 8, overflow: 'hidden' }}>
                <CassetteTimeline
                  records={timelineRecords}
                  hasMore={false}
                  loading={loading}
                  title={`${entityType} / ${entityId}`}
                  supportsMessageType={true}
                  onGroupByModeChange={mode => setGroupByKind(mode.kind)}
                  highlightPredicate={highlightPredicate}
                  extraControls={
                    loading
                      ? <span style={{ fontSize: 12, color: '#718096' }}>
                          Loading… {loadedCount} messages
                          <button style={{ ...secondaryBtn, marginLeft: 8 }} onClick={handleCancel}>Cancel</button>
                        </span>
                      : undefined
                  }
                />
              </div>
              <DatasetSummaryPanel
                kind="entity"
                entityType={entityType}
                entityId={entityId}
                selected={summarySelection}
                onSelect={handleSummarySelect}
              />
            </div>
```

(`useMemo` should already be imported from `react` per line 2 — confirm it's there; it is.)

- [ ] **Step 3: Type-check**

Run: `cd ui && pnpm exec tsc --noEmit`
Expected: no errors.

- [ ] **Step 4: Manual verification**

Run: `cd ui && pnpm dev`, open an entity's Timeline page, click "Load Messages".

Expected: the summary panel renders beside the timeline canvas; clicking a source-topic or message-type row dims non-matching markers, clicking again undims.

- [ ] **Step 5: Full frontend test suite and final check**

Run: `cd ui && pnpm test`
Expected: all tests pass, including the three new `DatasetSummaryPanel` tests from Task 7 and no regressions elsewhere.

Run: `cd joxette-service && mvn -q test`
Expected: all tests pass, including the 7 new summary tests from Tasks 2 and 3.

- [ ] **Step 6: Commit**

```bash
git add ui/src/routes/entities/\$entityType/\$entityId_.timeline.tsx
git commit -m "feat(ui): wire DatasetSummaryPanel into the entity timeline page"
```

# Compaction Disk-Space Cleanup + Entity-Page Compact Shortcut Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop compaction from silently leaking disk space (merged files' predecessors are never deleted), and add a "Compact Entity Type" shortcut button to the entity detail page.

**Architecture:** `CompactionService.executeRun` gains one new step, run once per run (not per target): after all entity/general merges finish, expire snapshots older than a configurable retention window and delete the now-unreferenced files those snapshots were the last reference to. The entity detail page's new button reuses the existing `POST /compaction/trigger` endpoint verbatim, scoped to the viewed entity's type.

**Tech Stack:** Java 25 / Spring Boot 4 / DuckDB+DuckLake (backend); React 19 / TanStack Router / TypeScript (UI). See `docs/superpowers/specs/2026-08-06-compaction-cleanup-and-entity-shortcut-design.md` for full background and the DuckLake API research behind this plan.

## Global Constraints

- New config key: `joxette.compaction.snapshot-retention-hours`, default `24`.
- The new cleanup step runs **once per compaction run**, after all entity + general merges — never per-target (`ducklake_cleanup_old_files` is catalog-wide; running it per-target would be redundant work and redundant locking).
- Failure handling for the new step matches the existing `ducklake_merge_adjacent_files` failure pattern in this class: log a WARN (classified via `DuckDbErrors.isTransient()`), never fail the whole run.
- The entity-page button compacts the entity's **type**, not just the viewed entity (there is no narrower scope — see spec). Label/tooltip must say so explicitly.
- No new backend endpoint for the UI button — reuse `POST /compaction/trigger` and the existing `compactionApi.trigger` client function.
- TDD throughout: RED (watch it fail) → GREEN → COMMIT per step.

---

### Task 1: `snapshotRetentionHours` config field

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java`
- Test: `joxette-service/src/test/java/com/joxette/compaction/CompactionServiceTest.java`

**Interfaces:**
- Produces: `JoxetteProperties.Compaction#getSnapshotRetentionHours(): int`, `#setSnapshotRetentionHours(int): void` — consumed by Task 2.

- [ ] **Step 1: Write the failing test**

Add to `CompactionServiceTest.java`, in the "History and status" section (or any existing section — it's a pure config assertion, no DB needed beyond the existing `props` object):

```java
@Test
void compactionConfig_snapshotRetentionHours_defaultsTo24() {
    JoxetteProperties props = new JoxetteProperties();
    assertThat(props.getCompaction().getSnapshotRetentionHours()).isEqualTo(24);
}

@Test
void compactionConfig_snapshotRetentionHours_isSettable() {
    JoxetteProperties props = new JoxetteProperties();
    props.getCompaction().setSnapshotRetentionHours(48);
    assertThat(props.getCompaction().getSnapshotRetentionHours()).isEqualTo(48);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -pl joxette-service test -Dtest=CompactionServiceTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation failure — `cannot find symbol: method getSnapshotRetentionHours()`.

- [ ] **Step 3: Add the config field**

In `JoxetteProperties.java`, inside `public static class Compaction { ... }`, immediately after the existing `deadInstanceThresholdMinutes` field/before its closing brace (the class currently ends around line 297 with that field — add after it, before `Entity`/`General` nested classes' surrounding structure stays unchanged):

```java
    /**
     * How long an old (superseded-by-merge) snapshot is kept before it is
     * expired and its files become eligible for cleanup, in hours.
     *
     * <p>{@code ducklake_merge_adjacent_files} does not delete the files it
     * replaces — DuckLake keeps them for time travel until their snapshot
     * is explicitly expired. Without this step, every compaction run leaks
     * disk space equal to whatever it just merged. Trades off disk space
     * held by superseded files (higher = more retained history) against
     * the time-travel/restore window (lower = less history available via
     * {@code GET /cassettes/snapshots} restore).
     */
    private int snapshotRetentionHours = 24;
```

And its getter/setter alongside the class's other getters/setters:

```java
        public int getSnapshotRetentionHours() { return snapshotRetentionHours; }
        public void setSnapshotRetentionHours(int snapshotRetentionHours) {
            this.snapshotRetentionHours = snapshotRetentionHours;
        }
```

(Match the exact placement style of `lockTtlMinutes`'s getter/setter in the same class — find and mirror it.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -pl joxette-service test -Dtest=CompactionServiceTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS, all tests in the class green (no regressions).

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java joxette-service/src/test/java/com/joxette/compaction/CompactionServiceTest.java
git commit -m "feat(compaction): add snapshot-retention-hours config (default 24)"
```

---

### Task 2: `CompactionService.expireSnapshotsAndCleanup()`

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/compaction/CompactionService.java`
- Test: `joxette-service/src/test/java/com/joxette/compaction/CompactionServiceTest.java`

**Interfaces:**
- Consumes: `props.getCompaction().getSnapshotRetentionHours()` (Task 1), `duckDB` (existing field), `DuckDbErrors.isTransient(SQLException)` (existing utility).
- Produces: `private void expireSnapshotsAndCleanup()` — called by Task 3.

The fake `DuckDBTestSupport` harness (`ATTACH ':memory:' AS lake`, no real DuckLake catalog) makes `ducklake_expire_snapshots`/`ducklake_cleanup_old_files` fail every time in this test class — exactly like the existing `ducklake_merge_adjacent_files` calls already do here (see `executeRun_completesSuccessfully`, which passes today despite the merge itself failing against the fake harness). This test therefore verifies the **failure-handling contract** (attempted, logged, run still completes) via log capture — the same shape as `ReconciliationServiceLifecycleTest`'s sibling tests. Task 4 adds a real end-to-end IT test that verifies files are actually deleted.

- [ ] **Step 1: Write the failing test**

Add to `CompactionServiceTest.java` (needs `ch.qos.logback.classic.Level`/`ILoggingEvent`/`ListAppender`/`LoggerFactory` — already imported at the top of this file):

```java
@Test
void executeRun_attemptsSnapshotExpiryAndCleanup_logsFailureButStillCompletes() throws Exception {
    ch.qos.logback.classic.Logger logger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CompactionService.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    Level savedLevel = logger.getLevel();
    logger.setLevel(Level.DEBUG);
    logger.addAppender(appender);

    CompactionRun started = service.beginRun(TriggerSource.MANUAL, null);
    List<String> messages;
    try {
        service.executeRun(started.id(), null);
        messages = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    } finally {
        logger.detachAppender(appender);
        logger.setLevel(savedLevel);
    }

    // Ran against the fake (non-DuckLake) harness, so this always fails — verifies the
    // attempt happened and the run survived it, not that the SQL itself succeeded here.
    assertThat(messages).anyMatch(m -> m.contains("expire") || m.contains("cleanup"));
    CompactionRun completed = service.getRunById(started.id());
    assertThat(completed.status()).isEqualTo(RunStatus.COMPLETED);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl joxette-service test -Dtest=CompactionServiceTest#executeRun_attemptsSnapshotExpiryAndCleanup_logsFailureButStillCompletes`
Expected: FAIL — no log message contains "expire" or "cleanup" (the method doesn't exist/isn't called yet).

- [ ] **Step 3: Implement `expireSnapshotsAndCleanup()`**

Add to `CompactionService.java`, right after the existing `checkpoint()` method (around line 610, after its closing brace — same "DuckDB / DuckLake helpers" section):

```java
    /**
     * Expires snapshots older than {@code snapshot-retention-hours} and deletes the
     * now-unreferenced files those snapshots were the last reference to. Runs once per
     * compaction run (see caller), after every {@code ducklake_merge_adjacent_files}
     * call — merging alone never deletes the files it replaces; DuckLake keeps them for
     * time travel until their snapshot is explicitly expired.
     *
     * <p>Failure here follows the same "log and continue" pattern as
     * {@code ducklake_merge_adjacent_files} failures elsewhere in this class: a missed
     * cleanup this run just means the space is reclaimed on the next successful run
     * instead of this one.
     */
    private void expireSnapshotsAndCleanup() {
        int retentionHours = props.getCompaction().getSnapshotRetentionHours();
        try {
            synchronized (duckDB) {
                try (Statement st = duckDB.createStatement()) {
                    log.debug("Expiring snapshots older than {}h and cleaning up old files", retentionHours);
                    st.execute("CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL '"
                            + retentionHours + "' HOUR)");
                    st.execute("CALL ducklake_cleanup_old_files('lake', cleanup_all => true)");
                }
            }
        } catch (SQLException e) {
            if (DuckDbErrors.isTransient(e)) {
                log.warn("Transient S3 failure during snapshot expiry/cleanup (will retry next run): {}",
                        e.getMessage());
            } else {
                log.warn("Snapshot expiry/cleanup failed: {}", e.getMessage());
            }
        }
    }
```

- [ ] **Step 4: Run test to verify it still fails (method exists but isn't wired up)**

Run: `mvn -pl joxette-service test -Dtest=CompactionServiceTest#executeRun_attemptsSnapshotExpiryAndCleanup_logsFailureButStillCompletes`
Expected: still FAIL — same reason, `executeRun` doesn't call the new method yet. Confirms Step 3 alone isn't enough (proceed to Task 3 before re-checking).

- [ ] **Step 5: Commit (method added, not yet wired — Task 3 wires it and re-verifies this test)**

```bash
git add joxette-service/src/main/java/com/joxette/compaction/CompactionService.java joxette-service/src/test/java/com/joxette/compaction/CompactionServiceTest.java
git commit -m "feat(compaction): add expireSnapshotsAndCleanup (not yet called)"
```

---

### Task 3: Wire `expireSnapshotsAndCleanup()` into `executeRun`

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/compaction/CompactionService.java`

**Interfaces:**
- Consumes: `expireSnapshotsAndCleanup()` (Task 2).

- [ ] **Step 1: Wire the call into `executeRun`**

In `executeRun` (around line 146-187), insert the call between the two compaction passes and `checkpoint()`:

```java
            CompactionResult entityResult = compactEntityTypes(targets);
            entityTypes    = entityResult.unitsProcessed();
            totalFileStats = totalFileStats.add(entityResult.fileStats());

            CompactionResult generalResult = compactGeneralIfEnabled(targets);
            generalTopics  = generalResult.unitsProcessed();
            totalFileStats = totalFileStats.add(generalResult.fileStats());

            expireSnapshotsAndCleanup();

            checkpoint();
```

(Only the `expireSnapshotsAndCleanup();` line is new — it replaces the blank line that was between the general-compaction block and `checkpoint()`.)

- [ ] **Step 2: Run the Task 2 test to verify it now passes**

Run: `mvn -pl joxette-service test -Dtest=CompactionServiceTest#executeRun_attemptsSnapshotExpiryAndCleanup_logsFailureButStillCompletes`
Expected: PASS — the WARN log line now fires (fake harness still fails the SQL, but the attempt now happens), and the run still completes.

- [ ] **Step 3: Run the full test class to verify no regressions**

Run: `mvn -pl joxette-service test -Dtest=CompactionServiceTest,CompactionLockRaceTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS, all tests green.

- [ ] **Step 4: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/compaction/CompactionService.java
git commit -m "feat(compaction): expire old snapshots and clean up superseded files every run"
```

---

### Task 4: Real end-to-end IT — files are actually deleted from object storage

**Files:**
- Create: `joxette-service/src/test/java/com/joxette/it/CompactionSnapshotCleanupIT.java`

**Interfaces:**
- Consumes: `CompactionService` (Spring-injected, real DuckLake + MinIO via Testcontainers — same pattern as `ReconciliationOrphanedFilesIT`).

This is the test that proves the actual point of this feature: after compaction, the physical file count in object storage goes *down*, not just the catalog's logical view of it.

- [ ] **Step 1: Write the IT test**

```java
package com.joxette.it;

import com.joxette.compaction.CompactionService;
import com.joxette.compaction.RunStatus;
import com.joxette.compaction.TriggerSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class CompactionSnapshotCleanupIT {

    private static final String BUCKET = "joxette-compact-test";
    private static final String ENTITY_TYPE = "cleanuptest";

    @Container
    static final MinIOContainer minio =
            new MinIOContainer(DockerImageName.parse("minio/minio:RELEASE.2024-01-16T16-07-38Z"));

    @DynamicPropertySource
    static void minioProperties(DynamicPropertyRegistry registry) {
        String s3Url = minio.getS3URL();
        String userName = minio.getUserName();
        String password = minio.getPassword();
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(s3Url))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(userName, password)))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build()) {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
        registry.add("joxette.catalog.object-storage-path", () -> "s3://" + BUCKET + "/data/");
        registry.add("joxette.s3.endpoint",   () -> s3Url);
        registry.add("joxette.s3.access-key", () -> userName);
        registry.add("joxette.s3.secret-key", () -> password);
        registry.add("joxette.compaction.snapshot-retention-hours", () -> "0");
    }

    @Autowired private Connection duckDB;
    @Autowired private CompactionService compactionService;

    @Test
    void compaction_mergesFilesAndReclaimsDiskSpace() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            // Column list/types match SchemaManager.createEntityCassetteTable's real DDL
            // exactly (kafka_key is BLOB, not VARCHAR — easy to get wrong).
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.entity_" + ENTITY_TYPE + " (" +
                    "recorded_at TIMESTAMPTZ, entity_id VARCHAR, bucket INTEGER, message_type VARCHAR, " +
                    "topic VARCHAR, kafka_offset BIGINT, kafka_partition INTEGER, kafka_timestamp TIMESTAMPTZ, " +
                    "kafka_key BLOB, kafka_value BLOB, metadata VARCHAR, " +
                    "headers STRUCT(key VARCHAR, value VARCHAR)[])");
            st.execute("INSERT INTO entity_type_configs (entity_type, bucket_count) VALUES ('"
                    + ENTITY_TYPE + "', 64) ON CONFLICT DO NOTHING");
        }

        // Four separate inserts, each flushed immediately, to force four distinct small
        // Parquet files — matching how real batches land as separate files over time.
        Instant ts = Instant.parse("2020-01-01T00:00:00Z"); // cold enough for lookback-days=0 to compact it
        for (int i = 0; i < 4; i++) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "INSERT INTO lake.main.entity_" + ENTITY_TYPE +
                    " (recorded_at, entity_id, bucket, message_type, topic, kafka_offset, kafka_partition, " +
                    "  kafka_timestamp, kafka_key, kafka_value, metadata, headers) " +
                    "VALUES (?, ?, 0, 'order', 'orders.events', ?, 0, ?, ?, ?, NULL, [])")) {
                ps.setObject(1, ts);
                ps.setString(2, "ENT-" + i);
                ps.setLong(3, i); // kafka_offset
                ps.setObject(4, ts); // kafka_timestamp
                ps.setBytes(5, ("k" + i).getBytes()); // kafka_key is BLOB, not VARCHAR
                ps.setBytes(6, ("v" + i).getBytes()); // kafka_value
                ps.executeUpdate();
            }
            try (Statement st = duckDB.createStatement()) {
                st.execute("CALL ducklake_flush_inlined_data('lake')");
            }
        }

        int filesBefore = countParquetFiles();
        assertThat(filesBefore).isGreaterThanOrEqualTo(4);

        var run = compactionService.beginRun(TriggerSource.MANUAL, java.util.List.of(ENTITY_TYPE));
        compactionService.executeRun(run.id(), java.util.List.of(ENTITY_TYPE));

        var completed = compactionService.getRunById(run.id());
        assertThat(completed.status()).isEqualTo(RunStatus.COMPLETED);

        int filesAfter = countParquetFiles();
        assertThat(filesAfter)
                .as("compaction should have merged files AND cleaned up the superseded originals")
                .isLessThan(filesBefore);
    }

    private int countParquetFiles() throws Exception {
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT COUNT(*) FROM glob('s3://" + BUCKET + "/data/main/entity_" + ENTITY_TYPE + "/**/*.parquet')")) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
```

- [ ] **Step 2: Run it to verify it fails against the unmodified... wait, it won't fail**

This test exercises the already-implemented Task 3 code path, so it should PASS on first run if Tasks 1-3 are correct — there's no separate RED step for an IT test added after the unit-level RED/GREEN cycle already proved the logic. Run it once to confirm:

Run: `mvn -pl joxette-service verify -Dit.test=CompactionSnapshotCleanupIT -Dtest=CompactionSnapshotCleanupIT -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS. `filesAfter < filesBefore`, proving physical deletion happened, not just catalog-level supersession.

Note: `CompactionService.doCompactEntityType` calls `ducklake_merge_adjacent_files` unconditionally — `Entity#minFilesPerBucket`/`lookbackDays` config exists but is never read anywhere in `CompactionService.java` (confirmed via grep), so no bucket-count or age gating applies here; the merge runs against whatever files exist in the table, bounded only by `max_file_size`. If this test does NOT pass, that's a real signal Task 2/3's SQL or sequencing has a bug — fix forward, don't weaken the assertion.

- [ ] **Step 3: Commit**

```bash
git add joxette-service/src/test/java/com/joxette/it/CompactionSnapshotCleanupIT.java
git commit -m "test(compaction): verify compaction reclaims real disk space (Testcontainers MinIO)"
```

---

### Task 5: Entity-page "Compact Entity Type" button

**Files:**
- Modify: `ui/src/routes/entities/$entityType/$entityId.tsx`

**Interfaces:**
- Consumes: `compactionApi.trigger(body?: TriggerRequest)` (existing, `ui/src/api/client.ts:1031-1032`), `TriggerRequest { targets?: string[] }` (existing, `ui/src/api/client.ts:285-287`), `addToast(message: string, kind: 'success' | 'error')` (existing, `useToast()` — already imported and used by `deleteMutation` at `$entityId.tsx:181-189`).

No backend change, no new test file — this is a UI-only wiring task. Per project convention, verify via the browser (no page-level component tests exist in this codebase).

- [ ] **Step 1: Add `compactionApi` to the existing import**

`$entityId.tsx:12` currently reads:

```tsx
import { cassettesApi, entityOutputApi, entitiesApi, streamEntityRecords, type EntityRecord, type Order, type StreamMode, type EntityStreamParams, type PortraitResult } from '../../../api/client'
```

Add `compactionApi` to that same import list:

```tsx
import { cassettesApi, entityOutputApi, entitiesApi, compactionApi, streamEntityRecords, type EntityRecord, type Order, type StreamMode, type EntityStreamParams, type PortraitResult } from '../../../api/client'
```

- [ ] **Step 2: Add the mutation**

Immediately after the existing `deleteMutation` block (`$entityId.tsx:181-189`), add:

```tsx
  const compactMutation = useMutation({
    mutationFn: () => compactionApi.trigger({ targets: [`entity:${entityType}`] }),
    onSuccess: () => addToast(`Compaction triggered for all "${entityType}" entities`, 'success'),
    onError: (e: Error) => addToast(e.message, 'error'),
  })
```

- [ ] **Step 3: Add the button**

In the header action-button row (`$entityId.tsx:336-354`), add a third button between the existing Timeline link and Delete button — neutral gray styling (secondary action: not primary nav like Timeline's blue, not destructive like Delete's red):

```tsx
          <button
            data-testid="btn-compact-entity-type"
            aria-label={`Compact all entities of type ${entityType}`}
            title={`Merges small files for every entity of type "${entityType}" into fewer, larger ones — not just this entity. Runs in the background; large tables may take a while. See the Compaction page for progress.`}
            disabled={compactMutation.isPending}
            style={{ padding: '0.45rem 1rem', background: '#edf2f7', color: '#2d3748', border: '1px solid #cbd5e0', borderRadius: 4, cursor: compactMutation.isPending ? 'default' : 'pointer', fontSize: 14 }}
            onClick={() => compactMutation.mutate()}
          >
            {compactMutation.isPending ? 'Compacting…' : `Compact "${entityType}" Entities`}
          </button>
```

So the row becomes: Timeline link → new Compact button → Delete button (`$entityId.tsx:337-353` after this edit).

- [ ] **Step 4: Type-check**

Run: `cd ui && pnpm exec tsc --noEmit`
Expected: no new errors introduced by this file (pre-existing unrelated errors in other files, e.g. `SunburstChart.tsx`, are not this task's concern).

- [ ] **Step 5: Browser verification**

Start the backend (against real or local-filesystem object storage) and `pnpm dev` on the CORS-allowlisted port (4173 or 5173). Navigate to an entity detail page for a known entity type. Confirm:
- The button renders with the type-scoped label, next to Timeline/Delete.
- Hovering shows the tooltip clarifying it compacts the whole type, not just this entity.
- Clicking it shows "Compacting…", then either the success or error toast (via `addToast`).
- Cross-check the Compaction page (`/compaction`) shows a new run triggered with `targets: ["entity:{type}"]`.

- [ ] **Step 6: Commit**

```bash
git add ui/src/routes/entities/\$entityType/\$entityId.tsx
git commit -m "feat(ui/entities): add \"Compact Entity Type\" shortcut button"
```

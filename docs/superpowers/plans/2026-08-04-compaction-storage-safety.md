# Compaction & Storage Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close five concrete data-safety gaps in Joxette's compaction, retention, and snapshot-restore subsystems so that cross-node compaction is actually mutually exclusive, crashed-instance locks recover quickly, snapshot restores are verified and recorder-safe, retention deletes reclaim disk, and entity bucket-count changes cannot silently desynchronize data.

**Architecture:** `CompactionLockManager` already implements a correct catalog-row distributed lock (`compaction_locks` table) but was orphaned when `CompactionSingletonActor` was introduced — that actor's exclusivity is only cluster-wide under `clustering.mode: pekko-management`, and `CompactionScheduler`/`RetentionScheduler` bypass it entirely by running `@Scheduled` on every node. Tasks 1–2 restore `CompactionLockManager` as the real cross-node safety net (mode-independent) and fix its crash-recovery instance-ID scheme. Tasks 3–5 are independent hardening of snapshot restore, retention file reclamation, and entity bucket-count mutation — each closes a gap where an operation silently succeeds into an inconsistent state instead of failing loudly.

**Tech Stack:** Java 25, Spring Boot 4.0.5, DuckDB JDBC 1.5.3.0 / DuckLake, Apache Pekko typed actors + ClusterSingleton, JUnit 5, Awaitility (async assertions — never raw Thread.sleep before an assertion), Testcontainers

## Global Constraints

- Java 25 language features only (no Kotlin)
- Never use `Thread.sleep` before a test assertion — use `Awaitility.await().atMost(...).untilAsserted(...)`
- Prefer `@ParameterizedTest` (`@CsvSource` for scalars, `@MethodSource` for objects) over multiple near-identical `@Test` methods
- All JDBC operations on the shared `Connection` must be wrapped `synchronized(duckDB)` unless already inside a path holding that lock
- DuckLake catalog schema must remain identical across embedded/Quack/PostgreSQL backends — no backend-specific SQL in new code

## Task order

Tasks 1 and 2 touch the same lock-manager machinery and must be done together, in order, first. Tasks 3, 4, and 5 are independent of each other and of 1/2 — they may be executed in parallel by separate subagents once 1/2 are committed.

---

### Task 1: Wire `CompactionLockManager` back into `CompactionService`

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/compaction/CompactionService.java` (constructor lines 87–95; `doCompactEntityType` lines 274–319; `doCompactGeneralTopic` lines 355–387)
- Modify: `joxette-service/src/main/java/com/joxette/compaction/CompactionController.java` (constructor lines 60–80; `getLocks()` lines 226–242)
- Modify: `joxette-service/src/test/java/com/joxette/compaction/CompactionServiceTest.java` (constructor calls at lines 74 and 460)
- Modify: `joxette-service/src/test/java/com/joxette/api/error/CompactionControllerProblemDetailTest.java` (constructor calls at lines 110, 133, 155)
- Modify: `joxette-service/src/test/java/com/joxette/compaction/CompactionDistributedLockTest.java` (stale class docstring, lines 16–27)
- Test (create): `joxette-service/src/test/java/com/joxette/compaction/CompactionLockRaceTest.java`

**Interfaces:**
- Consumes: `CompactionLockManager.tryAcquire(String target): boolean throws SQLException`, `CompactionLockManager.release(String target): void`, `CompactionLockManager.listActiveLocks(): List<CompactionLockInfo> throws SQLException` (all pre-existing, unmodified)
- Produces: `CompactionService(Connection, JoxetteProperties, ConfigRepository, JoxetteMetrics, CompactionLockManager)` (new constructor signature); `CompactionController(CompactionService, RetentionService, ActorRef<CompactionSingletonActor.CompactionCommand>, ActorSystem<?>, JoxetteProperties, BackgroundTaskRegistry, CompactionLockManager)` (new constructor signature); `GET /compaction/locks` now returns real `List<CompactionLockInfo>` instead of `List.of()`

- [ ] **Step 1: Write failing test**

Create `joxette-service/src/test/java/com/joxette/compaction/CompactionLockRaceTest.java`:

```java
package com.joxette.compaction;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.support.DuckDBTestSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves {@link CompactionLockManager} is the actual cross-node mutual-exclusion
 * mechanism for {@link CompactionService}: two service instances (simulating two
 * Joxette processes sharing one catalog) racing to compact the same entity type
 * must never both proceed past lock acquisition to the merge call.
 */
class CompactionLockRaceTest {

    private static final JoxetteMetrics TEST_METRICS = new JoxetteMetrics(new SimpleMeterRegistry());
    private static final String ENTITY_TYPE = "order";

    private Connection duckDB;
    private CompactionService serviceA;
    private CompactionService serviceB;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createEntityTable(duckDB, ENTITY_TYPE);

        try (PreparedStatement ps = duckDB.prepareStatement(
                "INSERT INTO entity_type_configs (entity_type, bucket_count) VALUES (?, ?)")) {
            ps.setString(1, ENTITY_TYPE);
            ps.setInt(2, 64);
            ps.executeUpdate();
        }

        JoxetteProperties props = testProperties();
        ConfigRepository configRepo = new ConfigRepository(duckDB, props);

        CompactionLockManager lockA = new CompactionLockManager(duckDB, 120, "node-a:1001");
        CompactionLockManager lockB = new CompactionLockManager(duckDB, 120, "node-b:2002");
        serviceA = new CompactionService(duckDB, props, configRepo, TEST_METRICS, lockA);
        serviceB = new CompactionService(duckDB, props, configRepo, TEST_METRICS, lockB);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    @Test
    void twoNodesRacingToCompactSameEntityType_onlyOneProceedsPastTheLock() throws Exception {
        CountDownLatch startGate = new CountDownLatch(1);
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CompactionService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level savedLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            var futureA = pool.submit(() -> runOnce(serviceA, startGate));
            var futureB = pool.submit(() -> runOnce(serviceB, startGate));
            startGate.countDown();
            futureA.get(10, TimeUnit.SECONDS);
            futureB.get(10, TimeUnit.SECONDS);
        } finally {
            pool.shutdown();
            logger.detachAppender(appender);
            logger.setLevel(savedLevel);
        }

        long mergeAttempts = appender.list.stream()
                .filter(e -> e.getFormattedMessage().contains("Merging adjacent files for entity_type='order'"))
                .count();
        long skips = appender.list.stream()
                .filter(e -> e.getFormattedMessage().contains("held by another instance"))
                .count();

        assertThat(mergeAttempts)
                .as("Exactly one of the two racing instances must proceed past the lock to attempt the merge")
                .isEqualTo(1);
        assertThat(skips)
                .as("The other instance must observe the lock held and back off")
                .isEqualTo(1);
        assertThat(serviceA.isRunning()).isFalse();
        assertThat(serviceB.isRunning()).isFalse();
    }

    private Void runOnce(CompactionService service, CountDownLatch startGate) throws Exception {
        startGate.await(10, TimeUnit.SECONDS);
        CompactionRun run = service.beginRun(TriggerSource.MANUAL, List.of(ENTITY_TYPE));
        service.executeRun(run.id(), List.of(ENTITY_TYPE));
        return null;
    }

    private JoxetteProperties testProperties() {
        JoxetteProperties props = new JoxetteProperties();
        props.getCompaction().setSchedule("0 0 3 * * *");
        props.getCompaction().getEntity().setLookbackDays(0);
        props.getCompaction().getEntity().setMinFilesPerBucket(1);
        props.getCompaction().getGeneral().setEnabled(false);
        props.getCompaction().setLockTtlMinutes(120);
        return props;
    }
}
```

- [ ] **Step 2: Run it and confirm the exact failure**

```bash
mvn -pl joxette-service -am test -Dtest=CompactionLockRaceTest
```

Expected failure: **compile error**, not a test failure — `CompactionService` has no constructor accepting a `CompactionLockManager`:

```
cannot find symbol
  symbol:   constructor CompactionService(Connection,JoxetteProperties,ConfigRepository,JoxetteMetrics,CompactionLockManager)
```

- [ ] **Step 3: Write minimal real implementation code**

Edit `joxette-service/src/main/java/com/joxette/compaction/CompactionService.java`:

Add the field and constructor parameter:

```java
    private final Connection           duckDB;
    private final JoxetteProperties    props;
    private final ConfigRepository     configRepo;
    private final AtomicBoolean        running = new AtomicBoolean(false);
    private final io.micrometer.core.instrument.Counter filesProcessedCounter;
    private final io.micrometer.core.instrument.Counter filesCreatedCounter;
    private final Timer compactionTimer;
    private final CompactionLockManager lockManager;

    public CompactionService(Connection duckDB, JoxetteProperties props, ConfigRepository configRepo,
                              JoxetteMetrics joxetteMetrics, CompactionLockManager lockManager) {
        this.duckDB                = duckDB;
        this.props                 = props;
        this.configRepo            = configRepo;
        this.filesProcessedCounter = joxetteMetrics.compactionFilesProcessed();
        this.filesCreatedCounter   = joxetteMetrics.compactionFilesCreated();
        this.compactionTimer       = joxetteMetrics.compactionDuration();
        this.lockManager           = lockManager;
    }
```

Replace `doCompactEntityType`:

```java
    private CompactionResult doCompactEntityType(String entityType) {
        SchemaManager.validateEntityType(entityType);
        String lockTarget = "entity:" + entityType;
        boolean acquired;
        try {
            acquired = lockManager.tryAcquire(lockTarget);
        } catch (SQLException e) {
            log.warn("Could not acquire compaction lock '{}' ({}); skipping this run", lockTarget, e.getMessage());
            return CompactionResult.NONE;
        }
        if (!acquired) {
            log.info("Compaction lock '{}' held by another instance — skipping (will retry next scheduled run)",
                    lockTarget);
            return CompactionResult.NONE;
        }
        try {
            long maxFileSizeBytes = (long) props.getCompaction().getEntity().getTargetFileSizeMb() * 1024L * 1024L;
            int rowGroupMemoryLimitMb = props.getCompaction().getEntity().getRowGroupMemoryLimitMb();
            String sql = "CALL ducklake_merge_adjacent_files('lake', 'entity_" + entityType + "',"
                       + " max_file_size => " + maxFileSizeBytes + ")";
            log.debug("Merging adjacent files for entity_type='{}'", entityType);
            synchronized (duckDB) {
                if (rowGroupMemoryLimitMb > 0) {
                    try (Statement st = duckDB.createStatement()) {
                        st.execute("SET write_buffer_row_group_memory_limit = '"
                                + rowGroupMemoryLimitMb + "MB'");
                        log.debug("write_buffer_row_group_memory_limit set to {} MB for entity_{} merge",
                                rowGroupMemoryLimitMb, entityType);
                    } catch (SQLException setEx) {
                        log.debug("write_buffer_row_group_memory_limit not applied for entity_{} "
                                + "(DuckDB < 1.5.3?): {}", entityType, setEx.getMessage());
                    }
                }
                try (Statement st = duckDB.createStatement();
                     ResultSet rs = st.executeQuery(sql)) {
                    FileStats stats = FileStats.EMPTY;
                    while (rs.next()) {
                        stats = stats.add(new FileStats(
                                rs.getLong("files_processed"),
                                rs.getLong("files_created")));
                    }
                    log.debug("Merged adjacent files for entity_{}: files_processed={} files_created={}",
                            entityType, stats.filesProcessed(), stats.filesCreated());
                    return new CompactionResult(stats.filesProcessed() > 0 ? 1 : 0, stats);
                }
            }
        } catch (SQLException e) {
            if (DuckDbErrors.isTransient(e)) {
                log.warn("ducklake_merge_adjacent_files transient S3 failure for entity_type='{}' (will retry next run): {}",
                        entityType, e.getMessage());
            } else {
                log.warn("ducklake_merge_adjacent_files failed for entity_type='{}': {}", entityType, e.getMessage());
            }
            return CompactionResult.NONE;
        } finally {
            lockManager.release(lockTarget);
        }
    }
```

Replace `doCompactGeneralTopic`:

```java
    private CompactionResult doCompactGeneralTopic(String topic) {
        String tableName = "general_" + normalizeTopicName(topic);
        String lockTarget = "topic:" + tableName;
        boolean acquired;
        try {
            acquired = lockManager.tryAcquire(lockTarget);
        } catch (SQLException e) {
            log.warn("Could not acquire compaction lock '{}' ({}); skipping this run", lockTarget, e.getMessage());
            return CompactionResult.NONE;
        }
        if (!acquired) {
            log.info("Compaction lock '{}' held by another instance — skipping (will retry next scheduled run)",
                    lockTarget);
            return CompactionResult.NONE;
        }
        try {
            long maxFileSizeBytes = (long) props.getCompaction().getGeneral().getTargetFileSizeMb() * 1024L * 1024L;
            String sql = "CALL ducklake_merge_adjacent_files('lake', '" + tableName + "',"
                       + " max_file_size => " + maxFileSizeBytes + ")";
            log.debug("Merging adjacent files for general cassette topic='{}'", topic);
            synchronized (duckDB) {
                try (Statement st = duckDB.createStatement();
                     ResultSet rs = st.executeQuery(sql)) {
                    FileStats stats = FileStats.EMPTY;
                    while (rs.next()) {
                        stats = stats.add(new FileStats(
                                rs.getLong("files_processed"),
                                rs.getLong("files_created")));
                    }
                    log.debug("Merged adjacent files for general cassette topic='{}': "
                                    + "files_processed={} files_created={}",
                            topic, stats.filesProcessed(), stats.filesCreated());
                    return new CompactionResult(stats.filesProcessed() > 0 ? 1 : 0, stats);
                }
            }
        } catch (SQLException e) {
            if (DuckDbErrors.isTransient(e)) {
                log.warn("ducklake_merge_adjacent_files transient S3 failure for general cassette topic='{}' (will retry next run): {}",
                        topic, e.getMessage());
            } else {
                log.warn("ducklake_merge_adjacent_files failed for general cassette topic='{}': {}",
                        topic, e.getMessage());
            }
            return CompactionResult.NONE;
        } finally {
            lockManager.release(lockTarget);
        }
    }
```

Edit `joxette-service/src/main/java/com/joxette/compaction/CompactionController.java` — add the field, constructor param, and un-hardcode `getLocks()`:

```java
    private final CompactionService   compactionService;
    private final RetentionService    retentionService;
    private final ActorRef<CompactionSingletonActor.CompactionCommand> compactionSingleton;
    private final ActorSystem<?>      system;
    private final JoxetteProperties   props;
    private final BackgroundTaskRegistry taskRegistry;
    private final CompactionLockManager lockManager;

    public CompactionController(
            CompactionService compactionService,
            RetentionService retentionService,
            ActorRef<CompactionSingletonActor.CompactionCommand> compactionSingleton,
            ActorSystem<?> system,
            JoxetteProperties props,
            BackgroundTaskRegistry taskRegistry,
            CompactionLockManager lockManager) {
        this.compactionService   = compactionService;
        this.retentionService    = retentionService;
        this.compactionSingleton = compactionSingleton;
        this.system              = system;
        this.props               = props;
        this.taskRegistry        = taskRegistry;
        this.lockManager         = lockManager;
    }
```

```java
    @Operation(
        operationId = "getCompactionLocks",
        summary = "List active compaction distributed locks",
        description = "Returns every row currently held in the compaction_locks table — the cross-node " +
                      "mutual-exclusion mechanism CompactionService uses before running " +
                      "ducklake_merge_adjacent_files. Populated regardless of joxette.clustering.mode, " +
                      "since the lock is a catalog row, not a Pekko cluster mechanism."
    )
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Active (and any not-yet-swept stale) compaction locks",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                schema = @Schema(type = "array", implementation = CompactionLockInfo.class))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
    @GetMapping(value = "/locks", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<CompactionLockInfo> getLocks() throws SQLException {
        return lockManager.listActiveLocks();
    }
```

Fix the now-broken existing call sites — edit `joxette-service/src/test/java/com/joxette/compaction/CompactionServiceTest.java` line 74:

```java
        configRepo = new ConfigRepository(duckDB, props);
        service = new CompactionService(duckDB, props, configRepo, TEST_METRICS,
                new CompactionLockManager(duckDB, 120, "test-instance"));
```

and line 460:

```java
        JoxetteProperties props = testProperties();
        props.getCompaction().getEntity().setRowGroupMemoryLimitMb(limitMb);
        CompactionService svc = new CompactionService(duckDB, props, configRepo, TEST_METRICS,
                new CompactionLockManager(duckDB, 120, "test-instance"));
```

Edit `joxette-service/src/test/java/com/joxette/api/error/CompactionControllerProblemDetailTest.java` — add the mock field:

```java
    @Mock CompactionService compactionService;
    @Mock RetentionService retentionService;
    @Mock JoxetteProperties props;
    @Mock com.joxette.compaction.CompactionLockManager lockManager;
```

and append `, lockManager` to all three `new CompactionController(...)` calls (lines 110, 133, 155):

```java
        CompactionController controller = new CompactionController(
                compactionService, retentionService, busySingleton, actorSystem, props, taskRegistry(), lockManager);
```

Edit `joxette-service/src/test/java/com/joxette/compaction/CompactionDistributedLockTest.java` — remove the stale "no longer used" claim (lines 16–27):

```java
/**
 * Integration tests for {@link CompactionLockManager}.
 *
 * <p>Two lock-manager instances share the same in-memory DuckDB connection.
 * Covers lock acquisition, release, expiry cleanup, and startup cleanup.
 *
 * <p>{@link CompactionService} acquires this lock around every
 * {@code ducklake_merge_adjacent_files} call (see {@link CompactionLockRaceTest}
 * for the cross-instance race coverage) — it is the actual cross-node safety net
 * regardless of {@code joxette.clustering.mode}, since it is a catalog row, not
 * a Pekko cluster mechanism.
 */
```

- [ ] **Step 4: Run and confirm pass**

```bash
mvn -pl joxette-service -am test -Dtest=CompactionLockRaceTest,CompactionServiceTest,CompactionDistributedLockTest,CompactionControllerProblemDetailTest
```

Confirm all classes pass, including the new race assertions (`mergeAttempts == 1`, `skips == 1`).

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/compaction/CompactionService.java \
        joxette-service/src/main/java/com/joxette/compaction/CompactionController.java \
        joxette-service/src/test/java/com/joxette/compaction/CompactionServiceTest.java \
        joxette-service/src/test/java/com/joxette/compaction/CompactionDistributedLockTest.java \
        joxette-service/src/test/java/com/joxette/compaction/CompactionLockRaceTest.java \
        joxette-service/src/test/java/com/joxette/api/error/CompactionControllerProblemDetailTest.java
git commit -m "fix(compaction): wire CompactionLockManager into CompactionService as the real cross-node lock

CompactionScheduler/RetentionScheduler run on every node regardless of
clustering.mode, and CompactionSingletonActor's exclusivity is only
cluster-wide under pekko-management mode. CompactionLockManager was fully
built and tested but never called, leaving no cross-node mutual exclusion
under the default catalog clustering mode. doCompactEntityType/
doCompactGeneralTopic now acquire+release a compaction_locks row around
every ducklake_merge_adjacent_files call; GET /compaction/locks reports
real lock state instead of a hardcoded empty list."
```

---

### Task 2: Fix `CompactionLockManager.releaseOwnLocks()` startup-recovery bug

**REVISION (post-task-review, supersedes everything below in this Task 2 section — implemented on top of the hostname-only commit already on the branch, not a plain revert):**

The hostname-only `buildInstanceId()` fix (below) was implemented and shipped, but review found it trades one bug for another: two Joxette processes co-located on the same host (a documented, supported topology in `docs/clustering-deployment.md` §7 — role-specialised `systemd` units) now share the same lock-ownership identity, so one process's `@PostConstruct releaseOwnLocks()` can delete a *different, still-running* process's active lock. The human product owner rejected building the fix on `@ConditionalOnProperty`-derived "role" identity (that annotation-based gating mechanism is being phased out project-wide) and instead directed a data-driven redesign reusing the existing `InstanceRegistry`/`joxette_instances` heartbeat table — the same live cluster-visibility mechanism `GET /instances` already exposes.

**Corrected design:**

1. **`CompactionLockManager` stops computing its own instance identity.** Remove `buildInstanceId()` entirely. Inject `InstanceRegistry` via constructor and use `instanceRegistry.getInstanceId()` (format: `hostname:pid`, already globally unique per process — no ambiguity, no co-location collision, since `InstanceRegistry` already solves exactly this uniqueness problem for its own table) as `CompactionLockManager`'s `instanceId` field. Revert `CompactionLockInfo`'s `@Schema` example back to the `hostname:pid` shape it had before the hostname-only commit (it was correct before; the hostname-only commit was the deviation).

2. **Replace `@PostConstruct releaseOwnLocks()`'s same-identity-matching logic with liveness-registry-driven cleanup.** New method, e.g. `cleanLocksForDeadInstances()`: call `instanceRegistry.listAll()`, build the set of instance IDs whose `status()` is `"alive"`, then `DELETE FROM compaction_locks WHERE instance_id NOT IN (<that set>)` (parameterize safely — do not string-concatenate instance IDs into SQL; build the `IN` clause with `?` placeholders sized to the live set, or if the live set is empty, delete all rows). This works uniformly for a same-host restart (the old PID's `instance_id` naturally ages out of `joxette_instances` and becomes non-live) and a remote crashed instance in shared-catalog mode (Quack/PostgreSQL) — no special-casing "is this my own past self" is needed, because ownership recovery no longer depends on identity-matching at all.

3. **Call sites — two, for both fast startup recovery and ongoing protection:**
   - Keep a `@PostConstruct` call (rename `releaseOwnLocks()` → the new method name, or add a thin `@PostConstruct` wrapper calling it), but add `InstanceRegistry`'s Spring bean name to `CompactionLockManager`'s existing `@DependsOn` (`@DependsOn({"dbSchemaManager", "instanceRegistry"})`) so `InstanceRegistry.initialize()` — which calls `reapStaleInstances()` before `upsertSelf()` — has already run and reaped genuinely stale rows before this liveness check queries `joxette_instances`. Without this ordering, a fresh restart could see its own not-yet-reaped stale row and skip cleanup it should have done.
   - Also call the same method from `CompactionService.executeRun()`'s existing opportunistic-cleanup call site (where Task 1 already wired `cleanExpiredLocks()`), so recovery isn't gated on this specific instance restarting — any live instance's scheduled/triggered compaction run reclaims any other instance's dead locks, bounded by the heartbeat/staleness cadence (~1-2 minutes) rather than waiting for the next restart of the crashed instance.
   - **Keep `cleanExpiredLocks()` (TTL-based) as-is, called alongside, not replaced.** It remains the correct backstop for a lock held by a genuinely *alive* instance whose merge has simply run past the (240-minute) TTL — liveness-based cleanup must never delete a still-alive instance's lock just because its merge is slow, only because the instance itself is confirmed dead.

4. **Circular-dependency check:** verify `InstanceRegistry`'s own constructor dependencies (`Connection`, `JoxetteProperties`, `DuckLakeManager`, `@Lazy RecordingCoordinator`, `ObjectMapper`) do not transitively depend on `CompactionLockManager` or `CompactionService` — read `InstanceRegistry.java`'s imports/constructor to confirm before wiring the new dependency. If a cycle is found, use `@Lazy` injection on the `InstanceRegistry` parameter in `CompactionLockManager`'s constructor to break it.

5. **Tests:** remove or rewrite the two hostname-only-scheme tests added by the prior commit (`buildInstanceId_isHostnameOnly_stableAcrossCalls`, `releaseOwnLocks_reclaimsSameHostLockImmediately_notViaTtlExpiry`) since `buildInstanceId()` no longer exists. Add new tests proving: (a) a lock owned by an instance_id with no live `joxette_instances` row is reclaimed by `cleanLocksForDeadInstances()`; (b) a lock owned by an instance_id that DOES have a live row is NOT reclaimed (proving the co-location collision risk is closed — two co-located processes' locks no longer interfere with each other); (c) a lock owned by a live instance is also not touched by TTL-based `cleanExpiredLocks()` before its TTL actually elapses (regression guard, should already pass, confirms nothing broke). Use a real `InstanceRegistry` (or a minimal real DB-backed fixture inserting rows directly into `joxette_instances`) rather than mocking it, consistent with this suite's existing real-behavior-over-mocks convention.

---

**Original task (superseded above, kept for history):**

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/compaction/CompactionLockManager.java` (`buildInstanceId()` lines 270–278; class javadoc lines 40–43; `getInstanceId()` javadoc lines 253–254)
- Modify: `joxette-service/src/main/java/com/joxette/compaction/CompactionLockInfo.java` (`@Schema` example, lines 22–24)
- Modify: `joxette-service/src/test/java/com/joxette/compaction/CompactionDistributedLockTest.java` (add two tests)

**Interfaces:**
- Consumes: `InetAddress.getLocalHost().getHostName()` (unchanged JDK API)
- Produces: `CompactionLockManager.buildInstanceId(): String` — visibility changes from `private static` to package-private `static` (testable), return format changes from `hostname:pid` to `hostname`

- [ ] **Step 1: Write failing test**

Edit `joxette-service/src/test/java/com/joxette/compaction/CompactionDistributedLockTest.java` — add two `@Test` methods after `listActiveLocks_secondsRemainingIsNegativeForExpiredLock` (after line 224, before the `// Helpers` section):

```java
    // -------------------------------------------------------------------------
    // buildInstanceId — hostname-only scheme (survives PID change across restart)
    // -------------------------------------------------------------------------

    @Test
    void buildInstanceId_isHostnameOnly_stableAcrossCalls() {
        String id1 = CompactionLockManager.buildInstanceId();
        String id2 = CompactionLockManager.buildInstanceId();

        // Same host, called twice in the same process — must be identical, and
        // must not embed a PID (a real restart changes ProcessHandle.current().pid(),
        // so encoding it would defeat same-host crash recovery).
        assertThat(id1).isEqualTo(id2);
        assertThat(id1).doesNotContain(":");
    }

    @Test
    void releaseOwnLocks_reclaimsSameHostLockImmediately_notViaTtlExpiry() throws Exception {
        // Simulate a crash: the "before" process holds the lock and never releases it.
        String host = "worker-node-9";
        CompactionLockManager beforeCrash = new CompactionLockManager(conn, TTL_MINUTES, host);
        assertThat(beforeCrash.tryAcquire(LOCK_TARGET)).isTrue();

        // "after" process restarts on the same host. Under the old hostname:pid
        // scheme this would be a different instance ID (new PID) and would never
        // match; under the new hostname-only scheme it is the same ID.
        CompactionLockManager afterRestart = new CompactionLockManager(conn, TTL_MINUTES, host);
        afterRestart.releaseOwnLocks();

        // Reclaimed immediately — the 120-minute TTL never had a chance to elapse
        // in this synchronous test, so this proves reclaim happened via the
        // instance-ID match at startup, not via TTL expiry.
        assertThat(lockB.tryAcquire(LOCK_TARGET)).isTrue();
        lockB.release(LOCK_TARGET);
    }
```

- [ ] **Step 2: Run it and confirm the exact failure**

```bash
mvn -pl joxette-service -am test -Dtest=CompactionDistributedLockTest
```

Expected failure: **compile error** — `buildInstanceId()` is `private` in `CompactionLockManager`, inaccessible from the test class:

```
buildInstanceId() has private access in com.joxette.compaction.CompactionLockManager
```

- [ ] **Step 3: Write minimal real implementation code**

Edit `joxette-service/src/main/java/com/joxette/compaction/CompactionLockManager.java` — replace the "Instance ID" section of the class javadoc (lines 40–43):

```java
 * <h2>Instance ID</h2>
 * <p>Derived once at construction as this process's hostname alone (no PID — see
 * {@link #buildInstanceId()} for why). A stale lock row bearing the current
 * hostname is therefore safe to delete on restart: it can only have been left by
 * a previous incarnation of this process on this same host.
```

Replace `buildInstanceId()` (lines 270–278):

```java
    /**
     * Builds this process's compaction-lock instance identity.
     *
     * <h2>Why hostname only (not hostname:pid)</h2>
     * <p>The previous scheme ({@code hostname:pid}) meant a crashed-and-restarted
     * process could never match its own prior lock rows in {@link #releaseOwnLocks()},
     * because the new process has a different PID — the stale lock then had to wait
     * out the full {@code lock-ttl-minutes} (default 120) before being reclaimed.
     *
     * <p>Using the hostname alone fixes the common case: a container that crashes
     * and is restarted <em>in place</em> by kubelet (the same Kubernetes Pod, hence
     * the same {@code HOSTNAME}) gets a new PID but the same instance identity, so
     * its stale locks are reclaimed immediately at startup instead of waiting on TTL.
     *
     * <h2>Residual gap — full Pod replacement</h2>
     * <p>This does <em>not</em> cover a Pod being entirely rescheduled (node
     * failure, rolling deploy): a {@code Deployment}-managed Pod gets a brand-new
     * random hostname suffix on replacement, so the new process's identity will not
     * match the old one either way. That case still relies on {@code lock-ttl-minutes}
     * TTL expiry as the backstop, exactly as before this fix — see
     * docs/clustering-deployment.md §6 (the compaction tier is a {@code Deployment},
     * not a {@code StatefulSet}, so Pod names are not stable across replacement).
     */
    static String buildInstanceId() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "localhost";
        }
    }
```

Update `getInstanceId()` javadoc (lines 253–254):

```java
    /** The instance identifier used in lock rows (this process's hostname). */
    public String getInstanceId() { return instanceId; }
```

Edit `joxette-service/src/main/java/com/joxette/compaction/CompactionLockInfo.java` — update the example (lines 22–24):

```java
        @Schema(description = "Instance that holds the lock (hostname)",
                example = "worker-node-1")
        String instanceId,
```

- [ ] **Step 4: Run and confirm pass**

```bash
mvn -pl joxette-service -am test -Dtest=CompactionDistributedLockTest,CompactionLockRaceTest,CompactionServiceTest
```

Confirm `buildInstanceId_isHostnameOnly_stableAcrossCalls` and `releaseOwnLocks_reclaimsSameHostLockImmediately_notViaTtlExpiry` pass, and no other lock test regressed (they all construct `CompactionLockManager` via the explicit-instanceId test constructor, unaffected by `buildInstanceId()`'s change).

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/compaction/CompactionLockManager.java \
        joxette-service/src/main/java/com/joxette/compaction/CompactionLockInfo.java \
        joxette-service/src/test/java/com/joxette/compaction/CompactionDistributedLockTest.java
git commit -m "fix(compaction): use hostname-only instance ID so crash-restart locks self-heal

buildInstanceId() previously returned hostname:pid, so a process that
crashed and restarted (same host, new PID) could never match its own
stale compaction_locks rows in releaseOwnLocks() — recovery depended
entirely on the 120-minute TTL. Hostname alone is stable across an
in-place container restart on the same Pod, so releaseOwnLocks() now
reclaims those locks immediately at startup. Full Pod replacement
(new hostname) still relies on TTL expiry, documented as a residual gap."
```

---

### Task 3: Snapshot-restore verification and recorder coordination

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/api/error/ErrorTypes.java`
- Modify: `joxette-service/src/main/java/com/joxette/api/error/ErrorCodes.java`
- Create: `joxette-service/src/main/java/com/joxette/api/error/SnapshotVerificationException.java`
- Modify: `joxette-service/src/main/java/com/joxette/db/SchemaManager.java` (add `migrateSnapshots` call at line 471; new method after line 737)
- Modify: `joxette-service/src/test/java/com/joxette/support/DuckDBTestSupport.java` (`snapshots` CREATE TABLE, lines 158–163)
- Modify: `joxette-service/src/main/java/com/joxette/replay/CassetteLifecycleService.java` (constructor lines 63–73; `createSnapshot` lines 267–301; `restoreSnapshot` lines 326–343; `exportLakeTables` lines 388–415)
- Modify: `joxette-service/src/main/java/com/joxette/replay/CassetteController.java` (`@ApiResponses` on `restoreSnapshot`, lines 1746–1751)
- Modify: `joxette-service/src/test/java/com/joxette/api/error/GlobalExceptionHandlerTest.java` (add row to `joxetteExceptions()`)
- Test (create): `joxette-service/src/test/java/com/joxette/it/RestoreSnapshotVerificationIT.java`

**Interfaces:**
- Consumes: `RecordingCoordinator.activeTopics(): Set<String>`, `RecordingCoordinator.stopAll(): void`, `RecordingCoordinator.restartTopic(String): boolean` (all pre-existing, unmodified); `ObjectMapper` (Spring Boot's default bean)
- Produces: `CassetteLifecycleService(Connection, JoxetteProperties, ConfigRepository, Optional<S3Client>, RecordingCoordinator, ObjectMapper)` (new constructor signature); `SnapshotVerificationException.rowCountMismatch(String name, Map<String,Long> expected, Map<String,Long> actual): SnapshotVerificationException`; `snapshots.row_counts JSON` column

- [ ] **Step 1: Write failing test**

Create `joxette-service/src/test/java/com/joxette/it/RestoreSnapshotVerificationIT.java`:

```java
package com.joxette.it;

import com.joxette.recording.RecordingCoordinator;
import com.joxette.support.DuckDBTestSupport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for the snapshot-restore safety net:
 * {@code CassetteLifecycleService.restoreSnapshot()} must (1) pause active
 * recorders, (2) verify restored row counts against the snapshot's stored
 * metadata, (3) throw a typed exception on mismatch, and (4) resume recorders
 * in a {@code finally} regardless of verification outcome.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class RestoreSnapshotVerificationIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("joxette.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @LocalServerPort private int port;
    @Autowired private Connection duckDB;
    @Autowired private RecordingCoordinator recordingCoordinator;

    private final RestTemplate restTemplate = new RestTemplate();

    static final String TEST_TOPIC = "verify-restore-topic";
    static final String NORMALIZED_TABLE = "general_verify_restore_topic";
    static final String SNAPSHOT_NAME = "it-verify-restore-snap";

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @BeforeEach
    void setUp() throws Exception {
        createKafkaTopic(TEST_TOPIC, 1);
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM snapshots");
        }
        Path snapshotDir = Path.of("snapshots", SNAPSHOT_NAME);
        if (Files.exists(snapshotDir)) {
            deleteDirectory(snapshotDir);
        }
    }

    @Test
    void restoreSnapshot_truncatedBackingFile_throwsTypedExceptionAndStillResumesRecorders() throws Exception {
        // Step 1: register recording and produce 3 messages so the general
        // cassette table (and the recorder) have real content.
        restTemplate.postForEntity(url("/topics"),
                Map.of("topic", TEST_TOPIC, "mode", "general", "startFrom", "earliest"), Object.class);

        try (KafkaProducer<String, byte[]> producer = newProducer()) {
            for (int i = 0; i < 3; i++) {
                producer.send(new ProducerRecord<>(TEST_TOPIC, "key-" + i,
                        ("value-" + i).getBytes())).get(5, TimeUnit.SECONDS);
            }
        }

        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100)).untilAsserted(() ->
                assertThat(DuckDBTestSupport.countRows(duckDB, "lake.main." + NORMALIZED_TABLE)).isEqualTo(3));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(recordingCoordinator.activeTopics()).contains(TEST_TOPIC));

        // Step 2: snapshot — records row_counts = {general_verify_restore_topic: 3}.
        var createResp = restTemplate.postForEntity(
                url("/cassettes/snapshots"), Map.of("name", SNAPSHOT_NAME), Object.class);
        assertThat(createResp.getStatusCode().value()).isEqualTo(201);

        // Step 3: deliberately truncate the snapshot's backing Parquet file to
        // zero rows — the declared row_counts metadata (3) no longer matches
        // what IMPORT DATABASE will actually restore (0).
        Path parquet = Path.of("snapshots", SNAPSHOT_NAME, "lake", NORMALIZED_TABLE + ".parquet");
        assertThat(Files.exists(parquet)).as("snapshot Parquet export must exist").isTrue();
        try (Statement st = duckDB.createStatement()) {
            st.execute("COPY (SELECT * FROM lake.main." + NORMALIZED_TABLE + " LIMIT 0) TO '"
                    + parquet + "' (FORMAT PARQUET)");
        }

        // Step 4: restore must fail with a typed 409, not silently succeed with data loss.
        assertThatThrownBy(() -> restTemplate.postForEntity(
                url("/cassettes/snapshots/" + SNAPSHOT_NAME + "/restore"), null, Void.class))
                .isInstanceOf(HttpClientErrorException.class)
                .satisfies(ex -> {
                    HttpClientErrorException hce = (HttpClientErrorException) ex;
                    assertThat(hce.getStatusCode().value()).isEqualTo(409);
                    assertThat(hce.getResponseBodyAsString()).contains("ERR_SNAPSHOT_VERIFICATION_FAILED");
                });

        // Step 5: even though restore failed, the recorder paused for the restore
        // attempt must have been resumed — never left stopped indefinitely.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(recordingCoordinator.activeTopics()).contains(TEST_TOPIC));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private KafkaProducer<String, byte[]> newProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private void createKafkaTopic(String topic, int partitions) {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }

    private static void deleteDirectory(Path dir) throws IOException {
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }
}
```

- [ ] **Step 2: Run it and confirm the exact failure**

```bash
mvn -pl joxette-service -am test -Dtest=RestoreSnapshotVerificationIT
```

Expected failure: `assertThatThrownBy(...)` fails because the restore call **succeeds** (`200 OK`) instead of throwing — today's `restoreSnapshot()` performs no verification at all:

```
Expecting code to raise a throwable.
```

- [ ] **Step 3: Write minimal real implementation code**

Edit `joxette-service/src/main/java/com/joxette/api/error/ErrorTypes.java` — add a constant:

```java
    public static final URI FORBIDDEN            = URI.create("https://joxette.dev/problems/forbidden");
    public static final URI SNAPSHOT_VERIFICATION_FAILED =
            URI.create("https://joxette.dev/problems/snapshot-verification-failed");
```

Edit `joxette-service/src/main/java/com/joxette/api/error/ErrorCodes.java` — add a constant:

```java
    public static final String FORBIDDEN            = "ERR_FORBIDDEN";
    public static final String SNAPSHOT_VERIFICATION_FAILED = "ERR_SNAPSHOT_VERIFICATION_FAILED";
```

Create `joxette-service/src/main/java/com/joxette/api/error/SnapshotVerificationException.java`:

```java
package com.joxette.api.error;

import org.springframework.http.HttpStatus;

import java.util.Map;

/**
 * Thrown when {@code CassetteLifecycleService.restoreSnapshot()} completes
 * {@code IMPORT DATABASE} / lake-table reload but the restored row counts do
 * not match the counts recorded in {@code snapshots.row_counts} at snapshot
 * creation time — evidence that the snapshot's backing Parquet file(s) were
 * corrupted, truncated, or otherwise modified after the snapshot was taken.
 */
public class SnapshotVerificationException extends JoxetteException {

    public SnapshotVerificationException(String detail) {
        super(HttpStatus.CONFLICT, ErrorTypes.SNAPSHOT_VERIFICATION_FAILED,
                "Snapshot Verification Failed", detail, ErrorCodes.SNAPSHOT_VERIFICATION_FAILED);
    }

    public static SnapshotVerificationException rowCountMismatch(
            String snapshotName, Map<String, Long> expected, Map<String, Long> actual) {
        StringBuilder detail = new StringBuilder(
                "Snapshot '" + snapshotName + "' restored but row counts do not match stored metadata: ");
        expected.forEach((table, expectedCount) -> {
            if (actual.containsKey(table)) {
                detail.append(table).append(" expected=").append(expectedCount)
                      .append(" actual=").append(actual.get(table)).append("; ");
            }
        });
        return new SnapshotVerificationException(detail.toString());
    }
}
```

Edit `joxette-service/src/main/java/com/joxette/db/SchemaManager.java` — add the migration call after `migrateJoxetteInstances(conn);` (line 471):

```java
        migrateCompactionHistory(conn);
        migrateKnownEntities(conn);
        migrateJoxetteInstances(conn);
        migrateSnapshots(conn);
    }
```

Add the migration method after `migrateJoxetteInstances` (after line 737):

```java
    /**
     * Idempotent migration: adds {@code row_counts JSON} to {@code snapshots},
     * used by {@code CassetteLifecycleService} to verify restored data against
     * the counts recorded at snapshot-creation time.
     */
    private void migrateSnapshots(Connection conn) {
        try (Statement st = conn.createStatement()) {
            st.execute("ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS row_counts JSON");
            log.debug("snapshots migration applied: row_counts");
        } catch (SQLException e) {
            log.warn("snapshots migration failed (row_counts): {}", e.getMessage());
            try { conn.rollback(); } catch (SQLException re) {
                log.debug("rollback after snapshots migration: {}", re.getMessage());
            }
        }
    }
```

Edit `joxette-service/src/test/java/com/joxette/support/DuckDBTestSupport.java` — add the column to the test schema mirror (lines 158–163):

```java
            st.execute("""
                    CREATE TABLE IF NOT EXISTS snapshots (
                        name        VARCHAR     NOT NULL PRIMARY KEY,
                        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                        size_bytes  BIGINT,
                        row_counts  JSON
                    )""");
```

Edit `joxette-service/src/main/java/com/joxette/replay/CassetteLifecycleService.java` — add imports, constructor params, and rewrite `createSnapshot`/`restoreSnapshot`/`exportLakeTables`:

```java
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.recording.RecordingCoordinator;
```

```java
    private final Connection duckDB;
    private final Path snapshotsBase;
    private final String objectStoragePath;
    private final ConfigRepository configRepo;
    private final JoxetteProperties properties;
    private final S3Client s3Client;
    private final RecordingCoordinator recordingCoordinator;
    private final ObjectMapper objectMapper;

    public CassetteLifecycleService(Connection duckDB, JoxetteProperties properties,
                                    ConfigRepository configRepo, Optional<S3Client> s3Client,
                                    RecordingCoordinator recordingCoordinator, ObjectMapper objectMapper) {
        this.duckDB               = duckDB;
        this.configRepo           = configRepo;
        this.properties           = properties;
        this.s3Client             = s3Client.orElse(null);
        this.objectStoragePath    = properties.getCatalog().getObjectStoragePath();
        this.recordingCoordinator = recordingCoordinator;
        this.objectMapper         = objectMapper;
        Path catalogPath = Path.of(properties.getCatalog().getPath());
        Path parent = catalogPath.getParent();
        this.snapshotsBase = (parent != null ? parent : Path.of(".")).resolve("snapshots");
    }
```

Replace `createSnapshot` (lines 267–301):

```java
    public SnapshotInfo createSnapshot(String name) throws SQLException {
        validateSnapshotName(name);
        Path snapshotDir = snapshotsBase.resolve(name);
        if (Files.exists(snapshotDir)) {
            throw com.joxette.api.error.ConflictException.snapshotAlreadyExists(name);
        }
        try {
            Files.createDirectories(snapshotsBase);
        } catch (IOException e) {
            throw com.joxette.api.error.UpstreamUnavailableException.objectStore(
                    "Cannot create snapshots directory: " + snapshotsBase, e);
        }
        log.info("Creating snapshot '{}' at {}", name, snapshotDir);
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement()) {
                st.execute("EXPORT DATABASE '" + snapshotDir + "'");
            }
            Map<String, Long> rowCounts = exportLakeTables(snapshotDir);
            long sizeBytes = directorySize(snapshotDir);
            String rowCountsJson = serializeRowCounts(rowCounts);
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO snapshots (name, created_at, size_bytes, row_counts)
                    VALUES (?, now(), ?, ?)
                    ON CONFLICT (name) DO UPDATE SET created_at = now(), size_bytes = excluded.size_bytes,
                        row_counts = excluded.row_counts
                    """)) {
                ps.setString(1, name);
                ps.setLong(2, sizeBytes);
                ps.setString(3, rowCountsJson);
                ps.executeUpdate();
            }
            log.info("Snapshot '{}' created: {} bytes, {} lake table(s)", name, sizeBytes, rowCounts.size());
            return new SnapshotInfo(name, Instant.now(), sizeBytes);
        }
    }

    private String serializeRowCounts(Map<String, Long> rowCounts) {
        try {
            return objectMapper.writeValueAsString(rowCounts);
        } catch (JsonProcessingException e) {
            log.warn("Could not serialise snapshot row counts ({}); restore verification will be skipped", e.getMessage());
            return null;
        }
    }
```

Replace `restoreSnapshot` (lines 326–343):

```java
    public void restoreSnapshot(String name) throws SQLException {
        validateSnapshotName(name);
        Path snapshotDir = snapshotsBase.resolve(name);
        if (!Files.exists(snapshotDir)) {
            throw com.joxette.api.error.ResourceNotFoundException.snapshot(name);
        }
        Map<String, Long> expectedRowCounts = loadStoredRowCounts(name);
        log.warn("Restoring snapshot '{}' from {} — all existing tables will be replaced", name, snapshotDir);

        java.util.Set<String> pausedTopics = recordingCoordinator.activeTopics();
        log.info("Restore '{}': pausing {} active recorder(s): {}", name, pausedTopics.size(), pausedTopics);
        recordingCoordinator.stopAll();
        try {
            patchSchemaSqlForRestore(snapshotDir);
            synchronized (duckDB) {
                try (Statement st = duckDB.createStatement()) {
                    st.execute("IMPORT DATABASE '" + snapshotDir + "'");
                }
                restoreLakeTables(snapshotDir);
                verifyRestoredRowCounts(name, expectedRowCounts);
            }
            log.info("Snapshot '{}' restored", name);
        } finally {
            log.info("Restore '{}': resuming {} recorder(s)", name, pausedTopics.size());
            for (String topic : pausedTopics) {
                recordingCoordinator.restartTopic(topic);
            }
        }
    }

    private Map<String, Long> loadStoredRowCounts(String name) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT row_counts FROM snapshots WHERE name = ?")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return Map.of();
                    String json = rs.getString("row_counts");
                    if (json == null || json.isBlank()) return Map.of();
                    try {
                        return objectMapper.readValue(json, new TypeReference<Map<String, Long>>() {});
                    } catch (JsonProcessingException e) {
                        log.warn("Restore '{}': could not parse stored row_counts ({}); skipping verification",
                                name, e.getMessage());
                        return Map.of();
                    }
                }
            }
        }
    }

    /** Must be called inside {@code synchronized(duckDB)}, after {@link #restoreLakeTables(Path)}. */
    private void verifyRestoredRowCounts(String name, Map<String, Long> expected) throws SQLException {
        if (expected.isEmpty()) {
            log.debug("Restore '{}': no stored row_counts metadata to verify against (older snapshot?)", name);
            return;
        }
        Map<String, Long> actual = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : expected.entrySet()) {
            String table = entry.getKey();
            long count;
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM lake.main." + table)) {
                count = rs.next() ? rs.getLong(1) : -1L;
            } catch (SQLException e) {
                log.warn("Restore '{}': could not verify table {} ({})", name, table, e.getMessage());
                count = -1L;
            }
            actual.put(table, count);
        }
        boolean mismatch = expected.entrySet().stream()
                .anyMatch(e -> !e.getValue().equals(actual.get(e.getKey())));
        if (mismatch) {
            throw com.joxette.api.error.SnapshotVerificationException.rowCountMismatch(name, expected, actual);
        }
        log.info("Restore '{}': verified row counts for {} lake table(s)", name, expected.size());
    }
```

Replace `exportLakeTables` (lines 388–415) to return the row-count map it now already computes per table:

```java
    private Map<String, Long> exportLakeTables(Path snapshotDir) throws SQLException {
        Path lakeDir = snapshotDir.resolve("lake");
        try {
            Files.createDirectories(lakeDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create lake snapshot dir: " + lakeDir, e);
        }
        List<String> tableNames = new ArrayList<>();
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT table_name FROM duckdb_tables()" +
                     " WHERE database_name = 'lake' AND schema_name = 'main'" +
                     " ORDER BY table_name")) {
            while (rs.next()) {
                tableNames.add(rs.getString("table_name"));
            }
        }
        Map<String, Long> rowCounts = new LinkedHashMap<>();
        for (String tableName : tableNames) {
            Path parquet = lakeDir.resolve(tableName + ".parquet");
            try (Statement st = duckDB.createStatement()) {
                st.execute("COPY lake.main." + tableName +
                           " TO '" + parquet + "' (FORMAT PARQUET)");
            }
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM lake.main." + tableName)) {
                rowCounts.put(tableName, rs.next() ? rs.getLong(1) : 0L);
            }
            log.debug("Snapshot: exported lake table {} → {}", tableName, parquet);
        }
        log.info("Snapshot: exported {} lake table(s) to {}", tableNames.size(), lakeDir);
        return rowCounts;
    }
```

Edit `joxette-service/src/main/java/com/joxette/replay/CassetteController.java` — add a 409 response entry (lines 1746–1751):

```java
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Snapshot restored successfully"),
        @ApiResponse(responseCode = "404", description = "Snapshot not found"),
        @ApiResponse(responseCode = "409", description = "Restored row counts did not match the snapshot's stored metadata " +
            "(ERR_SNAPSHOT_VERIFICATION_FAILED) — the backing Parquet file(s) may be corrupted or truncated",
            content = @Content(mediaType = "application/problem+json", schema = @Schema(type = "object"))),
        @ApiResponse(responseCode = "500", description = "Database error",
            content = @Content(schema = @Schema(type = "string")))
    })
```

Edit `joxette-service/src/test/java/com/joxette/api/error/GlobalExceptionHandlerTest.java` — add a row to `joxetteExceptions()`:

```java
                Arguments.of(new InvalidCursorException("bad cursor"),
                        HttpStatus.BAD_REQUEST, ErrorTypes.INVALID_CURSOR, ErrorCodes.INVALID_CURSOR),
                Arguments.of(SnapshotVerificationException.rowCountMismatch(
                                "snap1", java.util.Map.of("general_orders", 5L), java.util.Map.of("general_orders", 2L)),
                        HttpStatus.CONFLICT, ErrorTypes.SNAPSHOT_VERIFICATION_FAILED, ErrorCodes.SNAPSHOT_VERIFICATION_FAILED)
```

- [ ] **Step 4: Run and confirm pass**

```bash
mvn -pl joxette-service -am test -Dtest=RestoreSnapshotVerificationIT,RestoreSnapshotIT,SnapshotTruncateRestoreIT,GlobalExceptionHandlerTest
```

Confirm `RestoreSnapshotVerificationIT` now passes (409 + `ERR_SNAPSHOT_VERIFICATION_FAILED`, recorder resumed), and the two pre-existing snapshot ITs (which restore an uncorrupted snapshot) still pass — proving verification does not false-positive on a healthy restore.

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/api/error/ErrorTypes.java \
        joxette-service/src/main/java/com/joxette/api/error/ErrorCodes.java \
        joxette-service/src/main/java/com/joxette/api/error/SnapshotVerificationException.java \
        joxette-service/src/main/java/com/joxette/db/SchemaManager.java \
        joxette-service/src/test/java/com/joxette/support/DuckDBTestSupport.java \
        joxette-service/src/main/java/com/joxette/replay/CassetteLifecycleService.java \
        joxette-service/src/main/java/com/joxette/replay/CassetteController.java \
        joxette-service/src/test/java/com/joxette/api/error/GlobalExceptionHandlerTest.java \
        joxette-service/src/test/java/com/joxette/it/RestoreSnapshotVerificationIT.java
git commit -m "fix(replay): verify row counts and pause/resume recorders on snapshot restore

restoreSnapshot() ran IMPORT DATABASE and returned with no verification
that the restored data matched the snapshot, and its docstring's warning
to stop recorders first was never enforced. createSnapshot() now records
per-table row counts in snapshots.row_counts; restoreSnapshot() pauses all
active recorders via RecordingCoordinator, restores, verifies restored
counts against that metadata (throwing SnapshotVerificationException on
mismatch), and resumes recorders in a finally block regardless of outcome."
```

---

### Task 4: Retention should reclaim tombstoned files

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java` (`Retention` class, lines 333–343)
- Modify: `joxette-service/src/main/java/com/joxette/compaction/RetentionService.java` (imports lines 1–22; `enforceTopicRetention` lines 195–215; `enforceEntityRetention` lines 233–259; new methods after line 280)
- Test (create): `joxette-service/src/test/java/com/joxette/compaction/RetentionServiceRewriteTest.java`

**Interfaces:**
- Consumes: `DuckDbErrors.isTransient(Throwable): boolean` (pre-existing, unmodified)
- Produces: `JoxetteProperties.Retention.getRewriteDeleteThreshold(): double` / `setRewriteDeleteThreshold(double): void` (new); `RetentionService` issues `CALL ducklake_rewrite_data_files('lake', '<table>', delete_threshold => <n>)` after any bulk delete that removes ≥1 row

- [ ] **Step 1: Write failing test**

Create `joxette-service/src/test/java/com/joxette/compaction/RetentionServiceRewriteTest.java`:

```java
package com.joxette.compaction;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.support.DuckDBTestSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves RetentionService reclaims tombstoned Parquet files via
 * ducklake_rewrite_data_files after a bulk delete actually removes rows,
 * and does not attempt it when nothing was deleted.
 */
class RetentionServiceRewriteTest {

    private static final JoxetteMetrics TEST_METRICS = new JoxetteMetrics(new SimpleMeterRegistry());
    private static final String ENTITY_TYPE = "order";
    private static final String TOPIC = "orders.events";

    private Connection duckDB;
    private RetentionService service;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createEntityTable(duckDB, ENTITY_TYPE);
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC);

        try (PreparedStatement ps = duckDB.prepareStatement(
                "INSERT INTO entity_type_configs (entity_type, bucket_count, retention_days) VALUES (?, ?, ?)")) {
            ps.setString(1, ENTITY_TYPE);
            ps.setInt(2, 64);
            ps.setInt(3, 0); // immediate eligibility
            ps.executeUpdate();
        }
        try (PreparedStatement ps = duckDB.prepareStatement(
                "INSERT INTO topic_configs (topic, mode, retention_days) VALUES (?, 'general', ?)")) {
            ps.setString(1, TOPIC);
            ps.setInt(2, 0);
            ps.executeUpdate();
        }

        JoxetteProperties props = new JoxetteProperties();
        ConfigRepository configRepo = new ConfigRepository(duckDB, props);
        service = new RetentionService(duckDB, configRepo, props, TEST_METRICS);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    @Test
    void executeRun_afterDeletingRows_attemptsRewriteDataFiles() throws Exception {
        insertOldEntityRows(5);

        ListAppender<ILoggingEvent> appender = attachAppender();
        try {
            RetentionRun run = service.beginRun(TriggerSource.MANUAL);
            service.executeRun(run.id());

            assertThat(service.getRunById(run.id()).status()).isEqualTo(RunStatus.COMPLETED);
            assertThat(rewriteAttempted(appender))
                    .as("A bulk delete of entity rows must trigger a ducklake_rewrite_data_files attempt")
                    .isTrue();
        } finally {
            detachAppender(appender);
        }
    }

    @Test
    void executeRun_noRowsDeleted_doesNotAttemptRewrite() throws Exception {
        // No rows inserted — nothing eligible for deletion.
        ListAppender<ILoggingEvent> appender = attachAppender();
        try {
            RetentionRun run = service.beginRun(TriggerSource.MANUAL);
            service.executeRun(run.id());

            assertThat(rewriteAttempted(appender))
                    .as("No rows deleted — ducklake_rewrite_data_files must not be attempted")
                    .isFalse();
        } finally {
            detachAppender(appender);
        }
    }

    private void insertOldEntityRows(int count) throws Exception {
        Instant ts = Instant.parse("2000-01-01T00:00:00Z"); // far in the past → always eligible
        for (int i = 0; i < count; i++) {
            DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE,
                    "ORD-" + i, i % 64, "order",
                    TOPIC, 0, i, ts, ts, "k" + i, ("v" + i).getBytes());
        }
    }

    private static boolean rewriteAttempted(ListAppender<ILoggingEvent> appender) {
        return appender.list.stream().anyMatch(e ->
                e.getFormattedMessage().contains("ducklake_rewrite_data_files")
                || e.getFormattedMessage().contains("Rewriting delete-heavy files"));
    }

    private static ListAppender<ILoggingEvent> attachAppender() {
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(RetentionService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        return appender;
    }

    private static void detachAppender(ListAppender<ILoggingEvent> appender) {
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(RetentionService.class);
        logger.detachAppender(appender);
    }
}
```

- [ ] **Step 2: Run it and confirm the exact failure**

```bash
mvn -pl joxette-service -am test -Dtest=RetentionServiceRewriteTest
```

Expected failure: `executeRun_afterDeletingRows_attemptsRewriteDataFiles` fails —

```
A bulk delete of entity rows must trigger a ducklake_rewrite_data_files attempt
Expecting value to be true but was false
```

(`executeRun_noRowsDeleted_doesNotAttemptRewrite` passes trivially since `RetentionService` never issues this call today.)

- [ ] **Step 3: Write minimal real implementation code**

Edit `joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java` — extend `Retention` (lines 333–343):

```java
    public static class Retention {
        /**
         * Cron expression for the scheduled retention enforcement run.
         * Uses Spring 6-field format: {@code <sec> <min> <hour> <dom> <month> <dow>}.
         * Default: daily at 01:00:00 (runs two hours before compaction at 03:00).
         */
        private String schedule = "0 0 1 * * *";
        /**
         * Delete-marker ratio threshold passed to {@code ducklake_rewrite_data_files}
         * as {@code delete_threshold} when reclaiming files after a bulk retention
         * delete. A table's Parquet files are rewritten (tombstoned rows physically
         * dropped) once this fraction of rows in a file are marked deleted.
         * Default {@code 0.1} (10%).
         */
        private double rewriteDeleteThreshold = 0.1;

        public String getSchedule() { return schedule; }
        public void setSchedule(String schedule) { this.schedule = schedule; }

        public double getRewriteDeleteThreshold() { return rewriteDeleteThreshold; }
        public void setRewriteDeleteThreshold(double rewriteDeleteThreshold) {
            this.rewriteDeleteThreshold = rewriteDeleteThreshold;
        }
    }
```

Edit `joxette-service/src/main/java/com/joxette/compaction/RetentionService.java` — add the import after line 4:

```java
import com.joxette.db.DuckDbErrors;
import com.joxette.db.SchemaManager;
```

Modify `enforceTopicRetention` (lines 195–215):

```java
    private long enforceTopicRetention() throws SQLException {
        List<TopicConfig> topics = configRepo.listTopics();
        long total = 0;
        for (TopicConfig tc : topics) {
            if (tc.retentionDays() == null) continue;
            if (tc.mode() == TopicMode.ENTITY_ONLY) continue;
            try {
                long deleted = deleteFromGeneralCassette(tc.topic(), tc.retentionDays());
                if (deleted > 0) {
                    log.debug("Retention: deleted {} rows from general cassette for topic '{}' (retention={} days)",
                            deleted, tc.topic(), tc.retentionDays());
                    rewriteGeneralCassette(tc.topic());
                }
                total += deleted;
            } catch (Exception e) {
                log.warn("Retention: skipping topic '{}' due to error: {}", tc.topic(), e.getMessage());
            }
        }
        return total;
    }
```

Modify `enforceEntityRetention` (lines 233–259):

```java
    private long[] enforceEntityRetention() throws SQLException {
        List<EntityTypeConfig> entities = configRepo.listEntityTypes();
        long entityRows = 0;
        long knownEntitiesRows = 0;
        for (EntityTypeConfig etc : entities) {
            if (etc.retentionDays() == null) continue;
            int days    = etc.retentionDays();
            String type = etc.entityType();
            SchemaManager.validateEntityType(type);
            try {
                long cassDeleted = deleteFromEntityCassette(type, days);
                long keDeleted   = deleteFromKnownEntities(type, days);
                if (cassDeleted > 0 || keDeleted > 0) {
                    log.debug("Retention: deleted {} entity rows, {} known_entities rows for type '{}' (retention={} days)",
                            cassDeleted, keDeleted, type, days);
                }
                if (cassDeleted > 0) {
                    rewriteEntityCassette(type);
                }
                entityRows        += cassDeleted;
                knownEntitiesRows += keDeleted;
            } catch (Exception e) {
                log.warn("Retention: skipping entity type '{}' due to error (will retry next schedule): {}",
                        type, e.getMessage());
            }
        }
        return new long[]{entityRows, knownEntitiesRows};
    }
```

Add new private methods after `deleteFromKnownEntities` (after line 280):

```java
    // =========================================================================
    // ducklake_rewrite_data_files — reclaim tombstoned files after a bulk delete
    // =========================================================================

    private void rewriteGeneralCassette(String topic) {
        rewriteDataFiles("general_" + SchemaManager.normalize(topic), "general cassette topic='" + topic + "'");
    }

    private void rewriteEntityCassette(String type) {
        rewriteDataFiles("entity_" + type, "entity type='" + type + "'");
    }

    /**
     * Rewrites Parquet files with a high delete-marker ratio for {@code tableName}
     * via {@code ducklake_rewrite_data_files} — DuckLake 1.0. Called after a bulk
     * retention delete so tombstoned rows are actually reclaimed from object
     * storage instead of only being logically deleted in the catalog. Idempotent
     * and non-fatal: errors are logged and do not fail the retention run,
     * mirroring CompactionService.doCompactEntityType's error-handling convention.
     */
    private void rewriteDataFiles(String tableName, String label) {
        double deleteThreshold = props.getRetention().getRewriteDeleteThreshold();
        String sql = "CALL ducklake_rewrite_data_files('lake', '" + tableName + "',"
                   + " delete_threshold => " + deleteThreshold + ")";
        log.debug("Rewriting delete-heavy files for {} ({})", label, tableName);
        try {
            synchronized (duckDB) {
                try (Statement st = duckDB.createStatement()) {
                    st.execute(sql);
                }
            }
            log.debug("Rewrite complete for {} ({})", label, tableName);
        } catch (SQLException e) {
            if (DuckDbErrors.isTransient(e)) {
                log.warn("ducklake_rewrite_data_files transient S3 failure for {} (will retry next run): {}",
                        label, e.getMessage());
            } else {
                log.warn("ducklake_rewrite_data_files failed for {}: {}", label, e.getMessage());
            }
        }
    }
```

- [ ] **Step 4: Run and confirm pass**

```bash
mvn -pl joxette-service -am test -Dtest=RetentionServiceRewriteTest
```

Confirm both tests pass: rewrite is attempted (logged) when rows were deleted, and not attempted when nothing was deleted.

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/config/JoxetteProperties.java \
        joxette-service/src/main/java/com/joxette/compaction/RetentionService.java \
        joxette-service/src/test/java/com/joxette/compaction/RetentionServiceRewriteTest.java
git commit -m "fix(retention): reclaim tombstoned Parquet files after bulk deletes

RetentionService deleted rows via plain DELETE but never called
ducklake_rewrite_data_files, so files with a high delete-marker ratio
were never reclaimed unless they also happened to hit
ducklake_merge_adjacent_files's size-based merge threshold. Both
enforceTopicRetention and enforceEntityRetention now call it per table
after a delete removes at least one row, using the same transient/
non-transient error classification convention as CompactionService."
```

---

### Task 5: Guard entity bucket-count changes against silent inconsistency

**Files:**
- Modify: `joxette-service/src/main/java/com/joxette/replay/KnownEntitiesRepository.java` (imports; new method after line 117)
- Modify: `joxette-service/src/main/java/com/joxette/api/error/ConflictException.java` (new factory method)
- Modify: `joxette-service/src/main/java/com/joxette/management/EntityController.java` (imports; constructor lines 59–64; `updateEntityType` lines 96–106)
- Modify: `joxette-service/src/test/java/com/joxette/api/error/EntityControllerProblemDetailTest.java` (mock field; constructor call line 51; new test)

**Interfaces:**
- Consumes: none new beyond `KnownEntitiesRepository` itself
- Produces: `KnownEntitiesRepository.countByType(String entityType): long throws SQLException` (new); `EntityController(ConfigRepository, SchemaManager, ConfigEventBus, KnownEntitiesRepository)` (new constructor signature); `ConflictException.bucketCountChangeRejected(String, int, int): ConflictException` (new)

- [ ] **Step 1: Write failing test**

Edit `joxette-service/src/test/java/com/joxette/api/error/EntityControllerProblemDetailTest.java` — add the mock field:

```java
    @Mock ConfigRepository config;
    @Mock SchemaManager schemaManager;
    @Mock ConfigEventBus eventBus;
    @Mock KnownEntitiesRepository knownEntities;
```

Update the constructor call in `setUp()` (line 51):

```java
        EntityController controller = new EntityController(config, schemaManager, eventBus, knownEntities);
```

Add imports and a new parameterized test after `createEntityType_duplicate_returnsConflictProblem`:

```java
import com.joxette.replay.KnownEntitiesRepository;
import org.junit.jupiter.params.provider.CsvSource;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
```

```java
    // =========================================================================
    // 409 / 200 — bucket-count change guarded by existing known_entities data
    // =========================================================================

    @ParameterizedTest(name = "knownEntityCount={0} -> status {1}")
    @CsvSource({
        "0, 200",
        "1, 409"
    })
    void updateEntityType_bucketCountChange_rejectedOnlyWhenEntitiesExist(
            long knownEntityCount, int expectedStatus) throws Exception {
        when(config.findEntityType("order")).thenReturn(Optional.of(new EntityTypeConfig("order", 256, null)));
        when(config.upsertEntityType("order", 512)).thenReturn(new EntityTypeConfig("order", 512, null));
        when(knownEntities.countByType("order")).thenReturn(knownEntityCount);

        String body = mapper.writeValueAsString(Map.of("buckets", 512));

        if (expectedStatus == 409) {
            mvc.perform(put("/entities/order").contentType(MediaType.APPLICATION_JSON).content(body))
               .andExpectAll(ProblemDetailAssertions.problemDetail(
                       409,
                       ErrorTypes.CONFLICT.toString(),
                       ErrorCodes.CONFLICT,
                       "/entities/order"));
        } else {
            mvc.perform(put("/entities/order").contentType(MediaType.APPLICATION_JSON).content(body))
               .andExpect(status().isOk())
               .andExpect(jsonPath("$.buckets").value(512));
        }
    }
```

- [ ] **Step 2: Run it and confirm the exact failure**

```bash
mvn -pl joxette-service -am test -Dtest=EntityControllerProblemDetailTest
```

Expected failure: **compile error** — `KnownEntitiesRepository.countByType(String)` does not exist and `EntityController` has no 4-arg constructor:

```
cannot find symbol
  symbol:   method countByType(java.lang.String)
  location: variable knownEntities of type com.joxette.replay.KnownEntitiesRepository
```

- [ ] **Step 3: Write minimal real implementation code**

Edit `joxette-service/src/main/java/com/joxette/replay/KnownEntitiesRepository.java` — add the import:

```java
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
```

Add the method after `consecutiveFailures()` (after line 117):

```java
    /**
     * Returns the number of known entities recorded for {@code entityType}.
     *
     * <p>Used by {@link com.joxette.management.EntityController} to block a
     * bucket-count change on an entity type that already has recorded data —
     * changing the modulus after data exists would silently desynchronize
     * existing rows' bucket assignments from new writes.
     */
    public long countByType(String entityType) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT COUNT(*) FROM known_entities WHERE entity_type = ?")) {
                ps.setString(1, entityType);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getLong(1) : 0L;
                }
            }
        }
    }
```

Edit `joxette-service/src/main/java/com/joxette/api/error/ConflictException.java` — add a factory method:

```java
    public static ConflictException scheduledReplayCannotCancel(String status) {
        return new ConflictException("Cannot cancel replay in status: " + status);
    }

    public static ConflictException bucketCountChangeRejected(String entityType, int currentBuckets, int requestedBuckets) {
        return new ConflictException(
                "Cannot change bucket count for entity type '" + entityType + "' from " + currentBuckets +
                " to " + requestedBuckets + ": entity type already has recorded data in known_entities. " +
                "Changing the bucket modulus after data exists would silently desynchronize existing rows' " +
                "bucket assignments from new writes and break bucket-pruned compaction/replay. " +
                "Use a documented rebalance procedure instead.");
    }
```

Edit `joxette-service/src/main/java/com/joxette/management/EntityController.java` — add the import:

```java
import com.joxette.replay.KnownEntitiesRepository;
```

Update the field, constructor (lines 59–64), and `updateEntityType` (lines 96–106):

```java
    private final ConfigRepository config;
    private final SchemaManager schemaManager;
    private final ConfigEventBus eventBus;
    private final KnownEntitiesRepository knownEntities;

    public EntityController(ConfigRepository config, SchemaManager schemaManager,
                            ConfigEventBus eventBus, KnownEntitiesRepository knownEntities) {
        this.config        = config;
        this.schemaManager = schemaManager;
        this.eventBus      = eventBus;
        this.knownEntities = knownEntities;
    }
```

```java
    @PutMapping(value = "/{type}",
                consumes = MediaType.APPLICATION_JSON_VALUE,
                produces = MediaType.APPLICATION_JSON_VALUE)
    public EntityTypeConfig updateEntityType(
            @PathVariable String type,
            @Valid @RequestBody UpdateEntityRequest body) throws SQLException {
        EntityTypeConfig existing = config.findEntityType(type)
                .orElseThrow(() -> ResourceNotFoundException.entityType(type));
        if (body.buckets() != existing.buckets() && knownEntities.countByType(type) > 0) {
            throw ConflictException.bucketCountChangeRejected(type, existing.buckets(), body.buckets());
        }
        EntityTypeConfig updated = config.upsertEntityType(type, body.buckets());
        publish(type, "updated");
        return updated;
    }
```

- [ ] **Step 4: Run and confirm pass**

```bash
mvn -pl joxette-service -am test -Dtest=EntityControllerProblemDetailTest
```

Confirm both parameterized cases pass: `knownEntityCount=0` → `200 OK` with `buckets=512`; `knownEntityCount=1` → `409 Conflict` with `errorCode=ERR_CONFLICT`.

- [ ] **Step 5: Commit**

```bash
git add joxette-service/src/main/java/com/joxette/replay/KnownEntitiesRepository.java \
        joxette-service/src/main/java/com/joxette/api/error/ConflictException.java \
        joxette-service/src/main/java/com/joxette/management/EntityController.java \
        joxette-service/src/test/java/com/joxette/api/error/EntityControllerProblemDetailTest.java
git commit -m "fix(entities): reject bucket-count changes once an entity type has recorded data

ConfigRepository.upsertEntityType() had no validation: PUT /entities/{type}
could change bucket_count at any time, immediately changing the bucket
modulus for new writes while old rows kept their stale bucket values
forever — defeating bucket-pruning and making scoped entity compaction
ambiguous across the boundary. updateEntityType() now checks
known_entities via the new KnownEntitiesRepository.countByType() and
rejects the change with a 409 once the entity type has recorded data,
directing operators to a documented rebalance procedure instead."
```

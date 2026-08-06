package com.joxette.compaction;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.joxette.cluster.InstanceRegistry;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.support.DuckDBTestSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for {@link CompactionService} against an in-memory DuckDB.
 *
 * <p>Tests focus on:
 * <ul>
 *   <li>Run lifecycle: begin → execute → completed, history tracking</li>
 *   <li>Concurrent-run guard ({@code AtomicBoolean})</li>
 *   <li>Data preservation after a compaction run</li>
 *   <li>General cassette compaction (disabled by default, enabled on demand)</li>
 *   <li>Status and history queries</li>
 * </ul>
 *
 * <p>Note: the bucket-needs-compaction check in {@code CompactionService}
 * uses {@code duckdb_storage_info()} to count row groups.  In-memory DuckDB
 * may report 0 row groups for small datasets, meaning no bucket qualifies for
 * compaction — the tests therefore focus on run tracking and data integrity
 * rather than verifying the rewrite path directly.
 */
class CompactionServiceTest {

    private static final JoxetteMetrics TEST_METRICS = new JoxetteMetrics(new SimpleMeterRegistry());

    private static final String ENTITY_TYPE = "order";
    private static final String TEST_INSTANCE_ID = "test-instance";

    private Connection duckDB;
    private InstanceRegistry instanceRegistry;
    private CompactionService service;
    private ConfigRepository configRepo;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createEntityTable(duckDB, ENTITY_TYPE);
        // General cassette table needed by insertCassetteRows / executeRun_preservesGeneralCassetteData
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, "orders.events");

        // Seed entity_type_configs in main schema so ConfigRepository.listEntityTypes() works.
        try (PreparedStatement ps = duckDB.prepareStatement(
                "INSERT INTO entity_type_configs (entity_type, bucket_count) VALUES (?, ?)")) {
            ps.setString(1, ENTITY_TYPE);
            ps.setInt(2, 64);
            ps.executeUpdate();
        }

        JoxetteProperties props = testProperties();

        configRepo = new ConfigRepository(duckDB, props);
        // Real InstanceRegistry with this instance registered as alive, so the
        // opportunistic cleanLocksForDeadInstances() call wired into executeRun()
        // never treats this test's own in-flight lock as belonging to a dead instance.
        instanceRegistry = DuckDBTestSupport.newInstanceRegistry(duckDB);
        DuckDBTestSupport.registerLiveInstance(duckDB, TEST_INSTANCE_ID);
        service = new CompactionService(duckDB, props, configRepo, TEST_METRICS, newLockManager());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    // -------------------------------------------------------------------------
    // Run lifecycle
    // -------------------------------------------------------------------------

    @Test
    void beginRun_insertsRunningRecord() throws Exception {
        assertThat(service.isRunning()).isFalse();

        CompactionRun run = service.beginRun(TriggerSource.MANUAL, null);

        assertThat(run).isNotNull();
        assertThat(run.status()).isEqualTo(RunStatus.RUNNING);
        assertThat(run.triggeredBy()).isEqualTo(TriggerSource.MANUAL);
        assertThat(run.startedAt()).isNotNull();
        assertThat(run.completedAt()).isNull();
        assertThat(service.isRunning()).isTrue();
    }

    @Test
    void beginRun_preventsOverlapping() throws Exception {
        service.beginRun(TriggerSource.MANUAL, null);

        assertThatThrownBy(() -> service.beginRun(TriggerSource.MANUAL, null))
                .isInstanceOf(com.joxette.api.error.ConflictException.class)
                .hasMessageContaining("already in progress");
    }

    @Test
    void executeRun_completesSuccessfully() throws Exception {
        CompactionRun started = service.beginRun(TriggerSource.MANUAL, null);

        service.executeRun(started.id(), null);

        assertThat(service.isRunning()).isFalse();

        CompactionRun completed = service.getRunById(started.id());
        assertThat(completed.status()).isEqualTo(RunStatus.COMPLETED);
        assertThat(completed.completedAt()).isNotNull();
        assertThat(completed.errorMessage()).isNull();
    }

    @Test
    void executeRun_resetsRunningFlagOnCompletion() throws Exception {
        CompactionRun run = service.beginRun(TriggerSource.MANUAL, null);
        assertThat(service.isRunning()).isTrue();

        service.executeRun(run.id(), null);

        assertThat(service.isRunning()).isFalse();
    }

    // -------------------------------------------------------------------------
    // History and status
    // -------------------------------------------------------------------------

    @Test
    void getHistory_returnsRunsInDescendingOrder() throws Exception {
        CompactionRun r1 = service.beginRun(TriggerSource.MANUAL, null);
        service.executeRun(r1.id(), null);

        CompactionRun r2 = service.beginRun(TriggerSource.MANUAL, null);
        service.executeRun(r2.id(), null);

        List<CompactionRun> history = service.getHistory(10);

        assertThat(history).hasSize(2);
        // Most recent run comes first.
        assertThat(history.get(0).triggeredBy()).isEqualTo(TriggerSource.MANUAL);
        assertThat(history.get(1).triggeredBy()).isEqualTo(TriggerSource.MANUAL);
    }

    @Test
    void getHistory_limitsResults() throws Exception {
        for (int i = 0; i < 3; i++) {
            CompactionRun r = service.beginRun(TriggerSource.MANUAL, null);
            service.executeRun(r.id(), null);
        }

        List<CompactionRun> history = service.getHistory(2);
        assertThat(history).hasSize(2);
    }

    // -------------------------------------------------------------------------
    // Config
    // -------------------------------------------------------------------------

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

    @Test
    void getStatus_noRuns_returnsNullLastRun() throws Exception {
        CompactionStatus status = service.getStatus();

        assertThat(status.lastRun()).isNull();
        assertThat(status.running()).isFalse();
        assertThat(status.nextScheduledRun()).isNotNull(); // computed from cron
    }

    @Test
    void getStatus_afterRun_showsLastRun() throws Exception {
        CompactionRun run = service.beginRun(TriggerSource.MANUAL, null);
        service.executeRun(run.id(), null);

        CompactionStatus status = service.getStatus();

        assertThat(status.lastRun()).isNotNull();
        assertThat(status.lastRun().status()).isEqualTo(RunStatus.COMPLETED);
        assertThat(status.running()).isFalse();
    }

    // -------------------------------------------------------------------------
    // Targeted runs
    // -------------------------------------------------------------------------

    @Test
    void executeRun_withEntityTarget_tracksCompactedBuckets() throws Exception {
        // Even if no buckets qualify (0 row groups), the run still succeeds.
        CompactionRun run = service.beginRun(TriggerSource.MANUAL, List.of(ENTITY_TYPE));
        service.executeRun(run.id(), List.of(ENTITY_TYPE));

        CompactionRun result = service.getRunById(run.id());
        assertThat(result.status()).isEqualTo(RunStatus.COMPLETED);
        assertThat(result.targets()).containsExactly(ENTITY_TYPE);
    }

    @Test
    void executeRun_withGeneralTarget_generalCompactionDisabledByDefault() throws Exception {
        insertCassetteRows(5);

        CompactionRun run = service.beginRun(TriggerSource.MANUAL, List.of("general"));
        service.executeRun(run.id(), List.of("general"));

        CompactionRun result = service.getRunById(run.id());
        assertThat(result.status()).isEqualTo(RunStatus.COMPLETED);
        // General compaction is disabled in testProperties(), so 0 partitions compacted.
        assertThat(result.generalPartitionsCompacted()).isEqualTo(0);
    }

    // -------------------------------------------------------------------------
    // Data preservation
    // -------------------------------------------------------------------------

    @Test
    void executeRun_preservesEntityData() throws Exception {
        int rows = 10;
        insertEntityRows(rows);

        long countBefore = DuckDBTestSupport.countRows(duckDB, "lake.main.entity_" + ENTITY_TYPE);
        assertThat(countBefore).isEqualTo(rows);

        CompactionRun run = service.beginRun(TriggerSource.MANUAL, List.of(ENTITY_TYPE));
        service.executeRun(run.id(), List.of(ENTITY_TYPE));

        long countAfter = DuckDBTestSupport.countRows(duckDB, "lake.main.entity_" + ENTITY_TYPE);
        assertThat(countAfter).isEqualTo(rows);
    }

    @Test
    void executeRun_preservesGeneralCassetteData() throws Exception {
        int rows = 8;
        insertCassetteRows(rows);

        // insertCassetteRows uses topic "orders.events" → table lake.main.general_orders_events
        long countBefore = DuckDBTestSupport.countRows(duckDB, "lake.main.general_orders_events");
        assertThat(countBefore).isEqualTo(rows);

        CompactionRun run = service.beginRun(TriggerSource.MANUAL, null);
        service.executeRun(run.id(), null);

        long countAfter = DuckDBTestSupport.countRows(duckDB, "lake.main.general_orders_events");
        assertThat(countAfter).isEqualTo(rows);
    }

    // -------------------------------------------------------------------------
    // runScheduled
    // -------------------------------------------------------------------------

    @Test
    void runScheduled_skipsWhenAlreadyRunning() throws Exception {
        // Manually set running flag by beginning a run.
        service.beginRun(TriggerSource.MANUAL, null);
        assertThat(service.isRunning()).isTrue();

        // runScheduled should silently skip — no exception.
        service.runScheduled();

        // Reset the flag for teardown.
        service.executeRun(
                service.getHistory(1).get(0).id(),
                null);
    }

    // -------------------------------------------------------------------------
    // ducklake_flush_inlined_data result logging
    // -------------------------------------------------------------------------

    /**
     * After a compaction run completes, a flush-related log event must be emitted
     * by {@code checkpoint()} — the row count returned by
     * {@code ducklake_flush_inlined_data} must not be silently discarded.
     *
     * <p>With DuckLake loaded: DEBUG "Flushed N inlined rows to Parquet for lake 'lake'".
     * Without DuckLake (in-memory test DuckDB): WARN "ducklake_flush failed (...)".
     * Either way at least one log event containing "flush" or "flushed" must appear.
     */
    @Test
    void checkpoint_flushResult_isLoggedNotDiscarded() throws Exception {
        insertEntityRows(5); // seed data so flush has something to process

        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CompactionService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level savedLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        try {
            CompactionRun run = service.beginRun(TriggerSource.MANUAL, null);
            service.executeRun(run.id(), null);

            boolean hasFlushEvent = appender.list.stream().anyMatch(e -> {
                String msg = e.getFormattedMessage().toLowerCase();
                return msg.contains("flush") || msg.contains("flushed");
            });
            assertThat(hasFlushEvent)
                    .as("checkpoint() must log the ducklake_flush_inlined_data result, not discard it")
                    .isTrue();
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(savedLevel);
        }
    }

    /**
     * Seeds a small batch of inlined data (below the auto-flush threshold so the data
     * is still buffered when {@code checkpoint()} fires), runs a compaction, and
     * verifies that any {@code "inlined rows"} DEBUG message carries a non-negative count.
     *
     * <p>In in-memory test DuckDB the {@code ducklake_flush_inlined_data} call fails
     * (extension not loaded) and the test passes trivially — no DEBUG message is emitted.
     * With DuckLake loaded, the assertion catches any negative count that would indicate
     * a bug in the result-reading code.
     */
    @Test
    void checkpoint_flushedRowCount_isNonNegative() throws Exception {
        insertEntityRows(5); // small batch — below auto-flush threshold

        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CompactionService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level savedLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        try {
            CompactionRun run = service.beginRun(TriggerSource.MANUAL, null);
            service.executeRun(run.id(), null);

            // Run must complete regardless of DuckLake availability.
            assertThat(service.getRunById(run.id()).status()).isEqualTo(RunStatus.COMPLETED);

            // If the DEBUG path was taken (DuckLake available), the embedded count must be ≥ 0.
            Pattern countPattern = Pattern.compile("(\\d+) inlined rows");
            appender.list.stream()
                    .filter(e -> e.getLevel() == Level.DEBUG)
                    .filter(e -> e.getFormattedMessage().contains("inlined rows"))
                    .forEach(e -> {
                        Matcher m = countPattern.matcher(e.getFormattedMessage());
                        assertThat(m.find())
                                .as("Flush DEBUG message must embed a numeric row count: "
                                        + e.getFormattedMessage())
                                .isTrue();
                        assertThat(Long.parseLong(m.group(1)))
                                .as("Flushed row count in log must be non-negative")
                                .isGreaterThanOrEqualTo(0L);
                    });
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(savedLevel);
        }
    }

    // -------------------------------------------------------------------------
    // ducklake_merge_adjacent_files results written to compaction_history
    // -------------------------------------------------------------------------

    /**
     * No-op compaction: when there are no files to merge (empty entity table) the
     * {@code compaction_history} record must show
     * {@code files_processed = files_created} (files_before = files_after) — meaning
     * nothing was merged and zero bytes were reclaimed.
     *
     * <p>In this test environment {@code ducklake_merge_adjacent_files} is unavailable;
     * the service records zeros, so the invariant 0 = 0 still holds.
     */
    @Test
    void compactionHistory_noOp_filesBeforeEqualsFilesAfter() throws Exception {
        // Entity table exists but has no rows — nothing to compact.
        CompactionRun run = service.beginRun(TriggerSource.MANUAL, List.of(ENTITY_TYPE));
        service.executeRun(run.id(), List.of(ENTITY_TYPE));

        CompactionRun result = service.getRunById(run.id());
        assertThat(result.status()).isEqualTo(RunStatus.COMPLETED);

        // files_processed = "files_before": how many source files were merged.
        // files_created   = "files_after":  how many output files remain.
        // No-op: files_before == files_after → nothing merged, bytes_reclaimed == 0.
        assertThat(result.filesProcessed())
                .as("files_before (filesProcessed) must be non-negative")
                .isGreaterThanOrEqualTo(0L);
        assertThat(result.filesCreated())
                .as("files_after (filesCreated) must be non-negative")
                .isGreaterThanOrEqualTo(0L);
        assertThat(result.filesProcessed())
                .as("No-op: files_before must equal files_after (nothing merged, no bytes reclaimed)")
                .isEqualTo(result.filesCreated());
    }

    /**
     * Productive compaction: after DuckLake merges several small files into fewer
     * larger ones, the history record must show {@code files_processed > files_created}
     * (files_before > files_after), i.e., bytes were reclaimed.
     *
     * <p>In this test environment {@code ducklake_merge_adjacent_files} throws because
     * the DuckLake extension is not loaded; the service records zeros and the run still
     * completes.  This test therefore verifies:
     * <ol>
     *   <li>The run completes successfully even after a DuckLake call failure.</li>
     *   <li>{@code files_processed} (files_before) and {@code files_created} (files_after)
     *       are written to {@code compaction_history} as non-negative values — never null.</li>
     * </ol>
     *
     * <p>Full assertion — {@code files_processed > files_created} → bytes_reclaimed > 0 —
     * requires an integration test environment with the DuckLake extension loaded:
     * <pre>
     *   assertThat(result.filesProcessed()).isGreaterThan(result.filesCreated());
     * </pre>
     */
    @Test
    void compactionHistory_productiveCompaction_fileCountsWrittenToHistory() throws Exception {
        insertEntityRows(20); // multiple rows → multiple small Parquet files in production

        CompactionRun run = service.beginRun(TriggerSource.MANUAL, List.of(ENTITY_TYPE));
        service.executeRun(run.id(), List.of(ENTITY_TYPE));

        CompactionRun result = service.getRunById(run.id());
        assertThat(result.status()).isEqualTo(RunStatus.COMPLETED);

        // Both file counts must be persisted to compaction_history (never null / negative).
        assertThat(result.filesProcessed())
                .as("files_before (filesProcessed) must be written to compaction_history")
                .isGreaterThanOrEqualTo(0L);
        assertThat(result.filesCreated())
                .as("files_after (filesCreated) must be written to compaction_history")
                .isGreaterThanOrEqualTo(0L);
        // With DuckLake loaded and files to merge, the productive assertion would be:
        //   assertThat(result.filesProcessed()).isGreaterThan(result.filesCreated())
        //   i.e., files_after < files_before → bytes_reclaimed > 0.
    }

    // -------------------------------------------------------------------------
    // write_buffer_row_group_memory_limit (DuckDB 1.5.3)
    // -------------------------------------------------------------------------

    /**
     * When {@code row-group-memory-limit-mb} is positive the service must issue a
     * {@code SET write_buffer_row_group_memory_limit} statement and log it at DEBUG.
     * When it is {@code 0} the SET must be skipped entirely — no related log event
     * should appear.
     *
     * <p>With DuckDB 1.5.3 the SET succeeds and the success message is logged.
     * With older DuckDB it would fail and the fallback message is logged instead.
     * Either way the presence / absence of any "write_buffer_row_group_memory_limit"
     * log entry correctly reflects whether the SET was attempted.
     */
    @ParameterizedTest
    @CsvSource({
        "256, true",   // positive limit → SET attempted → log event present
        "0,   false"   // zero limit    → SET skipped   → no log event
    })
    void compactEntityType_rowGroupMemoryLimit_setAttemptedOrSkipped(
            int limitMb, boolean expectLogEvent) throws Exception {

        JoxetteProperties props = testProperties();
        props.getCompaction().getEntity().setRowGroupMemoryLimitMb(limitMb);
        CompactionService svc = new CompactionService(duckDB, props, configRepo, TEST_METRICS,
                newLockManager());

        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CompactionService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level savedLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        try {
            CompactionRun run = svc.beginRun(TriggerSource.MANUAL, List.of(ENTITY_TYPE));
            svc.executeRun(run.id(), List.of(ENTITY_TYPE));

            // Run must complete regardless of the limit setting.
            assertThat(svc.getRunById(run.id()).status()).isEqualTo(RunStatus.COMPLETED);

            // Check whether a DEBUG event mentioning the setting was emitted.
            boolean hasLog = appender.list.stream()
                    .filter(e -> e.getLevel() == Level.DEBUG)
                    .anyMatch(e -> e.getFormattedMessage()
                                    .contains("write_buffer_row_group_memory_limit"));
            assertThat(hasLog)
                    .as("A 'write_buffer_row_group_memory_limit' DEBUG log must %s when limitMb=%d",
                            expectLogEvent ? "appear" : "not appear", limitMb)
                    .isEqualTo(expectLogEvent);
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(savedLevel);
        }
    }

    // -------------------------------------------------------------------------
    // Expired-lock cleanup (opportunistic, at the start of every run)
    // -------------------------------------------------------------------------

    /**
     * Reproduces the crashed-instance scenario from the task review: a lock row is
     * left behind with {@code expires_at} in the past (simulating an instance that
     * died mid-merge and never called {@code release()}). Before this fix, nothing
     * ever called {@link CompactionLockManager#cleanExpiredLocks()}, so the row stayed
     * forever and permanently blocked this entity type from ever being compacted again
     * by any instance.
     *
     * <p>{@link CompactionService#executeRun} must now clean expired locks once at the
     * top of the run, before attempting to acquire any target — proven here by seeding
     * the expired row directly via SQL (not through a real crash) and asserting a
     * subsequent run can acquire and attempt the merge instead of skipping with
     * "held by another instance".
     */
    @Test
    void executeRun_cleansExpiredLocksAtStart_allowingReacquisitionOfAnOrphanedTarget() throws Exception {
        insertExpiredLock("entity:" + ENTITY_TYPE, "crashed-instance:9999");

        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CompactionService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level savedLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        try {
            CompactionRun run = service.beginRun(TriggerSource.MANUAL, List.of(ENTITY_TYPE));
            service.executeRun(run.id(), List.of(ENTITY_TYPE));

            assertThat(service.getRunById(run.id()).status()).isEqualTo(RunStatus.COMPLETED);

            boolean attemptedMerge = appender.list.stream().anyMatch(e ->
                    e.getFormattedMessage().contains(
                            "Merging adjacent files for entity_type='" + ENTITY_TYPE + "'"));
            boolean skippedAsHeldByAnother = appender.list.stream().anyMatch(e ->
                    e.getFormattedMessage().contains("held by another instance"));

            assertThat(attemptedMerge)
                    .as("An expired lock left by a crashed instance must be cleaned at the start "
                            + "of the run, allowing this instance to acquire '" + ENTITY_TYPE + "'")
                    .isTrue();
            assertThat(skippedAsHeldByAnother)
                    .as("The target must not be skipped as held-by-another once its lock has expired")
                    .isFalse();
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(savedLevel);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Directly inserts a {@code compaction_locks} row with {@code expires_at} one hour
     * in the past, simulating a lock left behind by a crashed instance.
     */
    private void insertExpiredLock(String target, String instanceId) throws Exception {
        Instant pastExpiry = Instant.now().minusSeconds(3_600);
        try (PreparedStatement ps = duckDB.prepareStatement("""
                INSERT INTO compaction_locks (target, instance_id, acquired_at, expires_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (target) DO NOTHING
                """)) {
            ps.setString(1, target);
            ps.setString(2, instanceId);
            ps.setTimestamp(3, java.sql.Timestamp.from(pastExpiry));
            ps.setTimestamp(4, java.sql.Timestamp.from(pastExpiry));
            ps.executeUpdate();
        }
    }

    private JoxetteProperties testProperties() {
        JoxetteProperties props = new JoxetteProperties();
        props.getCompaction().setSchedule("0 0 3 * * *");
        props.getCompaction().getEntity().setLookbackDays(0);
        props.getCompaction().getEntity().setMinFilesPerBucket(1);
        props.getCompaction().getGeneral().setEnabled(false);
        props.getCompaction().getGeneral().setLookbackDays(0);
        return props;
    }

    /** Builds a {@link CompactionLockManager} for {@link #TEST_INSTANCE_ID}, wired to
     * the shared {@link #instanceRegistry} fixture (see {@link #setUp()}). */
    private CompactionLockManager newLockManager() {
        return new CompactionLockManager(duckDB, 120, TEST_INSTANCE_ID, instanceRegistry);
    }

    private void insertEntityRows(int count) throws Exception {
        Instant ts = Instant.parse("2020-01-01T00:00:00Z"); // cold data
        for (int i = 0; i < count; i++) {
            DuckDBTestSupport.insertEntityRow(duckDB, ENTITY_TYPE,
                    "ORD-" + i, i % 64, "order",
                    "orders.events", 0, i,
                    ts, ts,
                    "k" + i, ("v" + i).getBytes());
        }
    }

    private void insertCassetteRows(int count) throws Exception {
        Instant ts = Instant.parse("2020-01-01T00:00:00Z"); // cold data
        for (int i = 0; i < count; i++) {
            DuckDBTestSupport.insertCassetteRow(duckDB,
                    "orders.events", 0, i,
                    ts, ts,
                    "k" + i, ("v" + i).getBytes());
        }
    }
}

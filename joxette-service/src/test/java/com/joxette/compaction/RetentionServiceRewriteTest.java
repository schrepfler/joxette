package com.joxette.compaction;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.support.DuckDBTestSupport;
import com.joxette.support.SqlRecordingConnection;
import org.duckdb.DuckDBConnection;
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
    private Connection retentionConn;
    private SqlRecordingConnection shared;
    private CompactionLockManager lockManager;
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
        lockManager = new CompactionLockManager(duckDB, 120, "test-instance",
                DuckDBTestSupport.newInstanceRegistry(duckDB));
        retentionConn = ((DuckDBConnection) duckDB).duplicate();
        shared = SqlRecordingConnection.wrap(duckDB);
        service = new RetentionService(shared.connection(), retentionConn, configRepo, props, TEST_METRICS, lockManager);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (retentionConn != null && !retentionConn.isClosed()) retentionConn.close();
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

    @Test
    void executeRun_skipsAnEntityTypeWhileCompactionHoldsIt_andReleasesItsLocksAfterwards() throws Exception {
        insertOldEntityRows(5);
        String target = CompactionLockManager.targetForEntityType(ENTITY_TYPE);
        assertThat(lockManager.tryAcquireExclusive(target)).isTrue();   // a merge is in flight
        try {
            RetentionRun run = service.beginRun(TriggerSource.MANUAL);
            service.executeRun(run.id());

            assertThat(service.getRunById(run.id()).status()).isEqualTo(RunStatus.COMPLETED);
            assertThat(countEntityRows()).as("rows must be left for the next run, not deleted mid-merge")
                    .isEqualTo(5L);
        } finally {
            lockManager.releaseExclusive(target);
        }

        RetentionRun second = service.beginRun(TriggerSource.MANUAL);
        service.executeRun(second.id());
        assertThat(countEntityRows()).isZero();
        for (String t : new String[]{target,
                CompactionLockManager.targetForLakeTable("general_orders_events").orElseThrow()}) {
            assertThat(lockManager.tryAcquireExclusive(t)).as(t).isTrue();
            lockManager.releaseExclusive(t);
        }
    }

    /**
     * Retention's lake DELETEs, ducklake_rewrite_data_files and CHECKPOINT all reach object
     * storage. Run on the shared connection, a slow or wedged S3 call there held the monitor
     * that recording, replay and health checks all wait on.
     */
    @Test
    void executeRun_keepsObjectStorageWorkOffTheSharedConnection() throws Exception {
        insertOldEntityRows(5);
        Instant old = Instant.parse("2000-01-01T00:00:00Z");
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 0, old, old, "k", "v".getBytes());

        RetentionRun run = service.beginRun(TriggerSource.MANUAL);
        service.executeRun(run.id());

        assertThat(service.getRunById(run.id()).status()).isEqualTo(RunStatus.COMPLETED);
        assertThat(countEntityRows()).isZero();
        assertThat(DuckDBTestSupport.countRows(duckDB, "lake.main.general_orders_events")).isZero();
        assertThat(shared.sql())
                .as("SQL issued on the shared connection")
                .noneMatch(sql -> sql.contains("DELETE FROM lake.")
                        || sql.contains("ducklake_rewrite_data_files")
                        || sql.contains("CHECKPOINT"));
    }

    private long countEntityRows() throws Exception {
        try (var st = duckDB.createStatement();
             var rs = st.executeQuery("SELECT COUNT(*) FROM lake.main.entity_" + ENTITY_TYPE)) {
            rs.next();
            return rs.getLong(1);
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

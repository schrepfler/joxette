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

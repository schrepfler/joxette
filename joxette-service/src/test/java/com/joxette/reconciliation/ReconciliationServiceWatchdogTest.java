package com.joxette.reconciliation;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.joxette.cluster.InstanceRegistry;
import com.joxette.compaction.CompactionLockManager;
import com.joxette.config.JoxetteProperties;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.support.DuckDBTestSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises {@code ReconciliationService.withObjectStorageWatchdog} — the helper wrapping
 * the five object-storage-touching calls with (1) a DuckDB {@code http_timeout} bound as the
 * real cancellation mechanism and (2) a background thread that logs
 * {@code getQueryProgress()} once a call has run past a threshold. Uses a plain in-memory
 * DuckDB connection: the helper only issues {@code SET http_timeout}/{@code current_setting}
 * and runs an arbitrary {@link java.sql.Statement}-bound action, none of which need a real
 * DuckLake catalog.
 */
class ReconciliationServiceWatchdogTest {

    private Connection duckDB;
    private ReconciliationService service;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        InstanceRegistry instanceRegistry = DuckDBTestSupport.newInstanceRegistry(duckDB);
        JoxetteProperties props = new JoxetteProperties();
        props.getReconciliation().setObjectStorageTimeoutSeconds(7);
        CompactionLockManager lockManager = new CompactionLockManager(duckDB, props, instanceRegistry);
        service = new ReconciliationService(duckDB, props, lockManager,
                new JoxetteMetrics(new SimpleMeterRegistry()), new ObjectMapper());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    private int currentHttpTimeoutSeconds() throws SQLException {
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery("SELECT current_setting('http_timeout')")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Test
    void withObjectStorageWatchdog_setsConfiguredHttpTimeoutDuringTheCall() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            int[] observed = new int[1];
            service.withObjectStorageWatchdog("test-call", st, () -> {
                observed[0] = currentHttpTimeoutSeconds();
                return null;
            }, 10_000, 10_000);

            assertThat(observed[0]).isEqualTo(7);
        }
    }

    @Test
    void withObjectStorageWatchdog_restoresDefaultHttpTimeoutAfterTheCall() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            service.withObjectStorageWatchdog("test-call", st, () -> null, 10_000, 10_000);
        }

        assertThat(currentHttpTimeoutSeconds()).isEqualTo(ReconciliationService.DEFAULT_HTTP_TIMEOUT_SECONDS);
    }

    @Test
    void withObjectStorageWatchdog_restoresDefaultHttpTimeoutEvenWhenActionThrows() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            assertThatThrownBy(() -> service.withObjectStorageWatchdog("test-call", st, () -> {
                throw new SQLException("boom");
            }, 10_000, 10_000)).isInstanceOf(SQLException.class).hasMessageContaining("boom");
        }

        assertThat(currentHttpTimeoutSeconds()).isEqualTo(ReconciliationService.DEFAULT_HTTP_TIMEOUT_SECONDS);
    }

    @Test
    void withObjectStorageWatchdog_returnsTheActionsResult() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            String result = service.withObjectStorageWatchdog("test-call", st, () -> "hello", 10_000, 10_000);

            assertThat(result).isEqualTo("hello");
        }
    }

    @Test
    void withObjectStorageWatchdog_logsProgressOnceThresholdIsCrossed() throws Exception {
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ReconciliationService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level savedLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        try (Statement st = duckDB.createStatement()) {
            // Threshold/interval of 50ms each — the action itself just sleeps past that,
            // exercising the real watchdog timing logic without a slow 10s+ test.
            service.withObjectStorageWatchdog("slow-test-call", st, () -> {
                try {
                    Thread.sleep(400);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return null;
            }, 50, 50);

            List<String> messages = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
            assertThat(messages).anyMatch(m -> m.contains("slow-test-call") && m.contains("still running"));
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(savedLevel);
        }
    }

    @Test
    void withObjectStorageWatchdog_doesNotLogProgress_whenActionFinishesBeforeThreshold() throws Exception {
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ReconciliationService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level savedLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        try (Statement st = duckDB.createStatement()) {
            service.withObjectStorageWatchdog("fast-test-call", st, () -> null, 10_000, 10_000);

            List<String> messages = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
            assertThat(messages).noneMatch(m -> m.contains("fast-test-call") && m.contains("still running"));
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(savedLevel);
        }
    }
}

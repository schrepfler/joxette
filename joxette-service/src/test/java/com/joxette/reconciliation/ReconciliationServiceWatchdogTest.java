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
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import org.duckdb.DuckDBConnection;

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
    /** Reconciliation's own connection — where its object-storage calls and timeout override live. */
    private Connection lake;
    private ReconciliationService service;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        InstanceRegistry instanceRegistry = DuckDBTestSupport.newInstanceRegistry(duckDB);
        JoxetteProperties props = new JoxetteProperties();
        props.getReconciliation().setObjectStorageTimeoutSeconds(7);
        CompactionLockManager lockManager = new CompactionLockManager(duckDB, props, instanceRegistry);
        lake = ((DuckDBConnection) duckDB).duplicate();
        service = new ReconciliationService(duckDB, lake, props, lockManager,
                new JoxetteMetrics(new SimpleMeterRegistry()), new ObjectMapper());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (lake != null && !lake.isClosed()) lake.close();
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    private int currentHttpTimeoutSeconds() throws SQLException {
        return httpTimeoutSecondsOn(lake);
    }

    private static int httpTimeoutSecondsOn(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT current_setting('http_timeout')")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Test
    void withObjectStorageWatchdog_setsConfiguredHttpTimeoutDuringTheCall() throws Exception {
        try (Statement st = lake.createStatement()) {
            int[] observed = new int[1];
            service.withObjectStorageWatchdog("test-call", st, () -> {
                observed[0] = currentHttpTimeoutSeconds();
                return null;
            }, 10_000, 10_000);

            assertThat(observed[0]).isEqualTo(7);
        }
    }

    /**
     * {@code SET http_timeout} is per-connection; issued on the shared connection it would
     * also cap replay and recording reads for the length of every reconciliation call.
     */
    @Test
    void withObjectStorageWatchdog_leavesTheSharedConnectionsHttpTimeoutAlone() throws Exception {
        try (Statement st = lake.createStatement()) {
            int[] observedOnShared = new int[1];
            service.withObjectStorageWatchdog("test-call", st, () -> {
                observedOnShared[0] = httpTimeoutSecondsOn(duckDB);
                return null;
            }, 10_000, 10_000);

            assertThat(observedOnShared[0]).isEqualTo(ReconciliationService.DEFAULT_HTTP_TIMEOUT_SECONDS);
        }
    }

    @Test
    void withObjectStorageWatchdog_restoresDefaultHttpTimeoutAfterTheCall() throws Exception {
        try (Statement st = lake.createStatement()) {
            service.withObjectStorageWatchdog("test-call", st, () -> null, 10_000, 10_000);
        }

        assertThat(currentHttpTimeoutSeconds()).isEqualTo(ReconciliationService.DEFAULT_HTTP_TIMEOUT_SECONDS);
    }

    @Test
    void withObjectStorageWatchdog_restoresDefaultHttpTimeoutEvenWhenActionThrows() throws Exception {
        try (Statement st = lake.createStatement()) {
            assertThatThrownBy(() -> service.withObjectStorageWatchdog("test-call", st, () -> {
                throw new SQLException("boom");
            }, 10_000, 10_000)).isInstanceOf(SQLException.class).hasMessageContaining("boom");
        }

        assertThat(currentHttpTimeoutSeconds()).isEqualTo(ReconciliationService.DEFAULT_HTTP_TIMEOUT_SECONDS);
    }

    @Test
    void withObjectStorageWatchdog_returnsTheActionsResult() throws Exception {
        try (Statement st = lake.createStatement()) {
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
        try (Statement st = lake.createStatement()) {
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
        try (Statement st = lake.createStatement()) {
            service.withObjectStorageWatchdog("fast-test-call", st, () -> null, 10_000, 10_000);

            List<String> messages = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
            assertThat(messages).noneMatch(m -> m.contains("fast-test-call") && m.contains("still running"));
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(savedLevel);
        }
    }
}

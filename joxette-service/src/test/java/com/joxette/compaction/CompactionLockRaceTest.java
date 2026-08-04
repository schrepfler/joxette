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

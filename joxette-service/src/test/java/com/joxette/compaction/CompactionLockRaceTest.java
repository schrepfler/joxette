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
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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

        // Both racing "nodes" are registered as live *before* the race starts, so that
        // executeRun()'s opportunistic cleanLocksForDeadInstances() call (now wired in
        // alongside cleanExpiredLocks()) never mistakes the other node's genuinely
        // in-flight lock for one belonging to a dead instance mid-race.
        InstanceRegistry instanceRegistry = DuckDBTestSupport.newInstanceRegistry(duckDB);
        DuckDBTestSupport.registerLiveInstance(duckDB, "node-a:1001");
        DuckDBTestSupport.registerLiveInstance(duckDB, "node-b:2002");

        // Deterministic-race wiring: nothing else forces the loser's tryAcquire() attempt
        // to happen before the winner's release() completes — on an unlucky thread
        // schedule the winner can run acquire -> merge -> release to completion before the
        // loser even calls tryAcquire(), in which case the loser also succeeds (the row is
        // already gone), yielding 2 merges and 0 skips instead of the intended 1-and-1.
        //
        // loserAttempted is counted down by whichever side's tryAcquire() call returns
        // false (the loser, by construction — DB-level ON CONFLICT guarantees exactly one
        // side wins). Both lock managers' release() blocks on it first, so whichever side
        // turns out to be the winner cannot release the lock until the loser has definitely
        // already observed it held. This makes the "1 merge, 1 skip" outcome deterministic
        // instead of merely probable.
        CountDownLatch loserAttempted = new CountDownLatch(1);
        CompactionLockManager lockA = deterministicLockManager(duckDB, "node-a:1001", instanceRegistry, loserAttempted);
        CompactionLockManager lockB = deterministicLockManager(duckDB, "node-b:2002", instanceRegistry, loserAttempted);
        serviceA = new CompactionService(duckDB, new com.joxette.db.DuckDbSession(duckDB), props, configRepo, TEST_METRICS, lockA);
        serviceB = new CompactionService(duckDB, new com.joxette.db.DuckDbSession(duckDB), props, configRepo, TEST_METRICS, lockB);
    }

    /**
     * Wraps a real {@link CompactionLockManager} so that {@code tryAcquire()} signals
     * {@code loserAttempted} when it loses the race, and {@code release()} blocks on that
     * same latch before delegating — see the "Deterministic-race wiring" comment in
     * {@link #setUp()} for why this removes the test's ~1-in-13 flake.
     */
    private static CompactionLockManager deterministicLockManager(
            Connection duckDB, String instanceId, InstanceRegistry instanceRegistry, CountDownLatch loserAttempted) {
        return new CompactionLockManager(duckDB, 120, instanceId, instanceRegistry) {
            @Override
            public boolean tryAcquire(String target) throws SQLException {
                boolean acquired = super.tryAcquire(target);
                if (!acquired) {
                    loserAttempted.countDown();
                }
                return acquired;
            }

            @Override
            public void release(String target) {
                try {
                    boolean loserHasAttempted = loserAttempted.await(10, TimeUnit.SECONDS);
                    if (!loserHasAttempted) {
                        throw new AssertionError(
                                "Timed out waiting for the losing instance's tryAcquire() attempt "
                                        + "before releasing target '" + target + "'");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                super.release(target);
            }
        };
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

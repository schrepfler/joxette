package com.joxette.management;

import com.joxette.api.error.ConflictException;
import com.joxette.config.JoxetteProperties;
import com.joxette.config.events.ConfigEventBus;
import com.joxette.db.SchemaManager;
import com.joxette.replay.EntityRoute;
import com.joxette.replay.KnownEntitiesRepository;
import com.joxette.support.DuckDBTestSupport;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.SQLDialect.DUCKDB;

/**
 * Proves that {@link EntityController#updateEntityType} closes the race a task review
 * flagged in {@code bbd8e5f}: the guard ({@code KnownEntitiesRepository.countByType})
 * and the conditional write ({@code ConfigRepository.upsertEntityType}) used to be two
 * independently-synchronized JDBC round-trips, leaving a gap in which the recording
 * pipeline's write path ({@code TopicRecorder -> KnownEntitiesRepository.upsertBatch}),
 * called from a topic's own drain/consumer thread — see {@code CLAUDE.md}'s threading
 * model) could insert the very first {@code known_entities} row for an entity type
 * between the count check returning 0 and the bucket-count write committing.
 *
 * <h2>Why this is a lock-scope test, not a black-box outcome test</h2>
 * <p>Once both operations are fully serialized on the shared connection's monitor, the
 * <em>final</em> database state looks identical whether the concurrent
 * {@code upsertBatch} call happened to lose the race and run strictly after
 * {@code updateEntityType}'s synchronized block, or would have — absent the fix —
 * slipped into the gap: either way you end up with {@code bucket_count = new value} and
 * one {@code known_entities} row. A purely black-box assertion on end state cannot tell
 * those two cases apart. So instead this test instruments {@code Connection
 * .prepareStatement} via a delegating proxy shared by {@link ConfigRepository},
 * {@link KnownEntitiesRepository}, and {@link EntityController} (the same object all
 * three {@code synchronized} on in production, per {@code DuckDBConfig}), releases the
 * writer thread at the exact instant the checker thread's count-check statement is
 * prepared (the worst-case timing for the bug), and asserts — from the recorded
 * statement order — that the writer's {@code INSERT INTO known_entities} never executes
 * before the checker's {@code entity_type_configs} write completes. That ordering is
 * only guaranteed if both code paths hold the *same* lock for the *entire*
 * check-then-write sequence, which is exactly what this test is verifying.
 */
class EntityControllerBucketChangeRaceTest {

    private static final String ENTITY_TYPE = "order";

    private Connection realConn;
    private Connection proxyConn;
    private final List<String> statementLog = Collections.synchronizedList(new ArrayList<>());
    private final CountDownLatch countCheckReached = new CountDownLatch(1);

    private EntityController controller;
    private KnownEntitiesRepository knownEntities;

    @BeforeEach
    void setUp() throws Exception {
        realConn = DuckDBTestSupport.newConnection();
        try (PreparedStatement ps = realConn.prepareStatement(
                "INSERT INTO entity_type_configs (entity_type, bucket_count) VALUES (?, ?)")) {
            ps.setString(1, ENTITY_TYPE);
            ps.setInt(2, 256);
            ps.executeUpdate();
        }

        proxyConn = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] { Connection.class },
                (proxy, method, args) -> {
                    if ("prepareStatement".equals(method.getName())
                            && args != null && args.length > 0 && args[0] instanceof String sql) {
                        statementLog.add(Thread.currentThread().getName() + " :: "
                                + sql.strip().replaceAll("\\s+", " "));
                        Object realPs;
                        try {
                            realPs = method.invoke(realConn, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                        if (sql.contains("SELECT COUNT(*) FROM known_entities")) {
                            // Wrap the count-check statement so the artificial delay lands
                            // exactly where the task's race window is: AFTER the SELECT has
                            // already executed and produced its result (0 — DuckDB's
                            // snapshot isolation fixes this the instant the query runs, so
                            // a concurrent insert afterwards cannot change what this
                            // ResultSet reports), but BEFORE the caller (countByType, and
                            // transitively updateEntityType) acts on that result. This
                            // delay lives entirely in the test's own connection proxy, not
                            // in production code: on the buggy (unsynchronized) sequence it
                            // gives the writer thread ample real time to insert its
                            // known_entities row and complete before the checker resumes
                            // and writes entity_type_configs — reliably reproducing the
                            // race instead of leaving it to thread-scheduling luck. On the
                            // fixed sequence it changes only wall-clock time: the writer
                            // still can't even call prepareStatement on this proxy until
                            // the checker's whole synchronized(duckDB) block — including
                            // this delay — has completed, because upsertBatch takes the
                            // same monitor.
                            return wrapCountCheckStatement((PreparedStatement) realPs);
                        }
                        return realPs;
                    }
                    try {
                        return method.invoke(realConn, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });

        ConfigRepository configRepo = new ConfigRepository(proxyConn, new JoxetteProperties());
        knownEntities = new KnownEntitiesRepository(DSL.using(proxyConn, DUCKDB));
        SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
        ConfigEventBus eventBus = Mockito.mock(ConfigEventBus.class);

        controller = new EntityController(configRepo, schemaManager, eventBus, knownEntities, proxyConn);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (realConn != null && !realConn.isClosed()) realConn.close();
    }

    @Test
    void checkAndWriteHoldTheSameLockForTheirFullDuration_writerNeverSlipsIntoTheGap() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        AtomicReference<EntityTypeConfig> checkerResult = new AtomicReference<>();
        AtomicReference<Throwable> checkerFailure = new AtomicReference<>();

        try {
            var checkerFuture = pool.submit(() -> {
                try {
                    checkerResult.set(controller.updateEntityType(
                            ENTITY_TYPE, new EntityController.UpdateEntityRequest(512)));
                } catch (Throwable t) {
                    checkerFailure.set(t);
                }
                return null;
            });

            var writerFuture = pool.submit(() -> {
                // Simulate the recording pipeline's write path racing to insert the
                // *first* known_entities row for this entity type, timed to arrive
                // exactly when the checker thread has just read count == 0.
                countCheckReached.await(10, TimeUnit.SECONDS);
                knownEntities.upsertBatch(
                        List.of(new EntityRoute(ENTITY_TYPE, "order-1", 7, "OrderCreated", "orders.events")),
                        Instant.now());
                return null;
            });

            checkerFuture.get(10, TimeUnit.SECONDS);
            writerFuture.get(10, TimeUnit.SECONDS);
        } finally {
            pool.shutdown();
        }

        // The guard must have observed count == 0 (nothing was recorded yet when the
        // checker's synchronized block started) and so the bucket change must have been
        // accepted, not rejected.
        assertThat(checkerFailure.get()).isNull();
        assertThat(checkerResult.get()).isNotNull();
        assertThat(checkerResult.get().buckets()).isEqualTo(512);

        // The writer's insert must have actually landed (it wasn't dropped or skipped).
        assertThat(knownEntities.countByType(ENTITY_TYPE)).isEqualTo(1L);

        // The crux of the fix: even though the writer was released the instant the
        // checker's count-check statement was prepared — the worst-case timing for the
        // pre-fix bug — the writer's INSERT could not execute until the checker's whole
        // synchronized block (including the entity_type_configs write) had completed.
        int configWriteIndex = indexOfFirstContaining("INSERT INTO entity_type_configs");
        int knownEntitiesInsertIndex = indexOfFirstContaining("INSERT INTO known_entities");

        assertThat(configWriteIndex)
                .as("checker's entity_type_configs write must have been prepared")
                .isNotEqualTo(-1);
        assertThat(knownEntitiesInsertIndex)
                .as("writer's known_entities insert must have been prepared")
                .isNotEqualTo(-1);
        assertThat(knownEntitiesInsertIndex)
                .as("writer's INSERT INTO known_entities must never be prepared before the checker's "
                        + "entity_type_configs write — i.e. it must not slip into the gap between the "
                        + "count check and the bucket-count write. Full statement order: %s",
                        statementLog)
                .isGreaterThan(configWriteIndex);
    }

    @Test
    void secondUpdateAfterFirstEntityIsKnown_isRejectedNotRaced() throws Exception {
        // Sanity check on the other branch: once an entity is genuinely known before the
        // check runs, the guard must reject — proving the lock doesn't just serialize,
        // it still evaluates the same guard condition correctly.
        knownEntities.upsertBatch(
                List.of(new EntityRoute(ENTITY_TYPE, "order-1", 7, "OrderCreated", "orders.events")),
                Instant.now());

        org.junit.jupiter.api.Assertions.assertThrows(ConflictException.class, () ->
                controller.updateEntityType(ENTITY_TYPE, new EntityController.UpdateEntityRequest(512)));
    }

    /**
     * Wraps a real {@link PreparedStatement} so that {@code executeQuery()} releases the
     * writer thread and sleeps 300ms <em>after</em> the real query has already executed
     * and produced its {@link java.sql.ResultSet} — i.e. after the count is already
     * fixed at 0 — but before the caller acts on it. See the call site for why this
     * exact placement matters.
     */
    private PreparedStatement wrapCountCheckStatement(PreparedStatement realPs) {
        return (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class<?>[] { PreparedStatement.class },
                (proxy, method, args) -> {
                    Object result;
                    try {
                        result = method.invoke(realPs, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                    if ("executeQuery".equals(method.getName())) {
                        countCheckReached.countDown();
                        Thread.sleep(300);
                    }
                    return result;
                });
    }

    private int indexOfFirstContaining(String needle) {
        List<String> snapshot;
        synchronized (statementLog) {
            snapshot = new ArrayList<>(statementLog);
        }
        for (int i = 0; i < snapshot.size(); i++) {
            if (snapshot.get(i).contains(needle)) return i;
        }
        return -1;
    }
}

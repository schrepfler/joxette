package com.joxette.compaction;

import com.joxette.cluster.InstanceRegistry;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

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
class CompactionDistributedLockTest {

    private static final String ENTITY_TYPE = "order";
    private static final String LOCK_TARGET = "entity:" + ENTITY_TYPE;
    private static final String INSTANCE_A  = "host-a:1001";
    private static final String INSTANCE_B  = "host-b:2002";
    private static final int    TTL_MINUTES = 120;

    private Connection            conn;
    private InstanceRegistry      registry;
    private CompactionLockManager lockA;
    private CompactionLockManager lockB;
    private JoxetteProperties     props;
    private ConfigRepository      configRepo;

    @BeforeEach
    void setUp() throws Exception {
        conn = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createEntityTable(conn, ENTITY_TYPE);

        // Seed entity_type_configs so ConfigRepository.listEntityTypes() returns the entity.
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO entity_type_configs (entity_type, bucket_count) VALUES (?, ?)")) {
            ps.setString(1, ENTITY_TYPE);
            ps.setInt(2, 64);
            ps.executeUpdate();
        }

        props      = testProperties();
        configRepo = new ConfigRepository(conn, props);
        // Real InstanceRegistry backed by the same connection — no rows are pre-seeded
        // in joxette_instances here, so both INSTANCE_A and INSTANCE_B start out with
        // no live row (i.e. "dead" from cleanLocksForDeadInstances()'s point of view)
        // unless a test explicitly calls DuckDBTestSupport.registerLiveInstance(...).
        registry = DuckDBTestSupport.newInstanceRegistry(conn);
        lockA    = new CompactionLockManager(conn, TTL_MINUTES, INSTANCE_A, registry);
        lockB    = new CompactionLockManager(conn, TTL_MINUTES, INSTANCE_B, registry);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) conn.close();
    }

    // -------------------------------------------------------------------------
    // Basic lock acquisition
    // -------------------------------------------------------------------------

    @Test
    void tryAcquire_firstCaller_succeedsAndOwnsLock() throws Exception {
        assertThat(lockA.tryAcquire(LOCK_TARGET)).isTrue();
        lockA.release(LOCK_TARGET);
    }

    @Test
    void tryAcquire_secondCaller_failsWhenLockHeld() throws Exception {
        lockA.tryAcquire(LOCK_TARGET);

        assertThat(lockB.tryAcquire(LOCK_TARGET)).isFalse();

        lockA.release(LOCK_TARGET);
    }

    @Test
    void tryAcquire_afterRelease_succeedsForOtherInstance() throws Exception {
        lockA.tryAcquire(LOCK_TARGET);
        lockA.release(LOCK_TARGET);

        assertThat(lockB.tryAcquire(LOCK_TARGET)).isTrue();
        lockB.release(LOCK_TARGET);
    }

    @Test
    void tryAcquire_idempotentForSameInstance() throws Exception {
        // Same instance acquiring a lock it already holds returns true.
        lockA.tryAcquire(LOCK_TARGET);

        assertThat(lockA.tryAcquire(LOCK_TARGET))
                .as("Same instance re-acquiring its own lock should see its own row")
                .isTrue();

        lockA.release(LOCK_TARGET);
    }

    // -------------------------------------------------------------------------
    // Release safety
    // -------------------------------------------------------------------------

    @Test
    void release_doesNotDeleteLockOwnedByOtherInstance() throws Exception {
        lockA.tryAcquire(LOCK_TARGET);

        // Instance B attempts to release A's lock — must be a no-op.
        lockB.release(LOCK_TARGET);

        // A's lock row is still present; B cannot acquire.
        assertThat(lockB.tryAcquire(LOCK_TARGET))
                .as("Lock should still belong to A after B's spurious release call")
                .isFalse();

        lockA.release(LOCK_TARGET);
    }

    // -------------------------------------------------------------------------
    // Expired lock cleanup
    // -------------------------------------------------------------------------

    @Test
    void cleanExpiredLocks_removesStaleRows() throws Exception {
        insertExpiredLock(LOCK_TARGET, INSTANCE_A);

        lockB.cleanExpiredLocks();

        // The expired lock is gone; B can now acquire.
        assertThat(lockB.tryAcquire(LOCK_TARGET)).isTrue();
        lockB.release(LOCK_TARGET);
    }

    @Test
    void cleanExpiredLocks_doesNotRemoveActiveLocks() throws Exception {
        lockA.tryAcquire(LOCK_TARGET);   // active lock, expires_at is in the future

        lockB.cleanExpiredLocks();

        // A's active lock must survive the sweep.
        assertThat(lockB.tryAcquire(LOCK_TARGET))
                .as("Active lock (expires_at in the future) must not be removed by cleanExpiredLocks")
                .isFalse();

        lockA.release(LOCK_TARGET);
    }

    // -------------------------------------------------------------------------
    // Startup / opportunistic cleanup — cleanLocksForDeadInstances()
    //
    // Liveness-registry-driven: a lock is reclaimable once its owning instance_id is
    // confirmed absent from the live joxette_instances rows (InstanceRegistry.listAll(),
    // filtered to status "alive") — not because some other process recomputed a matching
    // identity string. This replaces the old identity-matching releaseOwnLocks(), whose
    // hostname-only instance ID let one co-located process delete another still-running
    // co-located process's active lock (see task-2-revision-brief.md).
    // -------------------------------------------------------------------------

    @Test
    void cleanLocksForDeadInstances_reclaimsLockFromInstanceNotInLiveRegistry() throws Exception {
        // INSTANCE_A holds the lock but has no row in joxette_instances at all —
        // e.g. it crashed and was reaped, or never registered. Simulates a genuinely
        // dead/crashed instance, on this host or a remote one (shared-catalog mode).
        assertThat(lockA.tryAcquire(LOCK_TARGET)).isTrue();

        lockB.cleanLocksForDeadInstances();

        // A's lock is gone; B can now acquire.
        assertThat(lockB.tryAcquire(LOCK_TARGET))
                .as("Lock owned by an instance absent from the live registry must be reclaimed")
                .isTrue();
        lockB.release(LOCK_TARGET);
    }

    @Test
    void cleanLocksForDeadInstances_doesNotReclaimLockFromLiveInstance() throws Exception {
        // INSTANCE_A is alive (fresh heartbeat row) and holds the lock.
        DuckDBTestSupport.registerLiveInstance(conn, INSTANCE_A);
        assertThat(lockA.tryAcquire(LOCK_TARGET)).isTrue();

        // A *different* CompactionLockManager (lockB) runs the cleanup — e.g. simulating
        // a co-located process on the same host, or any other instance's opportunistic
        // cleanup from CompactionService.executeRun(). Because A has a live heartbeat
        // row, its lock must survive: this is exactly the co-location collision the
        // hostname-only scheme introduced, closed by checking real liveness instead of
        // a recomputed identity string.
        lockB.cleanLocksForDeadInstances();

        assertThat(lockB.tryAcquire(LOCK_TARGET))
                .as("Live instance A's lock must not be reclaimed by another instance's cleanup")
                .isFalse();

        lockA.release(LOCK_TARGET);
    }

    /**
     * Proves {@code cleanLocksForDeadInstances()} decides "dead" from
     * {@code InstanceRecord.lastHeartbeat()} compared against the independent
     * {@code joxette.compaction.dead-instance-threshold-minutes} (default 30) —
     * NOT from {@code InstanceRecord.status()}, which is computed by
     * {@code InstanceRegistry} using its own 90-second {@code ALIVE_THRESHOLD} for a
     * different purpose ({@code GET /instances} dashboard freshness).
     *
     * <p>A 5-minute-old heartbeat is already {@code "stale"} by {@code InstanceRegistry}'s
     * 90-second threshold — the pre-fix predicate ({@code !"alive".equals(status())})
     * would have reclaimed this lock, exactly the bug: a merge that legitimately blocks
     * the heartbeat thread past 90 seconds (routine, since {@code lock-ttl-minutes}
     * defaults to 240 minutes) would have its lock stolen mid-merge. The fixed
     * threshold-based check must instead treat 5 minutes as comfortably alive, and only
     * reclaim once the heartbeat is older than the 30-minute dead-instance threshold.
     */
    @ParameterizedTest(name = "heartbeatAge={0} -> reclaimedAsDead={1}")
    @MethodSource("heartbeatAgeCases")
    void cleanLocksForDeadInstances_usesDeadInstanceThresholdNotRegistryStatus(
            Duration heartbeatAge, boolean expectReclaimed) throws Exception {
        DuckDBTestSupport.registerInstanceWithHeartbeatAge(conn, INSTANCE_A, heartbeatAge);
        assertThat(lockA.tryAcquire(LOCK_TARGET)).isTrue();

        lockB.cleanLocksForDeadInstances();

        boolean reclaimed = lockB.tryAcquire(LOCK_TARGET);
        assertThat(reclaimed)
                .as("heartbeatAge=%s: lock reclaimed by another instance's cleanup", heartbeatAge)
                .isEqualTo(expectReclaimed);

        if (reclaimed) {
            lockB.release(LOCK_TARGET);
        } else {
            lockA.release(LOCK_TARGET);
        }
    }

    static Stream<Arguments> heartbeatAgeCases() {
        return Stream.of(
                // Older than InstanceRegistry's 90-second ALIVE_THRESHOLD (so status()
                // would already report "stale") but well within the 30-minute
                // dead-instance threshold: must NOT be reclaimed.
                Arguments.of(Duration.ofMinutes(5), false),
                // Older than the 30-minute dead-instance threshold: must be reclaimed.
                Arguments.of(Duration.ofMinutes(35), true)
        );
    }

    @Test
    void cleanExpiredLocks_doesNotTouchLiveInstancesNonExpiredLock() throws Exception {
        // Regression guard: TTL-based cleanExpiredLocks() is independent of the liveness
        // registry and must keep working exactly as before — it must never delete a
        // still-alive instance's lock just because the merge hasn't finished yet.
        DuckDBTestSupport.registerLiveInstance(conn, INSTANCE_A);
        assertThat(lockA.tryAcquire(LOCK_TARGET)).isTrue();   // expires_at is in the future

        lockB.cleanExpiredLocks();

        assertThat(lockB.tryAcquire(LOCK_TARGET))
                .as("TTL sweep must not remove a live instance's non-expired lock")
                .isFalse();

        lockA.release(LOCK_TARGET);
    }

    // -------------------------------------------------------------------------
    // listActiveLocks (GET /compaction/locks data)
    // -------------------------------------------------------------------------

    @Test
    void listActiveLocks_returnsHeldLock() throws Exception {
        lockA.tryAcquire(LOCK_TARGET);

        List<CompactionLockInfo> locks = lockA.listActiveLocks();

        assertThat(locks).hasSize(1);
        CompactionLockInfo info = locks.get(0);
        assertThat(info.target()).isEqualTo(LOCK_TARGET);
        assertThat(info.instanceId()).isEqualTo(INSTANCE_A);
        assertThat(info.acquiredAt()).isNotNull();
        assertThat(info.expiresAt()).isAfter(Instant.now());
        assertThat(info.secondsRemaining()).isGreaterThan(0);

        lockA.release(LOCK_TARGET);
    }

    @Test
    void listActiveLocks_emptyWhenNoLocks() throws Exception {
        assertThat(lockA.listActiveLocks()).isEmpty();
    }

    @Test
    void listActiveLocks_secondsRemainingIsNegativeForExpiredLock() throws Exception {
        insertExpiredLock(LOCK_TARGET, INSTANCE_A);

        List<CompactionLockInfo> locks = lockA.listActiveLocks();

        assertThat(locks).hasSize(1);
        assertThat(locks.get(0).secondsRemaining())
                .as("Stale lock past its expires_at must report negative secondsRemaining")
                .isNegative();

        // Clean up
        lockA.cleanExpiredLocks();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Directly inserts a lock row with an {@code expires_at} one hour in the past,
     * simulating a stale lock left by a crashed instance.
     */
    private void insertExpiredLock(String target, String instanceId) throws SQLException {
        Instant pastExpiry = Instant.now().minusSeconds(3_600); // 1 hour ago
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO compaction_locks (target, instance_id, acquired_at, expires_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (target) DO NOTHING
                """)) {
            ps.setString(1, target);
            ps.setString(2, instanceId);
            ps.setTimestamp(3, Timestamp.from(pastExpiry));
            ps.setTimestamp(4, Timestamp.from(pastExpiry));
            ps.executeUpdate();
        }
    }

    private JoxetteProperties testProperties() {
        JoxetteProperties p = new JoxetteProperties();
        p.getCompaction().setSchedule("0 0 3 * * *");
        p.getCompaction().getEntity().setLookbackDays(0);
        p.getCompaction().getEntity().setMinFilesPerBucket(1);
        p.getCompaction().getGeneral().setEnabled(false);
        p.getCompaction().setLockTtlMinutes(TTL_MINUTES);
        return p;
    }
}

package com.joxette.compaction;

import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link CompactionLockManager}.
 *
 * <p>Two lock-manager instances share the same in-memory DuckDB connection.
 * Covers lock acquisition, release, expiry cleanup, and startup cleanup.
 *
 * <p>Note: since Stage 2 of the Pekko integration, {@link CompactionService}
 * no longer uses {@link CompactionLockManager} — the Pekko ClusterSingleton
 * guarantee replaces DB-level locking.  Tests that created {@code CompactionService}
 * with a lock manager have been removed; the remaining tests cover the lock
 * manager's own contract (useful if the class is re-enabled for non-singleton
 * deployments).
 */
class CompactionDistributedLockTest {

    private static final String ENTITY_TYPE = "order";
    private static final String LOCK_TARGET = "entity:" + ENTITY_TYPE;
    private static final String INSTANCE_A  = "host-a:1001";
    private static final String INSTANCE_B  = "host-b:2002";
    private static final int    TTL_MINUTES = 120;

    private Connection            conn;
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
        lockA      = new CompactionLockManager(conn, TTL_MINUTES, INSTANCE_A);
        lockB      = new CompactionLockManager(conn, TTL_MINUTES, INSTANCE_B);
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
    // Startup cleanup
    // -------------------------------------------------------------------------

    @Test
    void releaseOwnLocks_removesStaleLocksFromPreviousCrash() throws Exception {
        insertExpiredLock(LOCK_TARGET, INSTANCE_A);

        // Instance A "restarts" and runs its @PostConstruct cleanup.
        lockA.releaseOwnLocks();

        // A's stale lock is gone; B can now acquire.
        assertThat(lockB.tryAcquire(LOCK_TARGET)).isTrue();
        lockB.release(LOCK_TARGET);
    }

    @Test
    void releaseOwnLocks_doesNotAffectOtherInstancesLocks() throws Exception {
        lockA.tryAcquire(LOCK_TARGET);

        // B restarts — must not touch A's lock.
        lockB.releaseOwnLocks();

        assertThat(lockB.tryAcquire(LOCK_TARGET))
                .as("B's startup cleanup must not remove A's active lock")
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

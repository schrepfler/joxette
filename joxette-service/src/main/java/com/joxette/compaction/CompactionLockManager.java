package com.joxette.compaction;

import com.joxette.config.JoxetteProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import org.springframework.beans.factory.annotation.Autowired;

import java.net.InetAddress;
import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Manages distributed compaction locks stored in the {@code compaction_locks}
 * plain-DuckDB table (not DuckLake — operational state, not lakehouse data).
 *
 * <h2>Lock protocol</h2>
 * <ol>
 *   <li><b>Acquire</b>: {@code INSERT INTO compaction_locks … ON CONFLICT DO NOTHING},
 *       then read back the row to confirm this instance won the INSERT race.</li>
 *   <li><b>Heartbeat</b>: {@code UPDATE compaction_locks SET expires_at = …} every
 *       {@link #HEARTBEAT_INTERVAL_MINUTES} minutes while a target is being compacted,
 *       so the TTL does not expire under a legitimately long run.</li>
 *   <li><b>Release</b>: {@code DELETE … WHERE target = ? AND instance_id = ?} —
 *       the {@code AND instance_id} guard prevents an instance from accidentally
 *       deleting a lock it no longer owns.</li>
 *   <li><b>Startup cleanup</b>: {@link #releaseOwnLocks()} deletes all rows for this
 *       instance ID on startup, recovering any stale locks left by a previous crash.</li>
 *   <li><b>Expiry cleanup</b>: {@link #cleanExpiredLocks()} deletes rows whose
 *       {@code expires_at} is in the past, reclaiming locks from dead remote instances.
 *       Called at the start of each compaction run.</li>
 * </ol>
 *
 * <h2>Instance ID</h2>
 * <p>Derived once at construction as {@code hostname:pid}.  The same host+PID pair
 * appearing in a stale lock row is therefore safe to delete on restart — it can only
 * belong to a previous incarnation of this process.
 *
 * <h2>Thread safety</h2>
 * <p>All DB access is wrapped in {@code synchronized(duckDB)}, matching the convention
 * used throughout the codebase.
 */
@Component
@DependsOn("dbSchemaManager")
public class CompactionLockManager {

    private static final Logger log = LoggerFactory.getLogger(CompactionLockManager.class);

    /** Heartbeat fires this often to keep a long-running compaction's lock alive. */
    static final int HEARTBEAT_INTERVAL_MINUTES = 10;

    private final Connection duckDB;
    private final int        lockTtlMinutes;
    private final String     instanceId;

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    /** Production constructor — Spring wires {@link Connection} and {@link JoxetteProperties}. */
    @Autowired
    public CompactionLockManager(Connection duckDB, JoxetteProperties props) {
        this(duckDB, props.getCompaction().getLockTtlMinutes(), buildInstanceId());
    }

    /**
     * Test / internal constructor — allows injecting an explicit instance ID so that
     * two lock managers can be created in the same test process without colliding.
     */
    CompactionLockManager(Connection duckDB, int lockTtlMinutes, String instanceId) {
        this.duckDB         = duckDB;
        this.lockTtlMinutes = lockTtlMinutes;
        this.instanceId     = instanceId;
    }

    // -------------------------------------------------------------------------
    // Startup cleanup (@PostConstruct)
    // -------------------------------------------------------------------------

    /**
     * Removes all {@code compaction_locks} rows that belong to this instance ID.
     *
     * <p>Called automatically on Spring bean initialisation via {@link PostConstruct}.
     * Recovers any stale locks that were not released before a crash.
     */
    @PostConstruct
    public void releaseOwnLocks() {
        try {
            int deleted = deleteLocksForInstance(instanceId);
            if (deleted > 0) {
                log.warn("Startup cleanup: removed {} stale compaction lock(s) left by "
                        + "a previous crash (instance={})", deleted, instanceId);
            } else {
                log.debug("Startup cleanup: no stale compaction locks for instance={}", instanceId);
            }
        } catch (SQLException e) {
            log.warn("Startup compaction-lock cleanup failed: {}", e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Tries to acquire the distributed lock for {@code target}.
     *
     * <p>Uses {@code INSERT … ON CONFLICT (target) DO NOTHING} — the only statement
     * that can atomically win the lock race — followed by a {@code SELECT} to
     * confirm ownership.  If a concurrent instance inserted first, the SELECT will
     * return that instance's ID and this method returns {@code false}.
     *
     * @param target lock key, e.g. {@code "entity:order"} or {@code "topic:orders_events"}
     * @return {@code true} if this instance now holds the lock;
     *         {@code false} if another instance holds it
     */
    public boolean tryAcquire(String target) throws SQLException {
        Instant now      = Instant.now();
        Instant expiresAt = now.plus(lockTtlMinutes, ChronoUnit.MINUTES);
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO compaction_locks (target, instance_id, acquired_at, expires_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (target) DO NOTHING
                    """)) {
                ps.setString(1, target);
                ps.setString(2, instanceId);
                ps.setTimestamp(3, Timestamp.from(now));
                ps.setTimestamp(4, Timestamp.from(expiresAt));
                ps.executeUpdate();
            }
            // Read back to confirm ownership.  INSERT … DO NOTHING gives no indication
            // of whether a conflict occurred; checking the row is the only reliable way.
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT instance_id FROM compaction_locks WHERE target = ?")) {
                ps.setString(1, target);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return false;   // cleaned between INSERT and SELECT
                    return instanceId.equals(rs.getString(1));
                }
            }
        }
    }

    /**
     * Extends the expiry of a lock owned by this instance.
     *
     * <p>Called from the heartbeat thread that runs alongside each compaction target
     * to prevent the TTL from expiring during a legitimately long merge.
     * If the UPDATE matches 0 rows the lock was stolen or expired — logged as a warning.
     */
    public void refresh(String target) throws SQLException {
        Instant newExpiry = Instant.now().plus(lockTtlMinutes, ChronoUnit.MINUTES);
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    UPDATE compaction_locks
                    SET    expires_at  = ?
                    WHERE  target      = ?
                    AND    instance_id = ?
                    """)) {
                ps.setTimestamp(1, Timestamp.from(newExpiry));
                ps.setString(2, target);
                ps.setString(3, instanceId);
                int updated = ps.executeUpdate();
                if (updated == 0) {
                    log.warn("Heartbeat for compaction lock '{}' updated 0 rows — "
                            + "lock may have been stolen or already expired", target);
                }
            }
        }
    }

    /**
     * Releases the lock for {@code target} if this instance owns it.
     *
     * <p>The {@code AND instance_id = ?} guard ensures this call is a no-op when
     * another instance holds the lock (e.g. after a TTL-driven steal).
     * Errors are swallowed and logged — the TTL is the safety net.
     */
    public void release(String target) {
        try {
            synchronized (duckDB) {
                try (PreparedStatement ps = duckDB.prepareStatement(
                        "DELETE FROM compaction_locks WHERE target = ? AND instance_id = ?")) {
                    ps.setString(1, target);
                    ps.setString(2, instanceId);
                    ps.executeUpdate();
                }
            }
        } catch (SQLException e) {
            log.warn("Could not release compaction lock for '{}': {}", target, e.getMessage());
        }
    }

    /**
     * Deletes all lock rows whose {@code expires_at} is in the past.
     *
     * <p>Called at the start of each compaction run to reclaim locks from dead
     * instances.  Locks held by healthy instances (heartbeat refreshing {@code expires_at})
     * are unaffected.
     */
    public void cleanExpiredLocks() throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM compaction_locks WHERE expires_at < ?")) {
                ps.setTimestamp(1, Timestamp.from(Instant.now()));
                int deleted = ps.executeUpdate();
                if (deleted > 0) {
                    log.info("Cleaned {} expired compaction lock(s) from dead instances", deleted);
                }
            }
        }
    }

    /**
     * Returns all rows currently in {@code compaction_locks}, ordered by acquisition
     * time ascending.  Used by {@code GET /compaction/locks}.
     */
    public List<CompactionLockInfo> listActiveLocks() throws SQLException {
        List<CompactionLockInfo> result = new ArrayList<>();
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery("""
                     SELECT target,
                            instance_id,
                            acquired_at,
                            expires_at,
                            CAST(EXTRACT(EPOCH FROM (expires_at - now())) AS BIGINT)
                                AS seconds_remaining
                     FROM   compaction_locks
                     ORDER  BY acquired_at ASC
                     """)) {
            while (rs.next()) {
                result.add(new CompactionLockInfo(
                        rs.getString("target"),
                        rs.getString("instance_id"),
                        rs.getTimestamp("acquired_at").toInstant(),
                        rs.getTimestamp("expires_at").toInstant(),
                        rs.getLong("seconds_remaining")
                ));
            }
        }
        return result;
    }

    /** The instance identifier used in lock rows ({@code hostname:pid}). */
    public String getInstanceId() { return instanceId; }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private int deleteLocksForInstance(String id) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM compaction_locks WHERE instance_id = ?")) {
                ps.setString(1, id);
                return ps.executeUpdate();
            }
        }
    }

    private static String buildInstanceId() {
        try {
            String host = InetAddress.getLocalHost().getHostName();
            long   pid  = ProcessHandle.current().pid();
            return host + ":" + pid;
        } catch (Exception e) {
            return "localhost:" + ProcessHandle.current().pid();
        }
    }
}

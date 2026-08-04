package com.joxette.compaction;

import com.joxette.cluster.InstanceRecord;
import com.joxette.cluster.InstanceRegistry;
import com.joxette.config.JoxetteProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import org.springframework.beans.factory.annotation.Autowired;

import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Manages distributed compaction locks stored in the {@code compaction_locks}
 * plain-DuckDB table (not DuckLake — operational state, not lakehouse data).
 *
 * <h2>Lock protocol</h2>
 * <ol>
 *   <li><b>Acquire</b>: {@code INSERT INTO compaction_locks … ON CONFLICT DO NOTHING},
 *       then read back the row to confirm this instance won the INSERT race.</li>
 *   <li><b>Heartbeat</b>: {@code UPDATE compaction_locks SET expires_at = …} every
 *       {@link #HEARTBEAT_INTERVAL_MINUTES} minutes while a target is being compacted.
 *       This is opportunistic, not a guarantee: it shares {@code synchronized(duckDB)}
 *       with the merge SQL on this instance's single embedded-mode connection, so a tick
 *       that fires mid-merge simply blocks until the merge's synchronized block exits —
 *       it cannot extend the TTL <em>during</em> an in-progress merge, only between
 *       merges. The lock's TTL ({@code joxette.compaction.lock-ttl-minutes}), not the
 *       heartbeat, is what must comfortably exceed a single merge's worst-case
 *       duration.</li>
 *   <li><b>Release</b>: {@code DELETE … WHERE target = ? AND instance_id = ?} —
 *       the {@code AND instance_id} guard prevents an instance from accidentally
 *       deleting a lock it no longer owns.</li>
 *   <li><b>Dead-instance cleanup</b>: {@link #cleanLocksForDeadInstances()} deletes all
 *       rows whose {@code instance_id} has no live (heartbeating) row in
 *       {@code joxette_instances}, recovering locks left by a crashed instance — on this
 *       host or, in shared-catalog mode (Quack/PostgreSQL), a remote one. Called both at
 *       startup ({@link PostConstruct}) and opportunistically from
 *       {@link CompactionService#executeRun}.</li>
 *   <li><b>Expiry cleanup</b>: {@link #cleanExpiredLocks()} deletes rows whose
 *       {@code expires_at} is in the past, reclaiming locks whose owning instance is
 *       still alive but whose merge has simply run past the TTL. Called at the start of
 *       each compaction run, alongside dead-instance cleanup, not in place of it.</li>
 * </ol>
 *
 * <h2>Instance ID</h2>
 * <p>Delegated to {@link InstanceRegistry#getInstanceId()} — the same
 * {@code hostname:pid} identity {@code InstanceRegistry} already uses for its own
 * {@code joxette_instances} rows, globally unique per process. This class does not
 * compute its own identity: reusing {@code InstanceRegistry}'s means two processes
 * co-located on the same host (a documented topology, see
 * {@code docs/clustering-deployment.md} §7) never collide, and lock-ownership recovery
 * is driven entirely by {@code InstanceRegistry}'s live-heartbeat data rather than by
 * one process recomputing an identity string that might match another still-running
 * process's.
 *
 * <h2>Thread safety</h2>
 * <p>All DB access is wrapped in {@code synchronized(duckDB)}, matching the convention
 * used throughout the codebase.
 */
@Component
@DependsOn({"dbSchemaManager", "instanceRegistry"})
public class CompactionLockManager {

    private static final Logger log = LoggerFactory.getLogger(CompactionLockManager.class);

    /** Heartbeat fires this often to keep a long-running compaction's lock alive. */
    static final int HEARTBEAT_INTERVAL_MINUTES = 10;

    private final Connection       duckDB;
    private final int              lockTtlMinutes;
    private final String           instanceId;
    private final InstanceRegistry instanceRegistry;
    private final int              deadInstanceThresholdMinutes;

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    /**
     * Production constructor — Spring wires {@link Connection}, {@link JoxetteProperties},
     * and {@link InstanceRegistry}. Reuses {@link InstanceRegistry#getInstanceId()} rather
     * than computing a separate identity (see class javadoc "Instance ID").
     */
    @Autowired
    public CompactionLockManager(Connection duckDB, JoxetteProperties props, InstanceRegistry instanceRegistry) {
        this(duckDB, props.getCompaction().getLockTtlMinutes(), instanceRegistry.getInstanceId(), instanceRegistry,
                props.getCompaction().getDeadInstanceThresholdMinutes());
    }

    /**
     * Test / internal constructor — allows injecting an explicit instance ID (so that
     * two lock managers can be created in the same test process without colliding) and
     * an explicit {@link InstanceRegistry} (so tests can control which instance IDs
     * appear "alive" for {@link #cleanLocksForDeadInstances()}). Uses the default
     * {@code dead-instance-threshold-minutes} (30); use the five-argument constructor
     * to override it explicitly.
     */
    CompactionLockManager(Connection duckDB, int lockTtlMinutes, String instanceId, InstanceRegistry instanceRegistry) {
        this(duckDB, lockTtlMinutes, instanceId, instanceRegistry, new JoxetteProperties.Compaction().getDeadInstanceThresholdMinutes());
    }

    /**
     * Test / internal constructor — as above, but also allows overriding
     * {@code deadInstanceThresholdMinutes} explicitly, e.g. so a test can use a small
     * threshold without waiting real wall-clock minutes for a heartbeat to age out.
     */
    CompactionLockManager(Connection duckDB, int lockTtlMinutes, String instanceId, InstanceRegistry instanceRegistry,
                           int deadInstanceThresholdMinutes) {
        this.duckDB                       = duckDB;
        this.lockTtlMinutes               = lockTtlMinutes;
        this.instanceId                   = instanceId;
        this.instanceRegistry             = instanceRegistry;
        this.deadInstanceThresholdMinutes = deadInstanceThresholdMinutes;
    }

    // -------------------------------------------------------------------------
    // Dead-instance cleanup (@PostConstruct + opportunistic, from CompactionService)
    // -------------------------------------------------------------------------

    /**
     * Removes all {@code compaction_locks} rows whose {@code instance_id}'s
     * {@link InstanceRecord#lastHeartbeat()} is older than
     * {@code joxette.compaction.dead-instance-threshold-minutes} (default 30) —
     * i.e. no {@code joxette_instances} row has heartbeated recently enough to count
     * as "alive" for lock-reclamation purposes.
     *
     * <p><b>Deliberately does not use {@link InstanceRecord#status()}</b> (the
     * {@code InstanceRegistry}-computed {@code "alive"}/{@code "stale"} field, driven
     * by its own 90-second {@code ALIVE_THRESHOLD}). That threshold is tuned for
     * {@code GET /instances} dashboard freshness, not for a safety-critical decision to
     * steal another instance's compaction lock. {@code InstanceRegistry.sendHeartbeat()}
     * and the {@code ducklake_merge_adjacent_files} merge SQL both execute under
     * {@code synchronized(duckDB)} on the single shared embedded-mode connection, so a
     * merge that legitimately holds that monitor for more than 90 seconds — entirely
     * plausible, given {@code lock-ttl-minutes} defaults to 240 <em>minutes</em> — blocks
     * the heartbeat thread and would make {@code status()} report "stale" for a healthy,
     * actively-merging instance. Reading {@code lastHeartbeat()} directly and comparing
     * against the separate, more generous {@code dead-instance-threshold-minutes}
     * avoids reclaiming (and corrupting) a live merge's lock on that false signal.
     *
     * <p>Called automatically on Spring bean initialisation via {@link PostConstruct}
     * (ordered, via {@code @DependsOn("instanceRegistry")}, after
     * {@link InstanceRegistry#initialize()} has reaped genuinely stale rows and upserted
     * this instance's own row — otherwise a fresh restart could see its own
     * not-yet-reaped stale row and skip cleanup it should have done), and again from
     * {@link CompactionService#executeRun} at the top of every run, so recovery is not
     * gated on this specific instance restarting.
     *
     * <p>Unlike the identity-matching scheme this replaces, this method's outcome does
     * not depend on which instance calls it — it reclaims <em>any</em> dead instance's
     * locks, not just "its own". A lock owned by a live instance (present in the
     * registry, heartbeating within the threshold) is left untouched no matter which
     * instance runs the sweep, which is what closes the co-location collision: two
     * processes sharing a host now have distinct {@code hostname:pid} identities in
     * {@code joxette_instances}, so neither can mistake the other's live lock for a dead
     * one.
     *
     * <h3>Belt-and-braces: the lock itself must also be old</h3>
     * <p>The DELETE additionally requires {@code acquired_at < deadBefore} — the lock row
     * itself must be at least {@code deadInstanceThresholdMinutes} old, not merely "owned
     * by an instance_id absent from the live set". This closes two failure modes that a
     * pure absent-from-live-set check does not:
     * <ul>
     *   <li>{@link InstanceRegistry#reapStaleInstances()} deletes a {@code joxette_instances}
     *       row after just 2 minutes of heartbeat silence — far shorter than this class's
     *       own 30-minute threshold — and runs unconditionally at <em>every</em> instance's
     *       startup, not just the affected one. A busy instance whose heartbeat lags past 2
     *       minutes under {@code synchronized(duckDB)} contention can have its registry row
     *       reaped by a completely unrelated instance's startup, well before its lock is
     *       actually stale. Requiring the lock's own age to also exceed the threshold means
     *       a freshly-acquired lock survives that premature reap regardless.</li>
     *   <li>{@link InstanceRegistry#listAll()} swallows {@link SQLException} and returns an
     *       empty list on a transient read failure. Without the age predicate, an empty live
     *       set reads as "every instance is dead" and this method would delete every lock in
     *       the table — including ones held by instances actively merging right now. With the
     *       age predicate, an empty live set only reclaims locks that are <em>also</em>
     *       independently old enough; a lock acquired moments ago survives a transient
     *       registry-read failure no matter what the (empty) live set says.</li>
     * </ul>
     *
     * <h3>Atomicity</h3>
     * <p>The read of {@link InstanceRegistry#listAll()} and the DELETE both execute inside
     * one {@code synchronized(duckDB)} block (this class and {@link InstanceRegistry} share
     * the same connection object, so the lock is the same monitor — {@code listAll()}'s own
     * internal {@code synchronized(connection)} is simply a reentrant no-op for the thread
     * already holding it here). This closes the check-then-act window where an instance
     * that registers and acquires a lock in between the read and the delete would otherwise
     * have its brand-new lock deleted — the same TOCTOU pattern fixed in
     * {@code EntityController.updateEntityType}.
     */
    @PostConstruct
    public void cleanLocksForDeadInstances() {
        try {
            int deleted;
            synchronized (duckDB) {
                Instant deadBefore = Instant.now().minus(deadInstanceThresholdMinutes, ChronoUnit.MINUTES);
                Set<String> liveInstanceIds = instanceRegistry.listAll().stream()
                        .filter(r -> !r.lastHeartbeat().isBefore(deadBefore))
                        .map(InstanceRecord::instanceId)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
                deleted = deleteLocksForDeadInstances(liveInstanceIds, deadBefore);
            }
            if (deleted > 0) {
                log.warn("Reclaimed {} compaction lock(s) owned by instance(s) no longer present "
                        + "in the live registry (crashed or rescheduled)", deleted);
            } else {
                log.debug("No compaction locks to reclaim from dead instances");
            }
        } catch (SQLException e) {
            log.warn("Dead-instance compaction-lock cleanup failed: {}", e.getMessage());
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
     * <p>Called from the heartbeat thread that runs alongside each compaction target.
     * Because this call and the merge SQL both execute under {@code synchronized(duckDB)}
     * on the single shared embedded-mode connection, a call that fires while a merge is
     * still executing simply blocks until the merge's synchronized block exits — it
     * cannot land, and so cannot extend the TTL, <em>during</em> an in-progress merge.
     * It only ever succeeds between merges. The lock's TTL is therefore the real safety
     * margin against a merge outliving its lock, not this heartbeat.
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
        synchronized (duckDB) {
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
        }
        return result;
    }

    /**
     * The instance identifier used in lock rows — delegated to
     * {@link InstanceRegistry#getInstanceId()} (format {@code hostname:pid}).
     */
    public String getInstanceId() { return instanceId; }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Deletes every {@code compaction_locks} row whose {@code instance_id} is not in
     * {@code liveInstanceIds} <em>and</em> whose {@code acquired_at} is older than
     * {@code deadBefore} ({@code now() - deadInstanceThresholdMinutes}).
     *
     * <p>The {@code acquired_at < deadBefore} predicate is required in addition to the
     * absent-from-live-set check — see the "Belt-and-braces" section of
     * {@link #cleanLocksForDeadInstances()}'s javadoc for why an absent owner alone is not
     * sufficient grounds to steal a lock.
     *
     * <p>Values are never string-concatenated into the SQL: the {@code IN} clause is
     * built with one {@code ?} placeholder per live instance ID, sized to the live set.
     * If the live set is empty, every lock row's owner is dead by definition and the
     * statement degrades to {@code DELETE FROM compaction_locks WHERE acquired_at < ?} —
     * there is no live instance for any row's {@code instance_id} to legitimately match,
     * but the age predicate still applies.
     *
     * <p>Must be called from within a {@code synchronized(duckDB)} block already held by
     * the caller (see {@link #cleanLocksForDeadInstances()}) so the live-set read and this
     * delete are atomic; the {@code synchronized(duckDB)} here is a reentrant no-op in that
     * case and a real guard for any other caller.
     */
    private int deleteLocksForDeadInstances(Set<String> liveInstanceIds, Instant deadBefore) throws SQLException {
        synchronized (duckDB) {
            if (liveInstanceIds.isEmpty()) {
                try (PreparedStatement ps = duckDB.prepareStatement(
                        "DELETE FROM compaction_locks WHERE acquired_at < ?")) {
                    ps.setTimestamp(1, Timestamp.from(deadBefore));
                    return ps.executeUpdate();
                }
            }
            String placeholders = String.join(",", Collections.nCopies(liveInstanceIds.size(), "?"));
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM compaction_locks WHERE instance_id NOT IN (" + placeholders + ") "
                            + "AND acquired_at < ?")) {
                int i = 1;
                for (String liveId : liveInstanceIds) {
                    ps.setString(i++, liveId);
                }
                ps.setTimestamp(i, Timestamp.from(deadBefore));
                return ps.executeUpdate();
            }
        }
    }
}

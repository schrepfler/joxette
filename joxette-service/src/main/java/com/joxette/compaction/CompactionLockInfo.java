package com.joxette.compaction;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;

/**
 * Snapshot of one row in {@code compaction_locks} — returned by
 * {@code GET /compaction/locks}.
 *
 * <p>{@code secondsRemaining} is negative when the lock has passed its
 * {@code expires_at} but has not yet been cleaned up by the next compaction run's
 * expiry sweep.
 */
@Schema(description = "An active (or stale) row in the compaction_locks distributed-lock table")
public record CompactionLockInfo(

        @Schema(description = "Lock target key, e.g. 'entity:order' or 'topic:orders_events'",
                example = "entity:order")
        String target,

        @Schema(description = "Instance that holds the lock (hostname)",
                example = "worker-node-1")
        String instanceId,

        @Schema(description = "When the lock was first acquired")
        Instant acquiredAt,

        @Schema(description = "When the lock expires. A heartbeat opportunistically extends " +
                              "this between merges, but cannot do so while a merge is actively " +
                              "running (it shares the single DuckDB connection's lock with the " +
                              "merge SQL) — lock-ttl-minutes, not the heartbeat, is the real " +
                              "safety margin against expiry mid-merge")
        Instant expiresAt,

        @Schema(description = "Seconds until expiry; negative if expires_at is already in the past",
                example = "7189")
        long secondsRemaining

) {}

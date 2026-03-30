package com.joxette.replay;

/**
 * Identifies a single entity cassette destination for a routed message.
 *
 * <p>{@code entityBucket} is pre-computed as
 * {@code hash(entityType, entityId) % buckets} and stored alongside the
 * message so that writers and compaction jobs can partition work by bucket
 * without recomputing the hash.
 */
public record EntityRoute(
        String entityType,
        String entityId,
        int entityBucket
) {}

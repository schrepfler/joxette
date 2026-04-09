package com.joxette.replay;

/**
 * Identifies a single entity cassette destination for a routed message.
 *
 * <p>{@code entityBucket} is pre-computed as
 * {@code hash(entityType, entityId) % buckets} and stored alongside the
 * message so that writers and compaction jobs can partition work by bucket
 * without recomputing the hash.
 *
 * <p>{@code messageType} is the logical label of the message variant that
 * matched (e.g. {@code "marketSet"}, {@code "resultSet"}). It is stored in
 * the {@code message_type} column of the entity cassette so that consumers
 * can filter by message variant when replaying an entity's history.
 */
public record EntityRoute(
        String entityType,
        String entityId,
        int entityBucket,
        String messageType
) {}

package com.joxette.model;

/**
 * Domain model that maps a Kafka topic to an entity type, describing the
 * recording mode for each source.
 *
 * <p>The per-message-type ID extraction rules (formerly {@code entityIdSource}
 * and {@code entityIdExpression}) were moved to {@code entity_source_matchers}
 * and are no longer part of this mapping record.
 *
 * @param entityType  the entity type this mapping belongs to
 * @param topic       the Kafka topic to consume
 * @param mode        recording mode: "entity_only" or "both"
 */
public record EntitySourceMapping(
        String entityType,
        String topic,
        String mode
) {}

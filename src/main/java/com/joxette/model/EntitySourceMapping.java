package com.joxette.model;

/**
 * Domain model that maps a Kafka topic to an entity type, describing how
 * to extract the entity ID from each message.
 *
 * @param entityType          the entity type this mapping belongs to
 * @param topic               the Kafka topic to consume
 * @param entityIdSource      where to extract the entity ID from: "key", "value", or "header"
 * @param entityIdExpression  JSONPath expression applied to {@code entityIdSource}
 */
public record EntitySourceMapping(
        String entityType,
        String topic,
        String entityIdSource,
        String entityIdExpression
) {}

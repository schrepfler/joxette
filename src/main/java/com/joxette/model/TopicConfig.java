package com.joxette.model;

/**
 * Domain model for a Kafka topic recording configuration.
 *
 * @param topic the Kafka topic name (primary key)
 * @param mode  one of "general", "entity_only", or "both"
 */
public record TopicConfig(String topic, String mode) {}

package com.joxette.model;

/**
 * Domain model for an entity type configuration.
 *
 * @param entityType logical name of the entity (primary key)
 * @param buckets    number of hash buckets used to partition entity cassettes
 */
public record EntityTypeConfig(String entityType, int buckets) {}

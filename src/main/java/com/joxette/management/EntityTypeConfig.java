package com.joxette.management;

/** Persisted configuration for a single entity type. */
public record EntityTypeConfig(
        String entityType,
        int buckets,
        /** Maximum age of records in days; {@code null} means no retention limit. */
        Integer retentionDays
) {}

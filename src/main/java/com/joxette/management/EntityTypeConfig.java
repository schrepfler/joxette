package com.joxette.management;

/** Persisted configuration for a single entity type. */
public record EntityTypeConfig(String entityType, int buckets) {}

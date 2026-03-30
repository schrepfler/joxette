package com.joxette.management;

/** Persisted source-mapping for an entity type: which topic feeds it and how to extract IDs. */
public record EntitySourceConfig(
        String entityType,
        String topic,
        /** "key" | "value" | "header" */
        String idSource,
        /** JSONPath expression (for value/header sources) or header name (for header source). */
        String idExpression
) {}

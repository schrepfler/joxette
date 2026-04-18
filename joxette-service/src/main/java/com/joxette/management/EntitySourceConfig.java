package com.joxette.management;

import java.util.ArrayList;
import java.util.List;

/**
 * Persisted source-mapping for an entity type: which topic feeds it, the
 * per-mapping cassette mode, and the list of message-variant matchers.
 *
 * <h2>Mode semantics</h2>
 * <dl>
 *   <dt>{@code "entity_only"}</dt>
 *   <dd>Messages from {@code topic} are written only to the entity cassette.</dd>
 *   <dt>{@code "both"}</dt>
 *   <dd>Messages from {@code topic} are written to both the entity and general
 *       cassettes.</dd>
 * </dl>
 *
 * <h2>Matchers</h2>
 * <p>Each matcher describes one message variant (identified by {@code messageType})
 * that carries the entity's ID. The router tries matchers in declaration order
 * and stops at the first one that yields an ID.
 */
public record EntitySourceConfig(
        String entityType,
        String topic,
        /** "entity_only" | "both" */
        String mode,
        List<MatcherConfig> matchers
) {
    /**
     * Configuration for one message-variant matcher.
     *
     * @param messageType  logical label stored in the cassette's {@code message_type} column
     * @param idSource     "key" | "value" | "header"
     * @param idExpression JSONPath (for value source) or header name (for headers source)
     */
    public record MatcherConfig(
            String messageType,
            String idSource,
            String idExpression
    ) {}

    /** Mutable builder used when assembling from multi-query DB results. */
    public static class Builder {
        private final String entityType;
        private final String topic;
        private final String mode;
        private final List<MatcherConfig> matchers = new ArrayList<>();

        public Builder(String entityType, String topic, String mode) {
            this.entityType = entityType;
            this.topic      = topic;
            this.mode       = mode;
        }

        public void addMatcher(MatcherConfig m) {
            matchers.add(m);
        }

        public EntitySourceConfig build() {
            return new EntitySourceConfig(entityType, topic, mode, List.copyOf(matchers));
        }
    }
}

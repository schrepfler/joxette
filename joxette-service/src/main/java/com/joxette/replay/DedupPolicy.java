package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Controls how duplicate Kafka messages are handled during entity cassette replay.
 *
 * <dl>
 *   <dt>{@code offset} (default)</dt>
 *   <dd>Keep the most-recently-recorded copy per {@code (topic, partition, offset)} tuple.
 *       Standard at-least-once deduplication. Equivalent to the existing
 *       {@code QUALIFY ROW_NUMBER() OVER (PARTITION BY topic, partition, offset ORDER BY recorded_at DESC) = 1}.</dd>
 *   <dt>{@code value}</dt>
 *   <dd>Keep the most-recently-recorded copy per {@code (topic, kafka_value)} tuple.
 *       Suitable for idempotent producers where the payload itself is the identity.
 *       Stronger deduplication — two messages on different offsets with identical value are collapsed.</dd>
 *   <dt>{@code none}</dt>
 *   <dd>No deduplication. All rows including re-deliveries are exposed.
 *       Useful for diagnosing redelivery patterns or auditing raw ingest.</dd>
 * </dl>
 */
public enum DedupPolicy {
    OFFSET("offset"),
    VALUE("value"),
    NONE("none");

    private final String value;

    DedupPolicy(String value) { this.value = value; }

    @JsonValue
    public String getValue() { return value; }
}

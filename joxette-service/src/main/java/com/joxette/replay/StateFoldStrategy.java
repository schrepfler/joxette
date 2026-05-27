package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Strategy for folding an entity's event stream into a single state object.
 *
 * <dl>
 *   <dt>{@code merge_patch} (default)</dt>
 *   <dd>Each event's decoded JSON value is applied to the accumulator via
 *       RFC 7396 JSON Merge Patch.  Non-object payloads are ignored.</dd>
 *   <dt>{@code last_value}</dt>
 *   <dd>The final event's value wins; all earlier events are discarded.</dd>
 *   <dt>{@code last_per_topic}</dt>
 *   <dd>The last value per source topic is kept, then all per-topic values
 *       are merged together (later topics override earlier ones in topic
 *       order of first appearance).</dd>
 * </dl>
 */
public enum StateFoldStrategy {
    MERGE_PATCH("merge_patch"),
    LAST_VALUE("last_value"),
    LAST_PER_TOPIC("last_per_topic");

    private final String value;

    StateFoldStrategy(String value) { this.value = value; }

    @JsonValue
    public String getValue() { return value; }
}

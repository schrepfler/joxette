package com.joxette.management;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Controls which cassette tables a topic's messages are written to.
 *
 * <ul>
 *   <li>{@link #GENERAL} — raw stream only; no entity extraction.</li>
 *   <li>{@link #ENTITY_ONLY} — entity cassettes only; general cassette skipped.</li>
 *   <li>{@link #BOTH} — raw stream and entity cassettes.</li>
 * </ul>
 */
public enum TopicMode {
    GENERAL("general"),
    ENTITY_ONLY("entity_only"),
    BOTH("both");

    private final String value;

    TopicMode(String value) { this.value = value; }

    @JsonValue
    public String getValue() { return value; }

    @JsonCreator
    public static TopicMode fromValue(String value) {
        if (value == null) return GENERAL;
        for (TopicMode m : values()) {
            if (m.value.equalsIgnoreCase(value)) return m;
        }
        throw new IllegalArgumentException("Unknown TopicMode: " + value);
    }

    /** Returns true when messages should be written to the general cassette. */
    public boolean writesGeneral() { return this == GENERAL || this == BOTH; }

    /** Returns true when entity extraction and routing should run. */
    public boolean writesEntities() { return this == ENTITY_ONLY || this == BOTH; }

    @Override
    public String toString() { return value; }
}

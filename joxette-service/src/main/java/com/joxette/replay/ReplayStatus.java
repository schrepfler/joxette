package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** Lifecycle status of a replay-to-topic operation tracked by {@link ReplayCoordinatorActor}. */
public enum ReplayStatus {
    RUNNING("running"),
    COMPLETED("completed"),
    FAILED("failed"),
    CANCELLED("cancelled");

    private final String value;

    ReplayStatus(String value) { this.value = value; }

    @JsonValue
    public String getValue() { return value; }

    @JsonCreator
    public static ReplayStatus fromValue(String value) {
        for (ReplayStatus s : values()) {
            if (s.value.equalsIgnoreCase(value)) return s;
        }
        throw new IllegalArgumentException("Unknown ReplayStatus: " + value);
    }

    @Override
    public String toString() { return value; }
}

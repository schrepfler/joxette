package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** Lifecycle status of a scheduled replay managed by {@link ScheduledReplayService}. */
public enum ScheduledReplayStatus {
    PENDING("pending"),
    STREAMING("streaming"),
    CANCELLED("cancelled"),
    COMPLETED("completed"),
    FAILED("failed");

    private final String value;

    ScheduledReplayStatus(String value) { this.value = value; }

    @JsonValue
    public String getValue() { return value; }

    @JsonCreator
    public static ScheduledReplayStatus fromValue(String value) {
        for (ScheduledReplayStatus s : values()) {
            if (s.value.equalsIgnoreCase(value)) return s;
        }
        throw new IllegalArgumentException("Unknown ScheduledReplayStatus: " + value);
    }

    /** Returns true when the replay can still be cancelled. */
    public boolean isCancellable() {
        return this == PENDING || this == STREAMING;
    }

    @Override
    public String toString() { return value; }
}

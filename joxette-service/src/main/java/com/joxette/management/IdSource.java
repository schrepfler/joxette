package com.joxette.management;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** Where in a Kafka message the entity ID expression is evaluated against. */
public enum IdSource {
    KEY("key"),
    VALUE("value"),
    HEADER("header");

    private final String value;

    IdSource(String value) { this.value = value; }

    @JsonValue
    public String getValue() { return value; }

    @JsonCreator
    public static IdSource fromValue(String value) {
        for (IdSource s : values()) {
            if (s.value.equalsIgnoreCase(value)) return s;
        }
        throw new IllegalArgumentException("Unknown IdSource: " + value);
    }

    @Override
    public String toString() { return value; }
}

package com.sol.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * A single event within a {@link Sequence}.
 *
 * <p>Dimensions are mutable so that SET operations can enrich events in-place
 * without copying the whole sequence.
 */
public final class Event {

    /** Well-known dimension key for the event name (type). */
    public static final String DIM_NAME = "name";

    /** Well-known dimension key for the event timestamp. */
    public static final String DIM_TS = "ts";

    private final String name;
    private final Instant ts;
    private final Map<String, Object> dims;

    public Event(String name, Instant ts, Map<String, Object> dims) {
        this.name = name;
        this.ts   = ts;
        this.dims = new HashMap<>(dims);
        this.dims.put(DIM_NAME, name);
        this.dims.put(DIM_TS,   ts);
    }

    public Event(String name, Instant ts) {
        this(name, ts, Map.of());
    }

    public String name()  { return name; }
    public Instant ts()   { return ts; }

    /** Live view of all dimensions (including {@code name} and {@code ts}). */
    public Map<String, Object> dims() { return dims; }

    public Object dim(String key) { return dims.get(key); }

    public void setDim(String key, Object value) { dims.put(key, value); }

    @Override
    public String toString() {
        return "Event{name=" + name + ", ts=" + ts + "}";
    }
}

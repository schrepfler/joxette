package com.sol.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An ordered list of {@link Event}s belonging to the same entity, plus
 * sequence-level dimensions (e.g. entity_id, entity_type).
 *
 * <p>SOL operations transform sequences independently — there is no
 * cross-sequence state.
 */
public final class Sequence {

    private final String id;
    private final List<Event> events;
    private final Map<String, Object> dims;

    public Sequence(String id, List<Event> events, Map<String, Object> dims) {
        this.id     = id;
        this.events = new ArrayList<>(events);
        this.dims   = new HashMap<>(dims);
    }

    public Sequence(String id, List<Event> events) {
        this(id, events, Map.of());
    }

    public String id()                  { return id; }
    public List<Event> events()         { return events; }
    public Map<String, Object> dims()   { return dims; }
    public Object dim(String key)       { return dims.get(key); }
    public void setDim(String key, Object value) { dims.put(key, value); }
    public int size()                   { return events.size(); }

    /** Returns the event at index {@code i} (negative indices count from the end). */
    public Event get(int i) {
        int idx = i < 0 ? events.size() + i : i;
        return events.get(idx);
    }

    @Override
    public String toString() {
        return "Sequence{id=" + id + ", events=" + events.size() + "}";
    }
}

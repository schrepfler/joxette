package com.sol.model;

import java.util.List;

/**
 * A named label over a contiguous half-open range {@code [from, to)} of events
 * in a {@link Sequence}.
 *
 * <p>Tags are ephemeral: each new MATCH operation removes all existing tags and
 * creates new ones. COMBINE also removes all tags.
 */
public record Tag(String name, int from, int to) {

    /** Number of events covered by this tag. */
    public int length() { return to - from; }

    public boolean isEmpty() { return from >= to; }

    /**
     * Returns the sub-list of events from the given sequence covered by this tag.
     * The list is a view — not a copy.
     */
    public List<Event> events(Sequence seq) {
        return seq.events().subList(from, to);
    }

    /**
     * Resolves a Python-style index (negative counts from the end) to an
     * absolute sequence index, or {@code -1} if out of range.
     */
    public int resolveIndex(int i) {
        int abs = i < 0 ? to + i : from + i;
        return (abs >= from && abs < to) ? abs : -1;
    }
}

package com.joxette.replay;

/**
 * Direction for replay queries and streams.
 *
 * <p>{@link #ASC} — oldest first, matching the natural cassette ordering and
 * preserving the pre-order-param API behaviour (this is the default).
 * <p>{@link #DESC} — latest first, for live-tail and "what's happening now" UX.
 *
 * <p>The cursor tuple is identical in both directions; only the comparison
 * flips. {@link TopicCursor} and {@link EntityCursor} both implement
 * {@link Comparable} with their natural (ASC) ordering; DESC callers should use
 * {@link java.util.Comparator#reverseOrder()} when needed.
 */
public enum Order {
    ASC,
    DESC;

    /** Case-insensitive parser: accepts both {@code asc|desc} and {@code ASC|DESC}. */
    public static Order parse(String raw) {
        if (raw == null) return ASC;
        return switch (raw.trim().toLowerCase()) {
            case "asc"  -> ASC;
            case "desc" -> DESC;
            default     -> throw new IllegalArgumentException(
                    "Invalid order '" + raw + "': must be 'asc' or 'desc'");
        };
    }
}

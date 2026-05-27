package com.joxette.replay;

/**
 * Granularity unit for {@link TimelineService} time buckets.
 *
 * <p>Auto-selected from the overall span of events when not specified:
 * <ul>
 *   <li>{@code MINUTE} — span &lt; 1 hour</li>
 *   <li>{@code HOUR}   — 1 hour ≤ span &lt; 7 days</li>
 *   <li>{@code DAY}    — span ≥ 7 days</li>
 * </ul>
 */
public enum TimelineBucket {
    MINUTE, HOUR, DAY
}

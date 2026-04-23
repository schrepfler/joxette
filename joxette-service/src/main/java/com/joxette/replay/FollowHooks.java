package com.joxette.replay;

import java.time.Duration;

/**
 * Callbacks the replay services use to notify the SSE/NDJSON handler about
 * follow-mode lifecycle transitions, so the handler can emit transport-specific
 * markers (preamble, heartbeat, overflow) without leaking transport concerns
 * into the service layer.
 *
 * @param <R> the record type (unused today but keeps the shape open for
 *            callbacks that want to peek at the last emitted record)
 */
public interface FollowHooks<R> {

    /**
     * Called exactly once, after the historical drain completes and before the
     * first live record is emitted.  The handler typically uses this to write a
     * {@code follow} preamble event telling the client "live tail begins now".
     */
    default void onHistoricalEnd() {}

    /**
     * Called each time the live loop's {@link FollowSubscription#awaitNext}
     * call times out without a record.  The handler uses this to emit a
     * transport-appropriate heartbeat so intermediaries keep the connection
     * open.
     */
    default void onHeartbeat() {}

    /**
     * Called when the bus marks the subscription overflowed (client consumed
     * too slowly).  The handler emits a terminal {@code overflow} event and
     * closes the stream.
     */
    default void onOverflow() {}

    /** Interval between heartbeats — read once, cached for the lifetime of the stream. */
    Duration heartbeatInterval();
}

package com.joxette.replay.transform;

import java.time.Duration;
import java.time.Instant;

/**
 * Mutable per-stream state shared across all {@link TransformPipeline#apply} calls
 * within a single streaming replay (SSE / NDJSON).
 *
 * <h2>Usage</h2>
 * <p>Create one instance before starting the stream, then pass it to every
 * {@link TransformPipeline#apply(ReplayMessage, String, TransformContext)} call.
 * After each call, read {@link #getPendingSleep()} and sleep that duration before
 * emitting the message to the client — this is how {@code time_compress} controls
 * replay speed.
 *
 * <p>For paginated (non-streaming) paths, use the two-argument overload
 * {@link TransformPipeline#apply(ReplayMessage, String)}, which creates a throwaway
 * context per message.  {@link #getPendingSleep()} on a fresh context always returns
 * {@link Duration#ZERO}.
 *
 * <h2>time_freeze "NOW"</h2>
 * <p>When a {@code time_freeze} step is configured with {@code "NOW"}, the frozen
 * instant is {@link #getReplayStartedAt()} — the wall-clock time this context was
 * created, i.e. when the replay stream began.
 *
 * <h2>time_compress anchor</h2>
 * <p>On the first message, {@link TransformPipeline} records the message's anchor
 * timestamp and the current wall-clock time.  On subsequent messages it computes
 * the scaled gap and stores the result in {@link #getPendingSleep()} for the
 * streaming layer to honour.
 *
 * <h2>REPLAY_SEQUENCE</h2>
 * <p>{@link #sequence()} returns a monotonically increasing ordinal for the current
 * message within the replay session.  {@link TransformPipeline} advances it once per
 * message via {@link #setCurrentSequence(long)}, driven by the pipeline's own
 * {@code AtomicLong} counter so the ordinal is consistent across both streaming and
 * paginated paths.
 */
public final class TransformContext {

    /** Wall-clock instant when this replay stream started (used by {@code time_freeze "NOW"}). */
    private final Instant replayStartedAt;

    /** Timestamp of the first message processed by a {@code time_compress} step. */
    private Instant compressAnchorMsgTs  = null;

    /** Wall-clock time when the first {@code time_compress} anchor was recorded. */
    private Instant compressAnchorWallTs = null;

    /**
     * Sleep duration the streaming layer should observe before emitting the current
     * message.  Stays {@link Duration#ZERO} until a {@code time_compress} step processes
     * its second (or later) message.
     */
    private Duration pendingSleep = Duration.ZERO;

    /**
     * Monotonically increasing ordinal for the current message in the replay session.
     * Set once per message by {@link TransformPipeline} before dispatching steps.
     */
    private long sequence = 0;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** Creates a context anchored to {@code Instant.now()} with sequence starting at 0. */
    public TransformContext() {
        this.replayStartedAt = Instant.now();
    }

    /**
     * Public constructor for tests in other packages that need a specific sequence
     * value (e.g. to assert {@code REPLAY_SEQUENCE} output).  The {@code replayId}
     * string is accepted for API compatibility but not stored — replay ID is
     * injected via provenance headers, not via context.
     */
    public TransformContext(String replayId, long sequence) {
        this.replayStartedAt = Instant.now();
        this.sequence        = sequence;
    }

    /** Package-visible constructor for testing — injects a fixed replay-start instant. */
    TransformContext(Instant replayStartedAt) {
        this.replayStartedAt = replayStartedAt;
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    /** The instant when this replay stream started ({@code time_freeze "NOW"} target). */
    public Instant getReplayStartedAt() {
        return replayStartedAt;
    }

    /** First-message timestamp seen by the {@code time_compress} step; null until set. */
    public Instant getCompressAnchorMsgTs() {
        return compressAnchorMsgTs;
    }

    /** Wall-clock time when the first compress anchor was established; null until set. */
    public Instant getCompressAnchorWallTs() {
        return compressAnchorWallTs;
    }

    /**
     * Sleep duration the streaming layer should observe before emitting the current
     * message.  Always {@link Duration#ZERO} for paginated paths or when no
     * {@code time_compress} step is active.
     */
    public Duration getPendingSleep() {
        return pendingSleep;
    }

    /**
     * Monotonically increasing message ordinal for this replay session, starting
     * at {@code 0} for the first message processed by the pipeline.
     *
     * <p>Used by {@code AddComputedFieldStep} when the expression is
     * {@code REPLAY_SEQUENCE}.
     */
    public long sequence() {
        return sequence;
    }

    // -------------------------------------------------------------------------
    // Package-private mutators — only TransformPipeline should call these
    // -------------------------------------------------------------------------

    void setCompressAnchor(Instant msgTs, Instant wallTs) {
        this.compressAnchorMsgTs  = msgTs;
        this.compressAnchorWallTs = wallTs;
    }

    void setPendingSleep(Duration d) {
        this.pendingSleep = (d == null || d.isNegative()) ? Duration.ZERO : d;
    }

    /** Sets the per-message sequence ordinal; called by the pipeline before dispatching steps. */
    void setCurrentSequence(long seq) {
        this.sequence = seq;
    }
}

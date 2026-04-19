package com.joxette.replay.transform;

import com.joxette.replay.transform.gap.FragmentDefinition;
import com.joxette.replay.transform.gap.MessagePattern;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    /** Timestamp of the previous message, used to compute inter-message gap duration. Null for first message. */
    private Instant prevMsgTimestamp = null;

    /**
     * Per-predicate match history: each predicate maps to an ordered list of all messages
     * that matched it, represented as (timestamp, sequenceIndex) pairs.
     */
    private final Map<MessagePattern, List<MatchedOccurrence>> anchorTracker = new LinkedHashMap<>();

    /**
     * Resolved fragment spans: fragment name → resolved span. Only populated once both
     * from+to anchors of a fragment are satisfied (and the optional IfClause passes).
     */
    private final Map<String, ResolvedFragment> resolvedFragments = new HashMap<>();

    /** Fragment definitions loaded from the preset. Set by the pipeline on construction. */
    private List<FragmentDefinition> fragmentDefs = List.of();

    /**
     * All MessagePatterns to actively observe during replay — derived from both fragment
     * definitions and gap selector anchors. Set once by the pipeline via
     * {@link #setWatchedPatterns(java.util.List)}.
     */
    private List<MessagePattern> watchedPatterns = List.of();

    /** A matched occurrence of a MessagePattern — the message timestamp and pipeline sequence index. */
    public record MatchedOccurrence(Instant timestamp, long sequenceIndex) {}

    /** A fully resolved fragment span. */
    public record ResolvedFragment(Instant startAt, Instant endAt, long eventCount) {}

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
    // Mutators — called by TransformPipeline and time transform steps
    // -------------------------------------------------------------------------

    /**
     * Records the compression anchor: the first-message timestamp and the
     * wall-clock time at which it was observed.  Called once by
     * {@link com.joxette.replay.transform.steps.TimeCompressStep} on the first
     * message in the stream.
     */
    public void setCompressAnchor(Instant msgTs, Instant wallTs) {
        this.compressAnchorMsgTs  = msgTs;
        this.compressAnchorWallTs = wallTs;
    }

    /**
     * Sets the sleep duration the streaming layer should observe before emitting
     * the current message.  Negative or null values are clamped to
     * {@link Duration#ZERO}.  Called by
     * {@link com.joxette.replay.transform.steps.TimeCompressStep} once per message.
     */
    public void setPendingSleep(Duration d) {
        this.pendingSleep = (d == null || d.isNegative()) ? Duration.ZERO : d;
    }

    /** Sets the per-message sequence ordinal; called by the pipeline before dispatching steps. */
    void setCurrentSequence(long seq) {
        this.sequence = seq;
    }

    /** Timestamp of the message immediately preceding the current one; null for the first message. */
    public Instant getPrevMsgTimestamp() {
        return prevMsgTimestamp;
    }

    /** Records the current message timestamp as the previous one, called after gap evaluation. */
    public void setPrevMsgTimestamp(Instant ts) {
        this.prevMsgTimestamp = ts;
    }

    /** Live anchor-match history indexed by MessagePattern. */
    public Map<MessagePattern, List<MatchedOccurrence>> getAnchorTracker() {
        return anchorTracker;
    }

    /** Resolved fragment spans indexed by fragment name. */
    public Map<String, ResolvedFragment> getResolvedFragments() {
        return resolvedFragments;
    }

    /** Fragment definitions loaded from the preset. */
    public List<FragmentDefinition> getFragmentDefs() {
        return fragmentDefs;
    }

    /** Called once by the pipeline after construction to inject fragment definitions from the preset. */
    public void setFragmentDefs(List<FragmentDefinition> defs) {
        this.fragmentDefs = defs != null ? List.copyOf(defs) : List.of();
    }

    /** All patterns the pipeline should track during message observation. */
    public List<MessagePattern> getWatchedPatterns() {
        return watchedPatterns;
    }

    /** Called once by the pipeline to register all patterns to track. */
    public void setWatchedPatterns(List<MessagePattern> patterns) {
        this.watchedPatterns = patterns != null ? List.copyOf(patterns) : List.of();
    }

    /**
     * Records a match for the given pattern, creating the list entry if absent.
     */
    public void recordAnchorMatch(MessagePattern pattern, MatchedOccurrence occurrence) {
        anchorTracker.computeIfAbsent(pattern, k -> new ArrayList<>()).add(occurrence);
    }
}

package com.joxette.replay.transform.gap;

import com.joxette.replay.transform.PredicateEvaluator;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformContext;
import com.joxette.replay.transform.TransformContext.MatchedOccurrence;
import com.joxette.replay.transform.TransformContext.ResolvedFragment;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Stateful gap evaluation engine. Called once per message by {@link
 * com.joxette.replay.transform.TransformPipeline} before dispatching steps.
 *
 * <p>Three responsibilities:
 * <ol>
 *   <li>{@link #observeMessage} — record anchor matches for all predicates referenced by
 *       fragment definitions and gap selectors; resolve fragments once both anchors fire.</li>
 *   <li>{@link #resolveGapDuration} — given a gap selector and the current inter-message gap,
 *       return the gap duration when the selector applies to this gap.</li>
 *   <li>{@link #applyOperation} — pure function: old gap → new gap.</li>
 * </ol>
 */
public final class GapEvaluator {

    private GapEvaluator() {}

    // -------------------------------------------------------------------------
    // observeMessage
    // -------------------------------------------------------------------------

    /**
     * Observes the current message: tests it against every predicate referenced by the preset's
     * fragment definitions and records matches in {@code ctx.anchorTracker}. Once both anchors
     * of a fragment are satisfied the resolved span is cached in {@code ctx.resolvedFragments}.
     *
     * <p>Must be called <em>before</em> {@code ctx.setPrevMsgTimestamp(msg.timestamp)} so that
     * {@code prevMsgTimestamp} still refers to the previous message during gap evaluation.
     */
    public static void observeMessage(ReplayMessage msg, TransformContext ctx) {
        long seqIndex = ctx.sequence();

        // Test all registered patterns (from fragment defs + gap selector anchors)
        for (MessagePattern pattern : ctx.getWatchedPatterns()) {
            testAndRecord(pattern, msg, seqIndex, ctx);
        }

        // Attempt fragment resolution for any fragment not yet resolved
        for (FragmentDefinition frag : ctx.getFragmentDefs()) {
            if (ctx.getResolvedFragments().containsKey(frag.name())) continue;

            Optional<Instant> startOpt = resolvePatternTimestamp(frag.from(), ctx, null);
            if (startOpt.isEmpty()) continue;

            Optional<Instant> endOpt = resolvePatternTimestamp(frag.to(), ctx, startOpt.get());
            if (endOpt.isEmpty()) continue;

            Instant start = startOpt.get();
            Instant end   = endOpt.get();

            // Apply IfClause timing constraint
            if (frag.ifClause() != null) {
                long durationMs = Duration.between(start, end).toMillis();
                if (frag.ifClause().minDurationMs() != null && durationMs < frag.ifClause().minDurationMs()) continue;
                if (frag.ifClause().maxDurationMs() != null && durationMs > frag.ifClause().maxDurationMs()) continue;
            }

            // Count events within the fragment span (messages with timestamp in [start, end])
            // We can't count retroactively without buffering all messages, so we approximate
            // with the sequence difference. The pipeline can improve this if needed.
            long eventCount = seqIndex - sequenceIndexForTimestamp(frag.from(), ctx) + 1;

            ctx.getResolvedFragments().put(frag.name(), new ResolvedFragment(start, end, eventCount));
        }
    }

    // -------------------------------------------------------------------------
    // resolveGapDuration
    // -------------------------------------------------------------------------

    /**
     * Returns the current inter-message gap duration when {@code selector} applies to this gap.
     *
     * <p>The current gap is {@code msg.timestamp - ctx.prevMsgTimestamp}. Returns
     * {@link Optional#empty()} when:
     * <ul>
     *   <li>This is the first message ({@code prevMsgTimestamp} is null)</li>
     *   <li>The {@code after} anchor has not yet been seen</li>
     *   <li>The {@code before} anchor does not match the current message</li>
     *   <li>A {@code within_fragment} is named but not yet resolved</li>
     *   <li>The gap duration falls outside {@code min_duration_ms}/{@code max_duration_ms}</li>
     * </ul>
     */
    public static Optional<Duration> resolveGapDuration(GapSelector selector,
                                                        ReplayMessage msg,
                                                        TransformContext ctx) {
        Instant prev = ctx.getPrevMsgTimestamp();
        if (prev == null) return Optional.empty(); // first message — no gap

        Duration gap = Duration.between(prev, msg.timestamp);
        if (gap.isNegative()) gap = Duration.ZERO; // clock skew guard

        // --- after anchor: the previous message must have matched the pattern ---
        if (selector.after() != null) {
            // The gap is "after" a message matching the pattern, so we need to verify
            // that prev == a matched occurrence's timestamp.
            if (!timestampMatchesPattern(selector.after(), prev, ctx)) return Optional.empty();
        }

        // --- before anchor: the current message must match the pattern ---
        if (selector.before() != null) {
            if (!testPattern(selector.before(), msg, ctx)) return Optional.empty();
        }

        // --- within_fragment: both prev and msg.timestamp must fall inside the span ---
        if (selector.withinFragment() != null) {
            ResolvedFragment frag = ctx.getResolvedFragments().get(selector.withinFragment());
            if (frag == null) return Optional.empty(); // fragment not yet resolved
            if (prev.isBefore(frag.startAt()) || msg.timestamp.isAfter(frag.endAt())) {
                return Optional.empty();
            }
        }

        // --- duration filters ---
        long gapMs = gap.toMillis();
        if (selector.minDurationMs() != null && gapMs < selector.minDurationMs()) return Optional.empty();
        if (selector.maxDurationMs() != null && gapMs > selector.maxDurationMs()) return Optional.empty();

        return Optional.of(gap);
    }

    // -------------------------------------------------------------------------
    // applyOperation  (pure)
    // -------------------------------------------------------------------------

    /**
     * Applies a {@link GapOperation} to the current gap, returning the new gap duration.
     * Never returns a negative duration — results are clamped to zero.
     */
    public static Duration applyOperation(Duration gap, GapOperation op) {
        long gapMs = gap.toMillis();
        long newMs = switch (op) {
            case GapOperation.Cut ignored          -> 0L;
            case GapOperation.Hold h               -> h.targetMs();
            case GapOperation.Trim t when t.byMs() != null
                                                   -> Math.max(0L, gapMs - t.byMs());
            case GapOperation.Trim t               -> Math.max(0L, (long)(gapMs * (1.0 - t.byFactor())));
            case GapOperation.Pad p                -> gapMs + p.byMs();
            case GapOperation.Scale s              -> (long)(gapMs * s.factor());
            default                                -> gapMs; // unreachable — sealed
        };
        return Duration.ofMillis(Math.max(0L, newMs));
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /** Tests msg against the pattern's predicate and records a match if it passes. */
    private static void testAndRecord(MessagePattern pattern, ReplayMessage msg,
                                      long seqIndex, TransformContext ctx) {
        if (PredicateEvaluator.evaluate(pattern.predicate(), msg)) {
            ctx.recordAnchorMatch(pattern, new MatchedOccurrence(msg.timestamp, seqIndex));
        }
    }

    /**
     * Returns the resolved timestamp for a pattern given current anchor tracker state.
     * {@code afterTs} is only used for {@code first_after} quantifier — only occurrences
     * strictly after that timestamp are considered.
     */
    private static Optional<Instant> resolvePatternTimestamp(MessagePattern pattern,
                                                             TransformContext ctx,
                                                             Instant afterTs) {
        List<MatchedOccurrence> matches = ctx.getAnchorTracker().get(pattern);
        if (matches == null || matches.isEmpty()) return Optional.empty();

        return switch (pattern.quantifier()) {
            case MessagePattern.Quantifier.First ignored -> Optional.of(matches.get(0).timestamp());
            case MessagePattern.Quantifier.Last  ignored -> Optional.of(matches.get(matches.size() - 1).timestamp());
            case MessagePattern.Quantifier.Any   ignored -> Optional.of(matches.get(matches.size() - 1).timestamp());
            case MessagePattern.Quantifier.Nth n -> {
                int idx = n.n() - 1; // 1-based → 0-based
                yield idx < matches.size() ? Optional.of(matches.get(idx).timestamp()) : Optional.empty();
            }
            case MessagePattern.Quantifier.FirstAfter fa -> {
                // Resolve the anchor's timestamp first
                Optional<Instant> anchorTs = resolvePatternTimestamp(fa.after(), ctx, null);
                if (anchorTs.isEmpty()) yield Optional.empty();
                // Find first match strictly after the anchor's timestamp
                yield matches.stream()
                        .filter(o -> o.timestamp().isAfter(anchorTs.get()))
                        .findFirst()
                        .map(MatchedOccurrence::timestamp);
            }
        };
    }

    /**
     * Returns true if {@code ts} exactly matches the timestamp of the resolved occurrence
     * for {@code pattern} (used for "after" gap selector — the previous message must
     * have been the matched anchor).
     */
    private static boolean timestampMatchesPattern(MessagePattern pattern,
                                                   Instant ts,
                                                   TransformContext ctx) {
        List<MatchedOccurrence> matches = ctx.getAnchorTracker().get(pattern);
        if (matches == null || matches.isEmpty()) return false;

        return switch (pattern.quantifier()) {
            case MessagePattern.Quantifier.First ignored ->
                matches.get(0).timestamp().equals(ts);
            case MessagePattern.Quantifier.Last ignored ->
                matches.get(matches.size() - 1).timestamp().equals(ts);
            case MessagePattern.Quantifier.Any ignored ->
                // Any: gap applies after every matching message
                matches.stream().anyMatch(o -> o.timestamp().equals(ts));
            case MessagePattern.Quantifier.Nth n -> {
                int idx = n.n() - 1;
                yield idx < matches.size() && matches.get(idx).timestamp().equals(ts);
            }
            case MessagePattern.Quantifier.FirstAfter fa -> {
                Optional<Instant> anchorTs = resolvePatternTimestamp(fa.after(), ctx, null);
                if (anchorTs.isEmpty()) yield false;
                yield matches.stream()
                        .filter(o -> o.timestamp().isAfter(anchorTs.get()))
                        .findFirst()
                        .map(o -> o.timestamp().equals(ts))
                        .orElse(false);
            }
        };
    }

    /**
     * Returns true if {@code msg} matches the pattern's predicate AND the match satisfies
     * the quantifier semantics in the current context. Used for "before" selectors where
     * the current message must be the target anchor.
     */
    private static boolean testPattern(MessagePattern pattern, ReplayMessage msg, TransformContext ctx) {
        if (!PredicateEvaluator.evaluate(pattern.predicate(), msg)) return false;

        // For "any" quantifier, any match on this message suffices
        if (pattern.quantifier() instanceof MessagePattern.Quantifier.Any) return true;

        // For other quantifiers we check if the current message's timestamp would be the
        // resolved one (after recording the match, but observeMessage runs first so the
        // match is already recorded when we get here for the "before" check — we rely on
        // the fact that observeMessage was called before resolveGapDuration).
        List<MatchedOccurrence> matches = ctx.getAnchorTracker().get(pattern);
        if (matches == null || matches.isEmpty()) return false;
        return timestampMatchesPattern(pattern, msg.timestamp, ctx);
    }

    /** Returns the sequence index of the first resolved occurrence for a pattern (used for event counting). */
    private static long sequenceIndexForTimestamp(MessagePattern pattern, TransformContext ctx) {
        List<MatchedOccurrence> matches = ctx.getAnchorTracker().get(pattern);
        if (matches == null || matches.isEmpty()) return 0;
        return matches.get(0).sequenceIndex();
    }
}

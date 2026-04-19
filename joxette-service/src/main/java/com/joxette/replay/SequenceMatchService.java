package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.joxette.replay.transform.Predicate;
import com.joxette.replay.transform.PredicateEvaluator;
import com.joxette.replay.transform.ReplayMessage;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * NFA-style sequence matcher over an ordered cassette stream.
 *
 * <p>The algorithm maintains a list of in-progress NFA threads, each
 * representing one candidate sequence that has matched some prefix of the step
 * list.  For every incoming message, each live thread is advanced according to
 * the step's {@code gap} and {@code required}/{@code repeated} flags.  When a
 * thread reaches the end of the step list it is emitted as a complete match;
 * threads that cannot advance further are discarded.
 *
 * <h2>Step semantics</h2>
 * <ul>
 *   <li>{@code gap=any} — wildcard messages are consumed between steps.</li>
 *   <li>{@code gap=immediate} — the next message MUST match the step predicate;
 *       any non-matching message kills the thread.</li>
 *   <li>{@code required=false} — the step is optional; the algorithm forks:
 *       one branch tries to match it, the other skips it.</li>
 *   <li>{@code repeated=true} — the step consumes one-or-more matching messages;
 *       the thread stays on the step while the predicate keeps matching.</li>
 * </ul>
 *
 * <h2>Duration constraints</h2>
 * <p>After a sequence completes, its duration is checked against
 * {@code constraints.minDurationMs} and {@code constraints.maxDurationMs}.
 * Sequences outside the window are not counted and not returned as examples.
 */
@Service
public class SequenceMatchService {

    // =========================================================================
    // DTOs
    // =========================================================================

    /** One step in a sequence pattern. */
    public record MatchStep(
            @JsonProperty("predicate") Predicate predicate,
            @JsonProperty("required")  boolean   required,
            @JsonProperty("repeated")  boolean   repeated,
            /** {@code "any"} or {@code "immediate"} */
            @JsonProperty("gap")       String    gap
    ) {
        public MatchStep {
            if (predicate == null) throw new IllegalArgumentException("predicate must not be null");
            if (gap == null || (!gap.equals("any") && !gap.equals("immediate"))) {
                throw new IllegalArgumentException("gap must be 'any' or 'immediate', got: " + gap);
            }
        }
    }

    /** Duration window and other sequence-level constraints. */
    public record SequenceConstraints(
            @JsonProperty("maxDurationMs") Long maxDurationMs,
            @JsonProperty("minDurationMs") Long minDurationMs
    ) {}

    /** Full request body for sequence matching. */
    public record SequenceMatchRequest(
            @JsonProperty("steps")       List<MatchStep>     steps,
            @JsonProperty("constraints") SequenceConstraints constraints,
            @JsonProperty("limit")       Integer             limit,
            @JsonProperty("from")        Instant             from,
            @JsonProperty("to")          Instant             to
    ) {}

    /** A complete matched sequence with all messages in the span. */
    public record MatchedSequence(
            @JsonProperty("anchorTimestamps") List<Instant>        anchorTimestamps,
            @JsonProperty("messages")         List<CassetteRecord> messages,
            @JsonProperty("durationMs")       long                 durationMs
    ) {}

    /** Response returned from sequence matching. */
    public record SequenceMatchResponse(
            @JsonProperty("totalMessages")    long                  totalMessages,
            @JsonProperty("matchedSequences") long                  matchedSequences,
            @JsonProperty("matchRate")        double                matchRate,
            @JsonProperty("reachRates")       double[]              reachRates,
            @JsonProperty("examples")         List<MatchedSequence> examples
    ) {}

    // =========================================================================
    // NFA thread
    // =========================================================================

    private static final class NfaThread {
        final int stepIndex;
        final List<CassetteRecord> span;
        final List<Instant> anchorTimestamps;

        NfaThread(int stepIndex, List<CassetteRecord> span, List<Instant> anchorTimestamps) {
            this.stepIndex        = stepIndex;
            this.span             = span;
            this.anchorTimestamps = anchorTimestamps;
        }

        NfaThread withMessage(int nextStep, CassetteRecord record, Instant anchor) {
            List<CassetteRecord> newSpan = new ArrayList<>(span);
            newSpan.add(record);
            List<Instant> newAnchors = new ArrayList<>(anchorTimestamps);
            if (anchor != null) newAnchors.add(anchor);
            return new NfaThread(nextStep, newSpan, newAnchors);
        }

        NfaThread withWildcard(CassetteRecord record) {
            List<CassetteRecord> newSpan = new ArrayList<>(span);
            newSpan.add(record);
            return new NfaThread(stepIndex, newSpan, anchorTimestamps);
        }
    }

    // =========================================================================
    // Public API
    // =========================================================================

    /**
     * Matches sequences over the general cassette for {@code topic}.
     */
    public SequenceMatchResponse matchTopic(TopicReplayService topicService,
                                            String topic,
                                            SequenceMatchRequest req) throws SQLException {
        List<CassetteRecord> buffer = new ArrayList<>();
        topicService.streamAll(topic, req.from(), req.to(), null, null, null, buffer::add);
        return match(req, buffer);
    }

    /**
     * Matches sequences over the entity cassette for {@code entityType}/{@code entityId}.
     */
    public SequenceMatchResponse matchEntity(EntityReplayService entityService,
                                             String entityType,
                                             String entityId,
                                             SequenceMatchRequest req) throws SQLException {
        List<CassetteRecord> buffer = new ArrayList<>();
        entityService.streamEntityEvents(entityType, entityId, req.from(), req.to(),
                r -> buffer.add(entityToCassette(r)));
        return match(req, buffer);
    }

    // =========================================================================
    // Core NFA matching (package-private for unit tests)
    // =========================================================================

    SequenceMatchResponse match(SequenceMatchRequest req, List<CassetteRecord> records) {
        List<MatchStep> steps = req.steps();
        if (steps == null || steps.isEmpty()) {
            return new SequenceMatchResponse(records.size(), 0, 0.0, new double[0], List.of());
        }

        int    stepCount = steps.size();
        int    limit     = req.limit() != null && req.limit() > 0 ? req.limit() : 25;
        long[] reachCounts = new long[stepCount];

        // Accumulation state.
        long matched = 0;
        List<MatchedSequence> examples = new ArrayList<>();
        List<NfaThread> threads = new ArrayList<>();

        for (CassetteRecord record : records) {
            ReplayMessage msg = new ReplayMessage(record);
            List<NfaThread> next = new ArrayList<>();

            // --- Advance existing threads ---
            for (NfaThread t : threads) {
                MatchStep step    = steps.get(t.stepIndex);
                boolean   matches = PredicateEvaluator.evaluate(step.predicate(), msg);

                if (matches) {
                    // Anchor this message as a step match.
                    if (step.repeated()) {
                        // Stay on same step for more repeats, but also advance.
                        next.add(t.withMessage(t.stepIndex, record, record.timestamp()));
                    }
                    NfaThread advanced = t.withMessage(t.stepIndex + 1, record, record.timestamp());
                    if (advanced.stepIndex >= stepCount) {
                        // Sequence complete.
                        MatchedSequence seq = buildSequence(advanced.anchorTimestamps, advanced.span);
                        if (satisfiesConstraints(seq, req.constraints())) {
                            for (int i = 0; i < Math.min(advanced.anchorTimestamps.size(), stepCount); i++) {
                                reachCounts[i]++;
                            }
                            matched++;
                            if (examples.size() < limit) examples.add(seq);
                        }
                    } else {
                        next.add(advanced);
                        // Fork across optional steps that can be skipped from here.
                        int s = advanced.stepIndex;
                        while (s < stepCount && !steps.get(s).required()) {
                            s++;
                            if (s >= stepCount) {
                                MatchedSequence seq = buildSequence(advanced.anchorTimestamps, advanced.span);
                                if (satisfiesConstraints(seq, req.constraints())) {
                                    for (int i = 0; i < Math.min(advanced.anchorTimestamps.size(), stepCount); i++) {
                                        reachCounts[i]++;
                                    }
                                    matched++;
                                    if (examples.size() < limit) examples.add(seq);
                                }
                            } else {
                                next.add(new NfaThread(s, new ArrayList<>(advanced.span),
                                        new ArrayList<>(advanced.anchorTimestamps)));
                            }
                        }
                    }
                } else {
                    // Predicate did not match.
                    if ("immediate".equals(step.gap())) {
                        // Thread dies — don't carry forward.
                    } else {
                        // gap=any: swallow wildcard and keep thread alive.
                        next.add(t.withWildcard(record));
                    }
                }
            }

            // --- Start a new thread at step 0 if first step matches ---
            MatchStep firstStep = steps.get(0);
            if (PredicateEvaluator.evaluate(firstStep.predicate(), msg)) {
                reachCounts[0]++;
                List<CassetteRecord> newSpan    = new ArrayList<>();
                newSpan.add(record);
                List<Instant> newAnchors = new ArrayList<>();
                newAnchors.add(record.timestamp());

                if (firstStep.repeated()) {
                    // Keep a thread that stays at step 0 to consume more repetitions.
                    next.add(new NfaThread(0, new ArrayList<>(newSpan), new ArrayList<>(newAnchors)));
                }

                NfaThread advanced = new NfaThread(1, newSpan, newAnchors);
                if (advanced.stepIndex >= stepCount) {
                    // Single-step sequence.
                    MatchedSequence seq = buildSequence(advanced.anchorTimestamps, advanced.span);
                    if (satisfiesConstraints(seq, req.constraints())) {
                        matched++;
                        if (examples.size() < limit) examples.add(seq);
                    }
                } else {
                    next.add(advanced);
                    // Fork for any optional steps immediately after step 0.
                    int s = 1;
                    while (s < stepCount && !steps.get(s).required()) {
                        s++;
                        if (s >= stepCount) {
                            MatchedSequence seq = buildSequence(newAnchors, newSpan);
                            if (satisfiesConstraints(seq, req.constraints())) {
                                matched++;
                                if (examples.size() < limit) examples.add(seq);
                            }
                        } else {
                            next.add(new NfaThread(s, new ArrayList<>(newSpan), new ArrayList<>(newAnchors)));
                        }
                    }
                }
            }

            threads = next;
        }

        long   total     = records.size();
        double matchRate = total == 0 ? 0.0 : (double) matched / total;

        long   step0Reach = reachCounts[0];
        double[] reachRates = new double[stepCount];
        for (int i = 0; i < stepCount; i++) {
            reachRates[i] = step0Reach == 0 ? 0.0 : Math.min(1.0, (double) reachCounts[i] / step0Reach);
        }

        return new SequenceMatchResponse(total, matched, matchRate, reachRates, examples);
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static MatchedSequence buildSequence(List<Instant> anchors, List<CassetteRecord> span) {
        long durationMs = anchors.size() >= 2
                ? Duration.between(anchors.get(0), anchors.get(anchors.size() - 1)).toMillis()
                : 0L;
        return new MatchedSequence(List.copyOf(anchors), List.copyOf(span), durationMs);
    }

    private static boolean satisfiesConstraints(MatchedSequence seq, SequenceConstraints c) {
        if (c == null) return true;
        if (c.maxDurationMs() != null && seq.durationMs() > c.maxDurationMs()) return false;
        if (c.minDurationMs() != null && seq.durationMs() < c.minDurationMs()) return false;
        return true;
    }

    private static CassetteRecord entityToCassette(EntityRecord r) {
        return new CassetteRecord(r.topic(), r.partition(), r.offset(),
                r.timestamp(), r.recordedAt(), r.key(), r.value(), r.headers(), r.messageType());
    }
}

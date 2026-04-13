package com.joxette.replay.transform;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.steps.AddComputedFieldStep;
import com.joxette.replay.transform.steps.AddHeaderStep;
import com.joxette.replay.transform.steps.ConditionalStep;
import com.joxette.replay.transform.steps.CopyToHeaderStep;
import com.joxette.replay.transform.steps.DeleteFieldStep;
import com.joxette.replay.transform.steps.FanOutStep;
import com.joxette.replay.transform.steps.FilterDropStep;
import com.joxette.replay.transform.steps.FlattenFieldStep;
import com.joxette.replay.transform.steps.KeyFromValueStep;
import com.joxette.replay.transform.steps.MergePatchStep;
import com.joxette.replay.transform.steps.NullKeyStep;
import com.joxette.replay.transform.steps.RedirectTopicStep;
import com.joxette.replay.transform.steps.RemapKeyStep;
import com.joxette.replay.transform.steps.RemoveHeaderStep;
import com.joxette.replay.transform.steps.RenameFieldStep;
import com.joxette.replay.transform.steps.TimeCompressStep;
import com.joxette.replay.transform.steps.TimeFreezeStep;
import com.joxette.replay.transform.steps.TimeShiftStep;
import com.joxette.replay.transform.steps.WallTimeStep;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An ordered sequence of {@link TransformStep}s applied to a {@link ReplayMessage}.
 *
 * <p>This class is <em>not</em> a Spring bean — it is per-replay-request and
 * passed as a method argument to the replay services. Use the static
 * {@link #IDENTITY} sentinel when no transformation is required (truly no-op,
 * no metadata injection).
 *
 * <h2>Execution model</h2>
 * <ol>
 *   <li>{@link ReplayMetadataInjector} always runs first when present, injecting
 *       the six provenance headers.</li>
 *   <li>A per-message sequence ordinal is read from the pipeline's internal counter
 *       and written into the {@link TransformContext} via
 *       {@link TransformContext#setCurrentSequence(long)} before steps execute.</li>
 *   <li>User-defined steps execute in declaration order on the current set of
 *       live messages.</li>
 *   <li>{@link FilterDropStep} — evaluates the predicate; drops matching messages
 *       from the live set.</li>
 *   <li>{@link FanOutStep} — expands each live message into N copies (one per
 *       target topic); subsequent steps run independently on each copy.</li>
 *   <li>{@link AddHeaderStep}, {@link RemoveHeaderStep}, {@link CopyToHeaderStep},
 *       {@link RedirectTopicStep} — implemented; structural / JSON steps
 *       ({@code rename_field}, {@code delete_field}, {@code flatten_field},
 *       {@code add_computed_field}, {@code merge_patch}, {@code remap_key},
 *       {@code null_key}, {@code key_from_value}) are fully implemented.</li>
 *   <li>The four time steps ({@code wall_time}, {@code time_shift},
 *       {@code time_compress}, {@code time_freeze}) are fully implemented in both
 *       the list and context-aware overloads.</li>
 *   <li>{@link ConditionalStep} — evaluates the condition predicate; applies
 *       {@code thenSteps} or {@code elseSteps} as a nested pipeline (no second
 *       metadata injection). A {@link FilterDropStep} or {@link FanOutStep} inside
 *       a branch works normally.</li>
 * </ol>
 *
 * <h2>Return type</h2>
 * <p>{@link #apply(ReplayMessage, String)} returns a {@code List<ReplayMessage>}:
 * <ul>
 *   <li>One element — normal pass-through.</li>
 *   <li>Empty — message was dropped by a {@link FilterDropStep}.</li>
 *   <li>Multiple elements — expanded by a {@link FanOutStep}.</li>
 * </ul>
 *
 * <h2>Two usage tiers</h2>
 * <ul>
 *   <li>{@link #IDENTITY} — static sentinel (null injector, no steps). Zero overhead.</li>
 *   <li>{@code new TransformPipeline(List.of(), injector)} — metadata-only pipeline
 *       (provenance headers injected, no user steps).</li>
 * </ul>
 *
 * <h2>Stateful steps and TransformContext</h2>
 * <p>Steps that need cross-message state (e.g. {@code time_compress}) read and write
 * a {@link TransformContext}.  Use {@link #apply(ReplayMessage, String, TransformContext)}
 * for streaming paths, sharing one context across the entire stream.  After each call,
 * check {@link TransformContext#getPendingSleep()} and sleep that duration before
 * emitting the message.
 *
 * <p>The two-argument overload {@link #apply(ReplayMessage, String)} creates a fresh
 * throwaway context per call — appropriate for paginated (non-streaming) paths where
 * cross-message state is not required.  It returns a {@code List<ReplayMessage>} to
 * support fan-out; the streaming overload returns {@code Optional<ReplayMessage>}
 * (fan-out is not applicable in streaming context).
 */
public final class TransformPipeline {

    /**
     * Truly no-op sentinel — skips both metadata injection and all steps.
     * Preserved for internal code paths that do not require provenance headers.
     */
    public static final TransformPipeline IDENTITY =
            new TransformPipeline(List.of(), null);

    private static final Base64.Decoder B64_DEC      = Base64.getUrlDecoder();
    private static final Pattern        TEMPLATE_VAR = Pattern.compile("\\$\\{([^}]+)\\}");

    private final List<TransformStep>    steps;
    private final ReplayMetadataInjector injector;        // null only for IDENTITY
    private final AtomicLong             sequenceCounter; // REPLAY_SEQUENCE source

    /**
     * Creates a pipeline with the given steps and metadata injector.
     *
     * @param steps    ordered list of steps; may be empty but not null
     * @param injector the {@link ReplayMetadataInjector} Spring component;
     *                 {@code null} is only valid for the {@link #IDENTITY} sentinel
     */
    public TransformPipeline(List<TransformStep> steps, ReplayMetadataInjector injector) {
        this.steps           = List.copyOf(steps);
        this.injector        = injector;
        this.sequenceCounter = new AtomicLong(0);
    }

    // -------------------------------------------------------------------------
    // Pipeline execution
    // -------------------------------------------------------------------------

    /**
     * Applies the pipeline to {@code msg}, returning the resulting messages.
     *
     * <p>A fresh {@link TransformContext} is created for this call — suitable for
     * paginated (non-streaming) paths where cross-message state is not needed.
     *
     * @param msg      the mutable message to transform (will be mutated in place
     *                 unless {@link FanOutStep} forces copies)
     * @param replayId UUID of the replay session, forwarded to the injector
     * @return list of output messages; empty if dropped, multiple if fan-out expanded
     */
    public List<ReplayMessage> apply(ReplayMessage msg, String replayId) {
        // Step 1: metadata injection (skipped for the IDENTITY null-injector sentinel)
        if (injector != null) {
            injector.inject(msg, replayId);
        }

        TransformContext ctx = new TransformContext();
        ctx.setCurrentSequence(sequenceCounter.getAndIncrement());

        // Start with the single input message
        List<ReplayMessage> current = new ArrayList<>();
        current.add(msg);

        // Step 2: user-defined steps in order
        for (TransformStep stepOrGuard : steps) {
            if (current.isEmpty()) break;

            // Unwrap GuardedStep — extract the guard predicate and the actual step
            final Predicate    guard;
            final TransformStep step;
            if (stepOrGuard instanceof GuardedStep gs) {
                guard = gs.when();
                step  = gs.delegate();
            } else {
                guard = null;
                step  = stepOrGuard;
            }

            if (guard != null) {
                // Per-message guarded dispatch: apply the step only to messages where the
                // guard passes; pass through messages that don't match the guard.
                List<ReplayMessage> next = new ArrayList<>(current.size());
                for (ReplayMessage m : current) {
                    if (PredicateEvaluator.evaluate(guard, m)) {
                        next.addAll(dispatchBatch(step, new ArrayList<>(List.of(m)), replayId, ctx));
                    } else {
                        next.add(m);
                    }
                }
                current = next;
            } else {
                current = dispatchBatch(step, current, replayId, ctx);
            }
        }

        return current;
    }

    /**
     * Applies the pipeline to {@code msg}, mutating it in place, using the supplied
     * {@code ctx} for cross-message state.
     *
     * <p>Use this overload for streaming (SSE / NDJSON) paths: create one
     * {@link TransformContext} before starting the stream and reuse it across all
     * messages.  After each call, check {@link TransformContext#getPendingSleep()}
     * and sleep that duration before emitting the message to the client.
     *
     * <p>Fan-out is not applicable in the streaming context; {@link FanOutStep} is
     * treated as a pass-through in this overload.
     *
     * @param msg      the mutable message to transform
     * @param replayId UUID of the replay session, forwarded to the injector
     * @param ctx      per-stream mutable context; never null
     * @return {@link Optional#of(Object) Optional.of(msg)} if the message survives
     *         all steps, or {@link Optional#empty()} if dropped by a filter step
     */
    public Optional<ReplayMessage> apply(ReplayMessage msg, String replayId, TransformContext ctx) {
        // Step 1: metadata injection
        if (injector != null) {
            injector.inject(msg, replayId);
        }

        // Step 2: advance the per-message sequence counter
        ctx.setCurrentSequence(sequenceCounter.getAndIncrement());

        // Step 3: user-defined steps in order
        for (TransformStep stepOrGuard : steps) {
            // Unwrap GuardedStep — evaluate the guard; skip this step if it doesn't match
            final TransformStep step;
            if (stepOrGuard instanceof GuardedStep gs) {
                if (!PredicateEvaluator.evaluate(gs.when(), msg)) {
                    continue; // guard didn't match — pass through this step
                }
                step = gs.delegate();
            } else {
                step = stepOrGuard;
            }

            switch (step) {
                case FilterDropStep   fds -> { if (fds.test(msg)) return Optional.empty(); }
                case WallTimeStep     s   -> applyWallTime(msg, s);
                case TimeShiftStep    s   -> applyTimeShift(msg, s);
                case TimeCompressStep s   -> applyTimeCompress(msg, s, ctx);
                case TimeFreezeStep   s   -> applyTimeFreeze(msg, s, ctx);
                // FanOutStep not applicable in streaming context — pass through
                case FanOutStep ignored   -> { }
                case RenameFieldStep      s -> s.apply(msg, ctx);
                case DeleteFieldStep      s -> s.apply(msg, ctx);
                case FlattenFieldStep     s -> s.apply(msg, ctx);
                case AddComputedFieldStep s -> s.apply(msg, ctx);
                case MergePatchStep       s -> s.apply(msg, ctx);
                case RemapKeyStep         s -> s.apply(msg, ctx);
                case NullKeyStep          s -> s.apply(msg, ctx);
                case KeyFromValueStep     s -> s.apply(msg, ctx);
                case ConditionalStep cs -> {
                    List<TransformStep> branch = PredicateEvaluator.evaluate(cs.condition(), msg)
                            ? cs.thenSteps()
                            : cs.elseSteps();
                    if (!branch.isEmpty()) {
                        // Apply branch steps; fan-out is not applicable in the streaming context.
                        List<ReplayMessage> result =
                                new TransformPipeline(branch, null).apply(msg, replayId);
                        if (result.isEmpty()) return Optional.empty();
                        // result.get(0) is the same mutated msg object
                    }
                }
                default -> step.apply(msg);  // AddHeader, RemoveHeader, CopyToHeader, Redirect, etc.
            }
        }

        return Optional.of(msg);
    }

    /** Returns {@code true} when this is the no-op {@link #IDENTITY} sentinel. */
    public boolean isIdentity() {
        return this == IDENTITY;
    }

    /**
     * Returns the ordered list of user-defined steps (unmodifiable).
     * Useful for {@link com.joxette.replay.transform.steps.SqlPushdownAnalyzer}.
     */
    public List<TransformStep> steps() {
        return steps;
    }

    /**
     * Returns a new pipeline with {@code newSteps}, preserving this pipeline's
     * injector. Used after SQL pushdown analysis to create a pruned pipeline.
     */
    public TransformPipeline withSteps(List<TransformStep> newSteps) {
        return new TransformPipeline(newSteps, this.injector);
    }

    // =========================================================================
    // Batch step dispatcher (used by both the main loop and guarded per-message paths)
    // =========================================================================

    /**
     * Dispatches a single {@link TransformStep} against a list of live messages,
     * returning the (potentially expanded or filtered) result list.
     *
     * <p>This helper is called both from the main pipeline loop (unguarded steps)
     * and from the per-message guarded dispatch path. It must not be called with a
     * {@link GuardedStep} — unwrapping is done in the main loop before calling here.
     */
    private List<ReplayMessage> dispatchBatch(TransformStep step,
                                              List<ReplayMessage> current,
                                              String replayId,
                                              TransformContext ctx) {
        return switch (step) {
            case FilterDropStep fds -> current.stream()
                    .filter(m -> !fds.test(m))
                    .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
            case FanOutStep fos -> {
                List<ReplayMessage> expanded = new ArrayList<>(
                        current.size() * fos.topics().size());
                for (ReplayMessage m : current) {
                    for (String targetTopic : fos.topics()) {
                        ReplayMessage copy = m.copy();
                        copy.topic = targetTopic;
                        expanded.add(copy);
                    }
                }
                yield expanded;
            }
            case AddHeaderStep    ahs  -> { current.forEach(m -> applyAddHeader(ahs, m));   yield current; }
            case RemoveHeaderStep rhs  -> { current.forEach(m -> applyRemoveHeader(rhs, m)); yield current; }
            case CopyToHeaderStep cths -> { current.forEach(m -> applyCopyToHeader(cths, m)); yield current; }
            case RedirectTopicStep rts -> { current.forEach(m -> applyRedirectTopic(rts, m)); yield current; }
            case WallTimeStep      s   -> { current.forEach(m -> applyWallTime(m, s));        yield current; }
            case TimeShiftStep     s   -> { current.forEach(m -> applyTimeShift(m, s));       yield current; }
            case TimeCompressStep  s   -> { current.forEach(m -> applyTimeCompress(m, s, ctx)); yield current; }
            case TimeFreezeStep    s   -> { current.forEach(m -> applyTimeFreeze(m, s, ctx));  yield current; }
            case RenameFieldStep      s -> { current.forEach(m -> s.apply(m, ctx)); yield current; }
            case DeleteFieldStep      s -> { current.forEach(m -> s.apply(m, ctx)); yield current; }
            case FlattenFieldStep     s -> { current.forEach(m -> s.apply(m, ctx)); yield current; }
            case AddComputedFieldStep s -> { current.forEach(m -> s.apply(m, ctx)); yield current; }
            case MergePatchStep       s -> { current.forEach(m -> s.apply(m, ctx)); yield current; }
            case RemapKeyStep         s -> { current.forEach(m -> s.apply(m, ctx)); yield current; }
            case NullKeyStep          s -> { current.forEach(m -> s.apply(m, ctx)); yield current; }
            case KeyFromValueStep     s -> { current.forEach(m -> s.apply(m, ctx)); yield current; }
            case ConditionalStep cs -> {
                List<ReplayMessage> next = new ArrayList<>(current.size());
                for (ReplayMessage m : current) {
                    List<TransformStep> branch = PredicateEvaluator.evaluate(cs.condition(), m)
                            ? cs.thenSteps()
                            : cs.elseSteps();
                    if (branch.isEmpty()) {
                        next.add(m);
                    } else {
                        // Run branch as a mini-pipeline; injector is null because
                        // metadata injection already ran at the top of the outer pipeline.
                        List<ReplayMessage> branchResult =
                                new TransformPipeline(branch, null).apply(m, replayId);
                        next.addAll(branchResult);
                    }
                }
                yield next;
            }
            default -> { current.forEach(step::apply); yield current; }
        };
    }

    // =========================================================================
    // Time step implementations
    // =========================================================================

    /**
     * Replaces the target timestamp field with the current wall-clock time.
     * Does not modify the message value payload.
     */
    private static void applyWallTime(ReplayMessage msg, WallTimeStep step) {
        Instant now = Instant.now();
        applyToTimestampTarget(msg, step.target(), __ -> now);
    }

    /**
     * Shifts the target timestamp field(s) by {@code step.shiftMs()} milliseconds.
     * Positive values move timestamps forward; negative values move them backward.
     */
    private static void applyTimeShift(ReplayMessage msg, TimeShiftStep step) {
        Duration delta = Duration.ofMillis(step.shiftMs());
        applyToTimestampTarget(msg, step.target(), ts -> ts.plus(delta));
    }

    /**
     * Computes the sleep duration the streaming layer should observe before emitting
     * this message.  Does <em>not</em> modify the message timestamp.
     *
     * <h3>Algorithm</h3>
     * <ol>
     *   <li><b>First message</b> — records {@code (msgTs, Instant.now())} as the anchor;
     *       sets {@link TransformContext#getPendingSleep()} to zero.</li>
     *   <li><b>Subsequent messages</b> — computes a scaled gap from the anchor:
     *       {@code scaledGap = rawGap / factor}, then sets {@code pendingSleep}
     *       to {@code max(0, anchorWall + scaledGap − now)}.</li>
     * </ol>
     */
    private static void applyTimeCompress(
            ReplayMessage msg, TimeCompressStep step, TransformContext ctx) {
        Instant msgTs = resolveTimestamp(msg, step.target());
        if (msgTs == null) {
            ctx.setPendingSleep(Duration.ZERO);
            return;
        }

        if (ctx.getCompressAnchorMsgTs() == null) {
            ctx.setCompressAnchor(msgTs, Instant.now());
            ctx.setPendingSleep(Duration.ZERO);
        } else {
            long rawGapMs = Duration.between(ctx.getCompressAnchorMsgTs(), msgTs).toMillis();
            if (rawGapMs <= 0) {
                ctx.setPendingSleep(Duration.ZERO);
            } else {
                long scaledMs = Math.round(rawGapMs / step.factor());
                Instant targetWall = ctx.getCompressAnchorWallTs().plusMillis(scaledMs);
                ctx.setPendingSleep(Duration.between(Instant.now(), targetWall));
            }
        }
    }

    /**
     * Freezes the target timestamp field(s) to a fixed instant for every message.
     *
     * <p>{@code "NOW"} (case-insensitive) freezes to {@link TransformContext#getReplayStartedAt()},
     * i.e. the wall-clock time the replay stream began. Any other value must be a
     * valid ISO-8601 instant string (e.g. {@code "2024-01-01T00:00:00Z"}).
     */
    private static void applyTimeFreeze(
            ReplayMessage msg, TimeFreezeStep step, TransformContext ctx) {
        Instant frozen = "NOW".equalsIgnoreCase(step.frozenAt())
                ? ctx.getReplayStartedAt()
                : Instant.parse(step.frozenAt());
        applyToTimestampTarget(msg, step.target(), __ -> frozen);
    }

    /**
     * Applies {@code fn} to the timestamp field(s) indicated by {@code target}.
     *
     * <p>Recognised targets:
     * <ul>
     *   <li>{@code "ALL_TIMESTAMPS"} — applies to {@link ReplayMessage#timestamp},
     *       {@link ReplayMessage#recordedAt}, and any header value that parses as
     *       an ISO-8601 instant.</li>
     *   <li>{@code "$.timestamp"} — the Kafka producer timestamp only.</li>
     *   <li>{@code "$.recorded_at"} — the cassette ingestion timestamp only.</li>
     *   <li>Anything else — silently skipped.</li>
     * </ul>
     */
    private static void applyToTimestampTarget(
            ReplayMessage msg, String target, UnaryOperator<Instant> fn) {
        switch (target) {
            case "ALL_TIMESTAMPS" -> {
                if (msg.timestamp  != null) msg.timestamp  = fn.apply(msg.timestamp);
                if (msg.recordedAt != null) msg.recordedAt = fn.apply(msg.recordedAt);
                shiftTimestampHeaders(msg, fn);
            }
            case "$.timestamp"   -> { if (msg.timestamp  != null) msg.timestamp  = fn.apply(msg.timestamp); }
            case "$.recorded_at" -> { if (msg.recordedAt != null) msg.recordedAt = fn.apply(msg.recordedAt); }
            default              -> { /* unsupported target — silently skip */ }
        }
    }

    /**
     * Scans all headers; for each header whose value parses as an ISO-8601 instant,
     * applies {@code fn} and writes back the result as an ISO-8601 string.
     * Non-timestamp headers are left untouched.
     */
    private static void shiftTimestampHeaders(
            ReplayMessage msg, UnaryOperator<Instant> fn) {
        for (int i = 0; i < msg.headers.size(); i++) {
            CassetteRecord.Header h = msg.headers.get(i);
            if (h.value() == null) continue;
            try {
                Instant ts      = Instant.parse(h.value());
                Instant shifted = fn.apply(ts);
                msg.headers.set(i, new CassetteRecord.Header(h.key(), shifted.toString()));
            } catch (DateTimeParseException ignored) {
                // Not an ISO-8601 timestamp header — leave untouched
            }
        }
    }

    /**
     * Returns the {@link Instant} named by {@code target} from the message envelope,
     * defaulting to {@link ReplayMessage#timestamp} for unrecognised targets.
     * Used by {@code time_compress} to obtain the anchor field value.
     */
    private static Instant resolveTimestamp(ReplayMessage msg, String target) {
        return switch (target) {
            case "$.recorded_at" -> msg.recordedAt;
            default              -> msg.timestamp;   // "$.timestamp", "ALL_TIMESTAMPS", others
        };
    }

    // =========================================================================
    // Header / redirect step dispatch helpers
    // =========================================================================

    private static void applyAddHeader(AddHeaderStep step, ReplayMessage msg) {
        if (step.ifAbsent()
                && msg.headers.stream().anyMatch(h -> step.key().equals(h.key()))) {
            return; // skip — header already exists
        }
        String resolvedValue = resolveTemplate(step.value(), msg);
        msg.headers.add(new CassetteRecord.Header(step.key(), resolvedValue));
    }

    private static void applyRemoveHeader(RemoveHeaderStep step, ReplayMessage msg) {
        msg.headers.removeIf(h -> step.key().equals(h.key()));
    }

    private static void applyCopyToHeader(CopyToHeaderStep step, ReplayMessage msg) {
        Object extracted = extractField(step.source(), msg);
        if (extracted != null) {
            msg.headers.add(new CassetteRecord.Header(step.headerKey(), extracted.toString()));
        }
    }

    private static void applyRedirectTopic(RedirectTopicStep step, ReplayMessage msg) {
        msg.topic = resolveTemplate(step.topic(), msg);
    }

    // =========================================================================
    // Template resolution  (${path} → extracted value)
    // =========================================================================

    /**
     * Resolves {@code ${...}} placeholders in {@code template} against {@code msg}.
     * Returns the template unchanged if it contains no placeholders.
     */
    static String resolveTemplate(String template, ReplayMessage msg) {
        if (template == null || !template.contains("${")) return template;
        Matcher m = TEMPLATE_VAR.matcher(template);
        StringBuilder sb = new StringBuilder();
        int last = 0;
        while (m.find()) {
            sb.append(template, last, m.start());
            Object val = extractField(m.group(1), msg);
            sb.append(val != null ? val : "");
            last = m.end();
        }
        sb.append(template, last, template.length());
        return sb.toString();
    }

    // =========================================================================
    // Field extraction from ReplayMessage by JSONPath-like path
    // =========================================================================

    /**
     * Extracts a field value from {@code msg} by a JSONPath-like {@code path}.
     *
     * <p>Top-level fields (topic, partition, offset, timestamp, key, recorded_at)
     * are read directly. Paths under {@code $.value.*} decode the base64 message
     * value and evaluate the remainder as a JSONPath expression.
     */
    static Object extractField(String path, ReplayMessage msg) {
        return switch (path) {
            case "$.topic"       -> msg.topic;
            case "$.partition"   -> msg.partition;
            case "$.offset"      -> msg.offset;
            case "$.timestamp"   -> msg.timestamp;
            case "$.key"         -> msg.key;
            case "$.recorded_at" -> msg.recordedAt;
            default -> {
                if (!path.startsWith("$.value") || msg.value == null) yield null;
                try {
                    byte[] raw  = B64_DEC.decode(msg.value);
                    String json = new String(raw, StandardCharsets.UTF_8);
                    if (path.equals("$.value")) yield json;
                    // $.value.foo.bar → $.foo.bar
                    String jsonPath = "$" + path.substring("$.value".length());
                    yield JsonPath.read(json, jsonPath);
                } catch (PathNotFoundException e) {
                    yield null;
                } catch (Exception e) {
                    yield null;
                }
            }
        };
    }
}

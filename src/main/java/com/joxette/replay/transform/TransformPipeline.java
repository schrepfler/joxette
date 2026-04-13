package com.joxette.replay.transform;

import com.joxette.replay.transform.steps.FilterDropStep;

import java.util.List;
import java.util.Optional;

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
 *   <li>User-defined steps execute in declaration order.</li>
 *   <li>A {@link FilterDropStep} drops the message — {@link Optional#empty()} is
 *       returned and remaining steps are skipped.</li>
 *   <li>All other steps are stubs at this stage — they pass through unchanged.
 *       Dispatch logic will be added per-step when each is implemented.</li>
 * </ol>
 *
 * <h2>Two usage tiers</h2>
 * <ul>
 *   <li>{@link #IDENTITY} — static sentinel (null injector, no steps). For
 *       internal code paths like {@code ReplayToTopicService} that manage their
 *       own transformations and need zero overhead.</li>
 *   <li>{@code new TransformPipeline(List.of(), injector)} — metadata-only pipeline
 *       (no user steps, but provenance headers are still injected). This is what
 *       {@code CassetteController} creates for every browse/stream request.</li>
 * </ul>
 *
 * <h2>FilterDropStep stub</h2>
 * <p>Condition evaluation is not yet implemented. The stub passes through
 * (never drops) to avoid silently discarding all messages if the step appears
 * in a pipeline before the evaluator is built. A TODO marks the future hook point.
 */
public final class TransformPipeline {

    /**
     * Truly no-op sentinel — skips both metadata injection and all steps.
     * Preserved for internal code paths that do not require provenance headers.
     *
     * <p>This is a static constant initialised before Spring starts, so it
     * carries a {@code null} injector reference (guarded in {@link #apply}).
     */
    public static final TransformPipeline IDENTITY =
            new TransformPipeline(List.of(), null);

    private final List<TransformStep>    steps;
    private final ReplayMetadataInjector injector;  // null only for IDENTITY

    /**
     * Creates a pipeline with the given steps and metadata injector.
     *
     * @param steps    ordered list of steps; may be empty but not null
     * @param injector the {@link ReplayMetadataInjector} Spring component;
     *                 {@code null} is only valid for the {@link #IDENTITY} sentinel
     */
    public TransformPipeline(List<TransformStep> steps, ReplayMetadataInjector injector) {
        this.steps    = List.copyOf(steps);
        this.injector = injector;
    }

    /**
     * Applies the pipeline to {@code msg}, mutating it in place.
     *
     * @param msg      the mutable message to transform
     * @param replayId UUID of the replay session, forwarded to the injector
     * @return {@link Optional#of(Object) Optional.of(msg)} if the message survives
     *         all steps, or {@link Optional#empty()} if a drop step removes it
     */
    public Optional<ReplayMessage> apply(ReplayMessage msg, String replayId) {
        // Step 1: metadata injection (skipped for the IDENTITY null-injector sentinel)
        if (injector != null) {
            injector.inject(msg, replayId);
        }

        // Step 2: user-defined steps in order
        for (TransformStep step : steps) {
            if (step instanceof FilterDropStep) {
                // TODO: evaluate step.condition() against msg; return Optional.empty() when true.
                // Stub: pass through (never drop) until condition evaluator is implemented.
                continue;
            }
            // TODO: dispatch remaining step types via pattern-matching switch when implemented.
            // All other steps are identity stubs — message is unchanged.
        }

        return Optional.of(msg);
    }

    /** Returns {@code true} when this is the no-op {@link #IDENTITY} sentinel. */
    public boolean isIdentity() {
        return this == IDENTITY;
    }
}

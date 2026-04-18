package com.joxette.replay.transform;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Transparent wrapper that pairs a {@link TransformStep} with an optional
 * {@link Predicate} guard.
 *
 * <p>This type is <em>not</em> registered as a named step type in
 * {@link TransformStep}'s {@code @JsonSubTypes} — it is created transparently by
 * {@link TransformStepDeserializer} when a step JSON object contains a {@code "when"}
 * field. The pipeline loop checks {@link #when()} before dispatching to the delegate:
 *
 * <ul>
 *   <li>Guard evaluates to {@code true} → the delegate step executes normally.</li>
 *   <li>Guard evaluates to {@code false} → the message passes through unchanged.</li>
 * </ul>
 *
 * <p>Example JSON that produces a {@code GuardedStep}:
 * <pre>{@code
 * {
 *   "type": "redact", "target": "$.value.email",
 *   "when": { "field": "$.headers[x-env]", "operator": "NEQ", "value": "prod" }
 * }
 * }</pre>
 *
 * <h2>Serialization</h2>
 * <p>{@link GuardedStepSerializer} handles round-trip serialization: it writes the
 * delegate's JSON (including its {@code "type"} field from {@code @JsonTypeInfo})
 * and injects the {@code "when"} predicate into the same object.
 *
 * <p>{@code @JsonTypeInfo(use = NONE)} suppresses the outer interface-level type
 * wrapper so the serializer has full control over the output shape.
 *
 * @param when     the guard predicate; never {@code null} (use the interface default
 *                 returning {@code null} for unguarded steps)
 * @param delegate the actual step to execute when the guard passes
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
@JsonSerialize(using = GuardedStepSerializer.class)
public record GuardedStep(Predicate when, TransformStep delegate) implements TransformStep {

    @Override
    public Predicate when() {
        return when;
    }

    @Override
    public void apply(ReplayMessage msg) {
        delegate.apply(msg);
    }
}

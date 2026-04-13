package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.MessageJsonPath;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformStep;

/**
 * Nulls out the field at {@code target} (JSONPath), removing its value entirely.
 *
 * <p>Intended for PII scrubbing before replaying to a staging environment.
 * If the field is absent or the value body is non-JSON the step is silently
 * skipped.
 *
 * <p>Example:
 * <pre>{@code {"type": "redact", "target": "$.value.ssn"}}</pre>
 */
public record RedactStep(String target) implements TransformStep {

    @Override
    public void apply(ReplayMessage msg) {
        // If there is no value body there is nothing to redact.
        if (target.startsWith("$.value") && msg.value == null) {
            return;
        }
        MessageJsonPath.write(msg, target, null);
    }
}

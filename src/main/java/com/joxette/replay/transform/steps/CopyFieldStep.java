package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.MessageJsonPath;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformStep;

/**
 * Copies the value at JSONPath {@code from} to JSONPath {@code to}.
 *
 * <p>If the source path resolves to {@code null} (absent field, null value, or
 * non-JSON body) the step is silently skipped and {@code to} is left unchanged.
 *
 * <p>Example — copy the order ID from the value body into the message key:
 * <pre>{@code {"type": "copy_field", "from": "$.value.order_id", "to": "$.key"}}</pre>
 */
public record CopyFieldStep(String from, String to) implements TransformStep {

    @Override
    public void apply(ReplayMessage msg) {
        Object val = MessageJsonPath.read(msg, from);
        if (val != null) {
            MessageJsonPath.write(msg, to, val);
        }
    }
}

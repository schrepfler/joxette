package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.MessageJsonPath;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformStep;

/**
 * Pseudonymises the field at {@code target} (JSONPath) by replacing its value
 * with an SHA-256 hex digest.
 *
 * <p>The hash input is {@code salt + stringValue} (salt prepended, empty string
 * when absent). The output is {@code prefix + hex(SHA-256(salt + value))}
 * (prefix prepended, empty string when absent).
 *
 * <p>If the field resolves to {@code null} (absent, null value, non-JSON body)
 * the step is silently skipped.
 *
 * <p>Example — anonymise an email address with an output prefix:
 * <pre>{@code
 * {
 *   "type": "mask_hash",
 *   "target": "$.value.email",
 *   "prefix": "anon-",
 *   "salt": "my-secret-salt"
 * }
 * }</pre>
 */
public record MaskHashStep(String target, String prefix, String salt) implements TransformStep {

    @Override
    public void apply(ReplayMessage msg) {
        Object val = MessageJsonPath.read(msg, target);
        if (val == null) {
            return;
        }
        String toHash = (salt != null ? salt : "") + String.valueOf(val);
        String hex    = MessageJsonPath.sha256Hex(toHash);
        String result = (prefix != null ? prefix : "") + hex;
        MessageJsonPath.write(msg, target, result);
    }
}

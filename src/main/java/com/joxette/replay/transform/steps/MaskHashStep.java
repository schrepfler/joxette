package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

/**
 * Hashes the value at {@code target} (JSONPath) using {@code algorithm} and
 * retains only the first {@code prefixLength} hex characters.
 *
 * <p>Defaults: {@code algorithm = "SHA-256"}, {@code prefixLength = 6}.
 *
 * <p>Example:
 * <pre>{@code {"type": "mask_hash", "target": "$.email", "prefixLength": 8}}</pre>
 */
public record MaskHashStep(String target, String algorithm, int prefixLength)
        implements TransformStep {
    public MaskHashStep {
        if (algorithm == null || algorithm.isBlank()) algorithm = "SHA-256";
        if (prefixLength <= 0) prefixLength = 6;
    }
}

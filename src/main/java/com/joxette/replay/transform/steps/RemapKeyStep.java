package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformStep;

import java.util.Map;

/**
 * Replaces the Kafka message key using a lookup table. If the current key matches
 * an entry in {@code mappings}, it is replaced with the mapped value. Unmatched
 * keys are left unchanged.
 *
 * <p>Example:
 * <pre>{@code {"type": "remap_key", "mappings": {"old-key": "new-key"}}}</pre>
 */
public record RemapKeyStep(Map<String, String> mappings) implements TransformStep {}

package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.databind.JsonNode;
import com.joxette.replay.transform.TransformStep;

/**
 * Applies an RFC 7396 JSON Merge Patch ({@code patch}) to the sub-tree at
 * {@code target} (JSONPath). Null values in the patch remove keys.
 *
 * <p>Example:
 * <pre>{@code {"type": "merge_patch", "target": "$.address", "patch": {"country": "US"}}}</pre>
 */
public record MergePatchStep(String target, JsonNode patch) implements TransformStep {}

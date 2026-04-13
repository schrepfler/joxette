package com.joxette.replay.transform.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.joxette.replay.transform.TransformStep;

/**
 * Injects a Kafka header into the message.
 *
 * <p>{@code value} may be a literal string or a {@code ${path}} template resolved
 * against the message at apply time (e.g. {@code ${$.topic}} or
 * {@code ${$.value.order_id}}).
 *
 * <p>When {@code ifAbsent} is {@code false} (the default) the header is always
 * appended; Kafka allows multiple headers with the same key so duplicates are
 * permitted. When {@code ifAbsent} is {@code true} the header is only added if no
 * header with that key already exists.
 *
 * <p>Examples:
 * <pre>{@code {"type": "add_header", "key": "x-env", "value": "staging"}}</pre>
 * <pre>{@code {"type": "add_header", "key": "x-topic", "value": "${$.topic}", "if_absent": true}}</pre>
 */
public record AddHeaderStep(
        String key,
        String value,
        @JsonProperty("if_absent") boolean ifAbsent
) implements TransformStep {}

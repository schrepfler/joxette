package com.joxette.management;

/**
 * A single row from {@code topic_message_type_matchers}.
 *
 * <p>Semantics: if {@code idSource}/{@code idExpression} extracts a non-null
 * value from a message on {@code topic}, the message is tagged with
 * {@code messageType} in the general cassette. Matchers are tried in insertion
 * order; the first match wins. If no matcher matches the message,
 * {@code message_type} is stored as {@code NULL}.
 *
 * @param topic        Kafka topic this matcher applies to
 * @param messageType  label to store in the cassette (e.g. {@code "OrderCreated"})
 * @param idSource     one of {@code "key"}, {@code "value"}, {@code "header"}
 * @param idExpression JSONPath for {@code "value"} source; header name for
 *                     {@code "header"} source; ignored for {@code "key"} source
 */
public record TopicMatcherConfig(
        String topic,
        String messageType,
        String idSource,
        String idExpression
) {}

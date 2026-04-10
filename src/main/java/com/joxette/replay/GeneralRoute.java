package com.joxette.replay;

/**
 * The general-cassette routing decision for a single message.
 *
 * <p>A non-null {@code GeneralRoute} in a {@link RouteDecision} means the
 * message should be written to the general cassette. The {@code messageType}
 * field is {@code null} when no {@code topic_message_type_matchers} row matched
 * the message (i.e. the message is recorded but the {@code message_type} column
 * will be {@code NULL}).
 *
 * @param messageType label to store in the cassette (e.g. {@code "OrderCreated"}),
 *                    or {@code null} if no matcher matched
 */
public record GeneralRoute(String messageType) {}

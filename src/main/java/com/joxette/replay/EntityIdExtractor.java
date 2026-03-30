package com.joxette.replay;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Extracts a scalar entity ID from a {@link KafkaMessage} using a configured
 * source/expression pair.
 *
 * <p>Three sources are supported:
 * <dl>
 *   <dt>{@code "key"}</dt>
 *   <dd>The raw message key string. {@code expression} is ignored.</dd>
 *   <dt>{@code "value"}</dt>
 *   <dd>The message value bytes parsed as JSON; {@code expression} is a
 *       JSONPath (e.g. {@code $.order_id}) applied to the parsed document.</dd>
 *   <dt>{@code "header"}</dt>
 *   <dd>The first header whose key equals {@code expression}, decoded as
 *       UTF-8.</dd>
 * </dl>
 *
 * <p>Any extraction failure (null input, parse error, missing path) returns
 * {@link Optional#empty()} rather than throwing, so the pipeline can skip
 * messages that carry no entity ID without aborting the batch.
 */
@Component
public class EntityIdExtractor {

    /**
     * Attempts to extract an entity ID from {@code message} using the given
     * {@code source} discriminant and {@code expression}.
     *
     * @param message    the Kafka message to inspect
     * @param source     one of {@code "key"}, {@code "value"}, {@code "header"}
     * @param expression JSONPath for {@code "value"} source; header name for
     *                   {@code "header"} source; ignored for {@code "key"} source
     * @return the extracted entity ID, or empty if extraction is not possible
     */
    public Optional<String> extract(KafkaMessage message, String source, String expression) {
        return switch (source) {
            case "key"    -> extractFromKey(message.key());
            case "value"  -> extractFromJson(message.value(), expression);
            case "header" -> extractFromHeaders(message.headers(), expression);
            default       -> Optional.empty();
        };
    }

    private Optional<String> extractFromKey(String key) {
        return Optional.ofNullable(key).filter(k -> !k.isBlank());
    }

    private Optional<String> extractFromJson(byte[] value, String expression) {
        if (value == null || value.length == 0 || expression == null) {
            return Optional.empty();
        }
        try {
            Object result = JsonPath.read(new ByteArrayInputStream(value), expression);
            if (result == null) {
                return Optional.empty();
            }
            return Optional.of(result.toString());
        } catch (PathNotFoundException e) {
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private Optional<String> extractFromHeaders(List<KafkaMessage.Header> headers, String headerName) {
        if (headers == null || headerName == null) {
            return Optional.empty();
        }
        return headers.stream()
                .filter(h -> headerName.equals(h.key()))
                .findFirst()
                .map(h -> new String(h.value(), StandardCharsets.UTF_8));
    }
}

package com.joxette.replay;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

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
 * <p>{@link #extract} returns {@link Optional#empty()} both when no id is
 * present (normal) and when extraction threw (a real error) — kept for
 * backward compatibility with callers that only care about the value.
 * {@link #extractDetailed} distinguishes the two via {@link Extraction#failed()}
 * so callers like {@link MessageRouter} can log/alert only on genuine failures.
 */
@Component
public class EntityIdExtractor {

    private static final Logger log = LoggerFactory.getLogger(EntityIdExtractor.class);

    /**
     * Cache of pre-compiled {@link JsonPath} instances, keyed on the raw
     * expression string (e.g. {@code $.order_id}).
     *
     * <p>Compiling a JsonPath expression is pure-syntax work that produces an
     * identical AST for the same string every time — it carries no
     * per-message or per-topic state.  The set of configured expressions is
     * small (typically &lt; 20) and fixed at startup, so the cache never
     * evicts in practice; {@code maximumSize} is a defensive safety cap.
     *
     * <p>{@code recordStats()} adds zero overhead on the hot path and allows
     * the stats to be exposed via Micrometer/Prometheus in the future.
     */
    private final LoadingCache<String, JsonPath> compiledPaths = Caffeine.newBuilder()
            .maximumSize(1_000)
            .recordStats()
            .build(JsonPath::compile);

    /**
     * Outcome of an extraction attempt.
     *
     * <p>{@code failed=true} means the extraction THREW (malformed JSON, JsonPath
     * evaluation error, etc.) — distinct from a normal "no id present" outcome
     * ({@code failed=false, value=Optional.empty()}), which happens whenever the
     * message simply doesn't carry this entity's id (missing key, path not
     * found, absent header). Callers that need to log/alert on genuine
     * extraction errors (see {@link MessageRouter#route}) should branch on
     * {@link #failed()}, not on {@link #value()} being empty.
     */
    public record Extraction(Optional<String> value, boolean failed, String failureReason) {
        static Extraction of(Optional<String> value) { return new Extraction(value, false, null); }
        static Extraction failure(String reason) { return new Extraction(Optional.empty(), true, reason); }
    }

    /**
     * Attempts to extract an entity ID from {@code message} using the given
     * {@code source} discriminant and {@code expression}.
     *
     * @param message    the Kafka message to inspect
     * @param source     where to evaluate the expression
     * @param expression JSONPath for {@code "value"} source; header name for
     *                   {@code "header"} source; ignored for {@code "key"} source
     * @return the extracted entity ID, or empty if no id is present OR extraction failed
     */
    public Optional<String> extract(KafkaMessage message,
                                     com.joxette.management.IdSource source,
                                     String expression) {
        return extractDetailed(message, source, expression).value();
    }

    /**
     * Like {@link #extract}, but distinguishes an extraction failure (an
     * exception thrown while parsing/evaluating) from a normal "no id present"
     * outcome via {@link Extraction#failed()}.
     */
    public Extraction extractDetailed(KafkaMessage message,
                                       com.joxette.management.IdSource source,
                                       String expression) {
        return switch (source) {
            case KEY    -> Extraction.of(extractFromKey(message.key()));
            case VALUE  -> extractFromJsonDetailed(message.value(), expression, message.topic());
            case HEADER -> Extraction.of(extractFromHeaders(message.headers(), expression));
        };
    }

    private Optional<String> extractFromKey(String key) {
        return Optional.ofNullable(key).filter(k -> !k.isBlank());
    }

    private Extraction extractFromJsonDetailed(byte[] value, String expression, String topic) {
        if (value == null || value.length == 0 || expression == null) {
            return Extraction.of(Optional.empty());
        }
        try {
            // compiledPaths.get() returns the cached compiled JsonPath after the
            // first call, avoiding repeated expression parsing on the hot path.
            // Decode to String before passing to JsonPath: avoids the InputStreamReader +
            // json-smart character-by-character reader path, reducing CPU by ~15% per profile.
            JsonPath compiled = compiledPaths.get(expression);
            Object result = compiled.read(new String(value, StandardCharsets.UTF_8));
            if (result == null) {
                return Extraction.of(Optional.empty());
            }
            return Extraction.of(Optional.of(result.toString()));
        } catch (PathNotFoundException e) {
            return Extraction.of(Optional.empty());
        } catch (Exception e) {
            log.warn("EntityIdExtractor: extraction failed for topic '{}', expression '{}': {}",
                    topic, expression, e.toString());
            return Extraction.failure(e.toString());
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

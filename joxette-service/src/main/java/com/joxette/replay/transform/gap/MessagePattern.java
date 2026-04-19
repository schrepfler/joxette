package com.joxette.replay.transform.gap;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.joxette.replay.transform.Predicate;

import java.io.IOException;

/**
 * Identifies a message occurrence by predicate match and a quantifier specifying
 * which occurrence to select.
 *
 * <p>Quantifier serialisation:
 * <ul>
 *   <li>{@code "first"}, {@code "last"}, {@code "any"} — plain string singletons</li>
 *   <li>{@code {"nth": 2}} — object for Nth occurrence</li>
 *   <li>{@code {"first_after": {...}}} — object wrapping a nested MessagePattern</li>
 * </ul>
 *
 * <p>Example — first OrderCreated:
 * <pre>{@code
 * { "predicate": { "field": "$.value.type", "operator": "EQ", "value": "OrderCreated" },
 *   "quantifier": "first" }
 * }</pre>
 *
 * <p>Example — second Payment:
 * <pre>{@code
 * { "predicate": { "field": "$.value.type", "operator": "EQ", "value": "Payment" },
 *   "quantifier": { "nth": 2 } }
 * }</pre>
 *
 * <p>Example — first PaymentSent after first OrderCreated:
 * <pre>{@code
 * { "predicate": { "field": "$.value.type", "operator": "EQ", "value": "PaymentSent" },
 *   "quantifier": { "first_after": { "predicate": {...}, "quantifier": "first" } } }
 * }</pre>
 */
public record MessagePattern(
        @JsonProperty("predicate") Predicate predicate,
        @JsonProperty("quantifier") Quantifier quantifier
) {

    /**
     * Sealed quantifier hierarchy with mixed string/object JSON representation.
     *
     * <p>Singletons ({@link First}, {@link Last}, {@link Any}) serialise as plain JSON
     * strings via {@code @JsonValue}. {@link Nth} and {@link FirstAfter} serialise as
     * objects. Deserialization is handled by {@link QuantifierDeserializer}.
     */
    @JsonDeserialize(using = QuantifierDeserializer.class)
    public sealed interface Quantifier
            permits Quantifier.First, Quantifier.Last, Quantifier.Any,
                    Quantifier.Nth, Quantifier.FirstAfter {

        /** Selects the first matching message. Serialises as {@code "first"}. */
        final class First implements Quantifier {
            public static final First INSTANCE = new First();

            private First() {}

            @JsonValue
            public String jsonValue() { return "first"; }
        }

        /** Selects the last matching message. Serialises as {@code "last"}. */
        final class Last implements Quantifier {
            public static final Last INSTANCE = new Last();

            private Last() {}

            @JsonValue
            public String jsonValue() { return "last"; }
        }

        /** Matches every occurrence. Serialises as {@code "any"}. */
        final class Any implements Quantifier {
            public static final Any INSTANCE = new Any();

            private Any() {}

            @JsonValue
            public String jsonValue() { return "any"; }
        }

        /**
         * Selects the Nth matching message (1-based). Serialises as {@code {"nth": N}}.
         *
         * @param n 1-based occurrence index
         */
        record Nth(@JsonProperty("nth") int n) implements Quantifier {}

        /**
         * Selects the first match occurring after a given anchor pattern is resolved.
         * Serialises as {@code {"first_after": {...}}}.
         *
         * @param after anchor pattern that must resolve before this one is considered
         */
        record FirstAfter(@JsonProperty("first_after") MessagePattern after) implements Quantifier {}
    }

    /**
     * Custom deserializer handling the mixed string/object representation.
     *
     * <ul>
     *   <li>String {@code "first"} → {@link Quantifier.First#INSTANCE}</li>
     *   <li>String {@code "last"}  → {@link Quantifier.Last#INSTANCE}</li>
     *   <li>String {@code "any"}   → {@link Quantifier.Any#INSTANCE}</li>
     *   <li>Object with {@code "nth"} field → {@link Quantifier.Nth}</li>
     *   <li>Object with {@code "first_after"} field → {@link Quantifier.FirstAfter}</li>
     * </ul>
     */
    static final class QuantifierDeserializer extends StdDeserializer<Quantifier> {

        QuantifierDeserializer() {
            super(Quantifier.class);
        }

        @Override
        public Quantifier deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
            JsonNode node = p.getCodec().readTree(p);
            if (node.isTextual()) {
                return switch (node.textValue()) {
                    case "first" -> Quantifier.First.INSTANCE;
                    case "last"  -> Quantifier.Last.INSTANCE;
                    case "any"   -> Quantifier.Any.INSTANCE;
                    default -> throw new IOException("Unknown quantifier string: " + node.textValue());
                };
            }
            if (node.isObject()) {
                if (node.has("nth")) {
                    return new Quantifier.Nth(node.get("nth").intValue());
                }
                if (node.has("first_after")) {
                    MessagePattern after = p.getCodec().treeToValue(node.get("first_after"), MessagePattern.class);
                    return new Quantifier.FirstAfter(after);
                }
                throw new IOException("Unknown quantifier object shape (expected 'nth' or 'first_after'): " + node);
            }
            throw new IOException("Expected string or object for Quantifier, got: " + node.getNodeType());
        }
    }
}

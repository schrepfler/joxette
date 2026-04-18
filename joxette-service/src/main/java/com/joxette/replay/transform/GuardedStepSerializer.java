package com.joxette.replay.transform;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * Jackson serializer for {@link GuardedStep}.
 *
 * <p>Serializes a {@code GuardedStep} by first serializing its delegate step (which
 * picks up the delegate's own {@code @JsonTypeInfo} → adds the {@code "type"} field),
 * then injecting the {@code "when"} predicate into the resulting JSON object.
 *
 * <p>Round-trip example — input:
 * <pre>{@code
 * { "type": "redact", "target": "$.value.email",
 *   "when": { "field": "$.headers[x-env]", "operator": "NEQ", "value": "prod" } }
 * }</pre>
 *
 * <p>Output after deserialization → re-serialization:
 * <pre>{@code
 * { "type": "redact", "target": "$.value.email",
 *   "when": { "match": "leaf", "field": "$.headers[x-env]", "operator": "NEQ", "value": "prod" } }
 * }</pre>
 * (Note: {@code "match": "leaf"} is added by {@link Predicate}'s {@code @JsonTypeInfo} on
 * the way out; it is optional on input due to {@code defaultImpl}.)
 */
public class GuardedStepSerializer extends StdSerializer<GuardedStep> {

    public GuardedStepSerializer() {
        super(GuardedStep.class);
    }

    @Override
    public void serialize(GuardedStep value, JsonGenerator gen, SerializerProvider provider)
            throws IOException {
        ObjectMapper mapper = (ObjectMapper) gen.getCodec();
        // Serialize the delegate step — picks up @JsonTypeInfo and adds "type" field
        ObjectNode node = mapper.valueToTree(value.delegate());
        // Inject the 'when' predicate into the same object
        node.set("when", mapper.valueToTree(value.when()));
        mapper.writeTree(gen, node);
    }
}

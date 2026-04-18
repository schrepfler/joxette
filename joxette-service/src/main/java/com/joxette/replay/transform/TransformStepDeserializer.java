package com.joxette.replay.transform;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joxette.replay.transform.steps.AddComputedFieldStep;
import com.joxette.replay.transform.steps.AddHeaderStep;
import com.joxette.replay.transform.steps.CoalesceStep;
import com.joxette.replay.transform.steps.ConditionalStep;
import com.joxette.replay.transform.steps.CopyFieldStep;
import com.joxette.replay.transform.steps.CopyToHeaderStep;
import com.joxette.replay.transform.steps.DeleteFieldStep;
import com.joxette.replay.transform.steps.FanOutStep;
import com.joxette.replay.transform.steps.FilterDropStep;
import com.joxette.replay.transform.steps.FlattenFieldStep;
import com.joxette.replay.transform.steps.KeyFromValueStep;
import com.joxette.replay.transform.steps.MaskHashStep;
import com.joxette.replay.transform.steps.MergePatchStep;
import com.joxette.replay.transform.steps.NullKeyStep;
import com.joxette.replay.transform.steps.RedactStep;
import com.joxette.replay.transform.steps.RedirectTopicStep;
import com.joxette.replay.transform.steps.RemapKeyStep;
import com.joxette.replay.transform.steps.RemoveHeaderStep;
import com.joxette.replay.transform.steps.RenameFieldStep;
import com.joxette.replay.transform.steps.SetConstantStep;
import com.joxette.replay.transform.steps.TemplateStep;
import com.joxette.replay.transform.steps.TimeCompressStep;
import com.joxette.replay.transform.steps.TimeFreezeStep;
import com.joxette.replay.transform.steps.TimeShiftStep;
import com.joxette.replay.transform.steps.WallTimeStep;

import java.io.IOException;
import java.util.Map;

/**
 * Custom Jackson deserializer for {@link TransformStep}.
 *
 * <p>Provides two capabilities above what {@code @JsonTypeInfo}/{@code @JsonSubTypes}
 * alone could offer:
 *
 * <ol>
 *   <li><b>Per-step {@code when} guard</b> — extracts the optional {@code "when"}
 *       field from any step JSON object and wraps the deserialized step in a
 *       {@link GuardedStep} carrying that {@link Predicate}.</li>
 *   <li><b>Type resolution</b> — maps the {@code "type"} discriminator string to
 *       the concrete step class, replicating the {@code @JsonSubTypes} mapping which
 *       still governs serialization (adding the {@code "type"} property on the way
 *       out).</li>
 * </ol>
 *
 * <p>Registration: applied via {@code @JsonDeserialize(using = TransformStepDeserializer.class)}
 * on the {@link TransformStep} interface. The {@code @JsonTypeInfo} and
 * {@code @JsonSubTypes} annotations on that interface are kept for serialization only.
 *
 * <h2>Recursion</h2>
 * <p>When deserializing a {@link ConditionalStep}, its nested {@code then_steps} and
 * {@code else_steps} lists are typed as {@code List<TransformStep>}. Jackson uses this
 * deserializer for each element, so nested steps inside conditionals also support
 * {@code when} guards.
 */
public class TransformStepDeserializer extends StdDeserializer<TransformStep> {

    /** Maps the {@code "type"} discriminator string to the concrete step class. */
    private static final Map<String, Class<? extends TransformStep>> TYPE_MAP = Map.ofEntries(
            Map.entry("set_constant",       SetConstantStep.class),
            Map.entry("copy_field",         CopyFieldStep.class),
            Map.entry("template",           TemplateStep.class),
            Map.entry("redact",             RedactStep.class),
            Map.entry("mask_hash",          MaskHashStep.class),
            Map.entry("coalesce",           CoalesceStep.class),
            Map.entry("wall_time",          WallTimeStep.class),
            Map.entry("time_shift",         TimeShiftStep.class),
            Map.entry("time_compress",      TimeCompressStep.class),
            Map.entry("time_freeze",        TimeFreezeStep.class),
            Map.entry("rename_field",       RenameFieldStep.class),
            Map.entry("delete_field",       DeleteFieldStep.class),
            Map.entry("flatten_field",      FlattenFieldStep.class),
            Map.entry("add_computed_field", AddComputedFieldStep.class),
            Map.entry("merge_patch",        MergePatchStep.class),
            Map.entry("remap_key",          RemapKeyStep.class),
            Map.entry("null_key",           NullKeyStep.class),
            Map.entry("key_from_value",     KeyFromValueStep.class),
            Map.entry("add_header",         AddHeaderStep.class),
            Map.entry("remove_header",      RemoveHeaderStep.class),
            Map.entry("copy_to_header",     CopyToHeaderStep.class),
            Map.entry("redirect_topic",     RedirectTopicStep.class),
            Map.entry("fan_out",            FanOutStep.class),
            Map.entry("filter_drop",        FilterDropStep.class),
            Map.entry("conditional",        ConditionalStep.class)
    );

    public TransformStepDeserializer() {
        super(TransformStep.class);
    }

    @Override
    public TransformStep deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectNode node = p.readValueAsTree();

        // Peek at the 'when' guard before delegating to the concrete type.
        // We remove it from the node so concrete step classes don't see an unknown property
        // (this avoids FAIL_ON_UNKNOWN_PROPERTIES errors on strictly-configured mappers).
        JsonNode whenNode = node.remove("when");

        // Resolve the concrete step class from the 'type' discriminator
        JsonNode typeNode = node.get("type");
        if (typeNode == null || typeNode.isNull()) {
            throw ctxt.instantiationException(TransformStep.class,
                    "Missing required 'type' discriminator field in transform step");
        }
        String type = typeNode.asText();
        Class<? extends TransformStep> concreteClass = TYPE_MAP.get(type);
        if (concreteClass == null) {
            throw ctxt.instantiationException(TransformStep.class,
                    "Unknown transform step type: '" + type + "'");
        }

        // Deserialize the concrete step. The 'type' field remains in the node but concrete
        // classes don't declare a 'type' component/property, so it is silently ignored.
        TransformStep step = p.getCodec().treeToValue(node, concreteClass);

        // Wrap in GuardedStep when a 'when' predicate was present
        if (whenNode != null && !whenNode.isNull() && !whenNode.isMissingNode()) {
            Predicate guard = p.getCodec().treeToValue(whenNode, Predicate.class);
            return new GuardedStep(guard, step);
        }
        return step;
    }
}

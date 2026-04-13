package com.joxette.replay.transform;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
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

/**
 * Sealed marker interface for all transformation pipeline steps.
 *
 * <p>Jackson deserialises the {@code "type"} discriminator field to select the
 * concrete implementation. All implementations live in the
 * {@code com.joxette.replay.transform.steps} package and are registered via
 * {@link JsonSubTypes} below.
 *
 * <p>A pipeline is a {@code List<TransformStep>} applied in order to each
 * {@link ReplayMessage} by {@link TransformPipeline}. Steps are stateless
 * per-message — no cross-message aggregation at this stage.
 */
@JsonTypeInfo(
    use     = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = SetConstantStep.class,      name = "set_constant"),
    @JsonSubTypes.Type(value = CopyFieldStep.class,        name = "copy_field"),
    @JsonSubTypes.Type(value = TemplateStep.class,         name = "template"),
    @JsonSubTypes.Type(value = RedactStep.class,           name = "redact"),
    @JsonSubTypes.Type(value = MaskHashStep.class,         name = "mask_hash"),
    @JsonSubTypes.Type(value = CoalesceStep.class,         name = "coalesce"),
    @JsonSubTypes.Type(value = WallTimeStep.class,         name = "wall_time"),
    @JsonSubTypes.Type(value = TimeShiftStep.class,        name = "time_shift"),
    @JsonSubTypes.Type(value = TimeCompressStep.class,     name = "time_compress"),
    @JsonSubTypes.Type(value = TimeFreezeStep.class,       name = "time_freeze"),
    @JsonSubTypes.Type(value = RenameFieldStep.class,      name = "rename_field"),
    @JsonSubTypes.Type(value = DeleteFieldStep.class,      name = "delete_field"),
    @JsonSubTypes.Type(value = FlattenFieldStep.class,     name = "flatten_field"),
    @JsonSubTypes.Type(value = AddComputedFieldStep.class, name = "add_computed_field"),
    @JsonSubTypes.Type(value = MergePatchStep.class,       name = "merge_patch"),
    @JsonSubTypes.Type(value = RemapKeyStep.class,         name = "remap_key"),
    @JsonSubTypes.Type(value = NullKeyStep.class,          name = "null_key"),
    @JsonSubTypes.Type(value = KeyFromValueStep.class,     name = "key_from_value"),
    @JsonSubTypes.Type(value = AddHeaderStep.class,        name = "add_header"),
    @JsonSubTypes.Type(value = RemoveHeaderStep.class,     name = "remove_header"),
    @JsonSubTypes.Type(value = CopyToHeaderStep.class,     name = "copy_to_header"),
    @JsonSubTypes.Type(value = RedirectTopicStep.class,    name = "redirect_topic"),
    @JsonSubTypes.Type(value = FanOutStep.class,           name = "fan_out"),
    @JsonSubTypes.Type(value = FilterDropStep.class,       name = "filter_drop"),
    @JsonSubTypes.Type(value = ConditionalStep.class,      name = "conditional")
})
public interface TransformStep {
}

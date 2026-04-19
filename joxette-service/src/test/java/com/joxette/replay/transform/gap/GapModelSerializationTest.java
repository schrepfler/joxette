package com.joxette.replay.transform.gap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.replay.transform.Predicate;
import com.joxette.replay.transform.TransformStep;
import com.joxette.replay.transform.TransformStepJacksonModule;
import com.joxette.replay.transform.gap.MessagePattern.Quantifier;
import com.joxette.replay.transform.steps.GapTransformStep;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Jackson round-trip tests for all types in the {@code gap} package and
 * {@link GapTransformStep}. Verifies serialise → deserialise identity for every
 * quantifier variant, every operation variant, and the composite types.
 */
class GapModelSerializationTest {

    private static final ObjectMapper OM = new ObjectMapper()
            .registerModule(new TransformStepJacksonModule());

    private static final Predicate ORDER_CREATED = new Predicate.Leaf(
            "$.value.type", Predicate.Operator.EQ, "OrderCreated");

    private static final MessagePattern FIRST_ORDER_CREATED = new MessagePattern(
            ORDER_CREATED, Quantifier.First.INSTANCE);

    // =========================================================================
    // Quantifier round-trips
    // =========================================================================

    static Stream<Arguments> quantifierVariants() {
        return Stream.of(
                Arguments.of("first",      Quantifier.First.INSTANCE),
                Arguments.of("last",       Quantifier.Last.INSTANCE),
                Arguments.of("any",        Quantifier.Any.INSTANCE),
                Arguments.of("nth(2)",     new Quantifier.Nth(2)),
                Arguments.of("nth(1)",     new Quantifier.Nth(1)),
                Arguments.of("first_after",
                        new Quantifier.FirstAfter(FIRST_ORDER_CREATED))
        );
    }

    @ParameterizedTest(name = "Quantifier round-trip: {0}")
    @MethodSource("quantifierVariants")
    void quantifier_roundTrip(String label, Quantifier quantifier) throws Exception {
        MessagePattern original = new MessagePattern(ORDER_CREATED, quantifier);
        String json = OM.writeValueAsString(original);
        MessagePattern restored = OM.readValue(json, MessagePattern.class);

        assertThat(restored.predicate()).isInstanceOf(Predicate.Leaf.class);
        assertThat(restored.quantifier()).isEqualTo(quantifier);
    }

    @Test
    void quantifier_first_serialisesAsString() throws Exception {
        MessagePattern p = new MessagePattern(ORDER_CREATED, Quantifier.First.INSTANCE);
        String json = OM.writeValueAsString(p);
        assertThat(json).contains("\"quantifier\":\"first\"");
    }

    @Test
    void quantifier_last_serialisesAsString() throws Exception {
        MessagePattern p = new MessagePattern(ORDER_CREATED, Quantifier.Last.INSTANCE);
        String json = OM.writeValueAsString(p);
        assertThat(json).contains("\"quantifier\":\"last\"");
    }

    @Test
    void quantifier_any_serialisesAsString() throws Exception {
        MessagePattern p = new MessagePattern(ORDER_CREATED, Quantifier.Any.INSTANCE);
        String json = OM.writeValueAsString(p);
        assertThat(json).contains("\"quantifier\":\"any\"");
    }

    @Test
    void quantifier_nth_serialisesAsObject() throws Exception {
        MessagePattern p = new MessagePattern(ORDER_CREATED, new Quantifier.Nth(3));
        String json = OM.writeValueAsString(p);
        assertThat(json).contains("\"nth\":3");
    }

    @Test
    void quantifier_firstAfter_serialisesAsObject() throws Exception {
        MessagePattern p = new MessagePattern(ORDER_CREATED,
                new Quantifier.FirstAfter(FIRST_ORDER_CREATED));
        String json = OM.writeValueAsString(p);
        assertThat(json).contains("\"first_after\"");
    }

    @Test
    void quantifier_unknownString_throws() {
        String json = "{\"predicate\":{\"field\":\"$.value.type\",\"operator\":\"EQ\",\"value\":\"X\"},"
                + "\"quantifier\":\"bogus\"}";
        assertThatThrownBy(() -> OM.readValue(json, MessagePattern.class))
                .hasMessageContaining("bogus");
    }

    // =========================================================================
    // GapOperation round-trips
    // =========================================================================

    static Stream<Arguments> operationVariants() {
        return Stream.of(
                Arguments.of("cut",           new GapOperation.Cut()),
                Arguments.of("hold",          new GapOperation.Hold(500L)),
                Arguments.of("trim_by_ms",    new GapOperation.Trim(2000L, null)),
                Arguments.of("trim_by_factor",new GapOperation.Trim(null, 0.5)),
                Arguments.of("pad",           new GapOperation.Pad(1000L)),
                Arguments.of("scale",         new GapOperation.Scale(0.1))
        );
    }

    @ParameterizedTest(name = "GapOperation round-trip: {0}")
    @MethodSource("operationVariants")
    void gapOperation_roundTrip(String label, GapOperation operation) throws Exception {
        String json = OM.writeValueAsString(operation);
        GapOperation restored = OM.readValue(json, GapOperation.class);
        assertThat(restored).isEqualTo(operation);
    }

    @Test
    void gapOperation_cut_hasNoExtraFields() throws Exception {
        String json = OM.writeValueAsString(new GapOperation.Cut());
        assertThat(json).isEqualTo("{\"op\":\"cut\"}");
    }

    @Test
    void gapOperation_hold_serialisesTargetMs() throws Exception {
        String json = OM.writeValueAsString(new GapOperation.Hold(750L));
        assertThat(json).contains("\"op\":\"hold\"");
        assertThat(json).contains("\"target_ms\":750");
    }

    @Test
    void gapOperation_trim_byMs_omitsByFactor() throws Exception {
        String json = OM.writeValueAsString(new GapOperation.Trim(2000L, null));
        assertThat(json).contains("\"by_ms\":2000");
        assertThat(json).doesNotContain("\"by_factor\"");
    }

    @Test
    void gapOperation_trim_byFactor_omitsByMs() throws Exception {
        String json = OM.writeValueAsString(new GapOperation.Trim(null, 0.25));
        assertThat(json).contains("\"by_factor\":0.25");
        assertThat(json).doesNotContain("\"by_ms\"");
    }

    @Test
    void gapOperation_trim_bothSet_throws() {
        assertThatThrownBy(() -> new GapOperation.Trim(100L, 0.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
    }

    @Test
    void gapOperation_trim_neitherSet_throws() {
        assertThatThrownBy(() -> new GapOperation.Trim(null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
    }

    // =========================================================================
    // GapSelector round-trips
    // =========================================================================

    @Test
    void gapSelector_after_roundTrip() throws Exception {
        GapSelector sel = new GapSelector(FIRST_ORDER_CREATED, null, null, 3000L, null);
        String json = OM.writeValueAsString(sel);
        GapSelector restored = OM.readValue(json, GapSelector.class);
        assertThat(restored.after().quantifier()).isInstanceOf(Quantifier.First.class);
        assertThat(restored.minDurationMs()).isEqualTo(3000L);
    }

    @Test
    void gapSelector_withinFragment_roundTrip() throws Exception {
        GapSelector sel = new GapSelector(null, null, "checkout", null, null);
        String json = OM.writeValueAsString(sel);
        GapSelector restored = OM.readValue(json, GapSelector.class);
        assertThat(restored.withinFragment()).isEqualTo("checkout");
    }

    @Test
    void gapSelector_noConstraint_throws() {
        assertThatThrownBy(() -> new GapSelector(null, null, null, 3000L, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one");
    }

    @Test
    void gapSelector_afterAndBefore_roundTrip() throws Exception {
        Predicate paymentPred = new Predicate.Leaf("$.value.type", Predicate.Operator.EQ, "PaymentSent");
        MessagePattern beforePat = new MessagePattern(paymentPred, Quantifier.Last.INSTANCE);
        GapSelector sel = new GapSelector(FIRST_ORDER_CREATED, beforePat, null, null, null);
        String json = OM.writeValueAsString(sel);
        GapSelector restored = OM.readValue(json, GapSelector.class);
        assertThat(restored.after()).isNotNull();
        assertThat(restored.before()).isNotNull();
    }

    // =========================================================================
    // FragmentDefinition round-trips
    // =========================================================================

    @Test
    void fragmentDefinition_withIfClause_roundTrip() throws Exception {
        Predicate paymentCompleted = new Predicate.Leaf(
                "$.value.type", Predicate.Operator.EQ, "PaymentCompleted");
        MessagePattern toPattern = new MessagePattern(paymentCompleted, Quantifier.First.INSTANCE);
        FragmentDefinition.IfClause ifClause = new FragmentDefinition.IfClause(null, 30000L);
        FragmentDefinition frag = new FragmentDefinition(
                "checkout", "Checkout Phase", "#4f8ef7",
                FIRST_ORDER_CREATED, toPattern, ifClause);

        String json = OM.writeValueAsString(frag);
        FragmentDefinition restored = OM.readValue(json, FragmentDefinition.class);

        assertThat(restored.name()).isEqualTo("checkout");
        assertThat(restored.label()).isEqualTo("Checkout Phase");
        assertThat(restored.color()).isEqualTo("#4f8ef7");
        assertThat(restored.from().quantifier()).isInstanceOf(Quantifier.First.class);
        assertThat(restored.to().quantifier()).isInstanceOf(Quantifier.First.class);
        assertThat(restored.ifClause()).isNotNull();
        assertThat(restored.ifClause().maxDurationMs()).isEqualTo(30000L);
        assertThat(restored.ifClause().minDurationMs()).isNull();
    }

    @Test
    void fragmentDefinition_withoutIfClause_roundTrip() throws Exception {
        Predicate shipped = new Predicate.Leaf("$.value.type", Predicate.Operator.EQ, "Shipped");
        MessagePattern toPattern = new MessagePattern(shipped, Quantifier.Last.INSTANCE);
        FragmentDefinition frag = new FragmentDefinition(
                "fulfilment", "Fulfilment", "#aabbcc",
                FIRST_ORDER_CREATED, toPattern, null);

        String json = OM.writeValueAsString(frag);
        FragmentDefinition restored = OM.readValue(json, FragmentDefinition.class);

        assertThat(restored.ifClause()).isNull();
        assertThat(json).doesNotContain("\"if\"");
    }

    @Test
    void fragmentDefinition_ifClause_serialisedAsIf() throws Exception {
        FragmentDefinition.IfClause ifClause = new FragmentDefinition.IfClause(1000L, 30000L);
        Predicate done = new Predicate.Leaf("$.value.type", Predicate.Operator.EQ, "Done");
        FragmentDefinition frag = new FragmentDefinition(
                "f", "F", "#fff",
                FIRST_ORDER_CREATED,
                new MessagePattern(done, Quantifier.First.INSTANCE),
                ifClause);

        String json = OM.writeValueAsString(frag);
        assertThat(json).contains("\"if\":");
        assertThat(json).contains("\"min_duration_ms\":1000");
        assertThat(json).contains("\"max_duration_ms\":30000");
    }

    // =========================================================================
    // GapTransformStep round-trip via TransformStep polymorphism
    // =========================================================================

    @Test
    void gapTransformStep_roundTrip_viaTransformStep() throws Exception {
        GapSelector sel = new GapSelector(FIRST_ORDER_CREATED, null, null, 3000L, null);
        GapOperation op = new GapOperation.Hold(500L);
        GapTransformStep step = new GapTransformStep(sel, op);

        String json = OM.writeValueAsString((TransformStep) step);
        assertThat(json).contains("\"type\":\"gap_transform\"");

        TransformStep restored = OM.readValue(json, TransformStep.class);
        assertThat(restored).isInstanceOf(GapTransformStep.class);
        GapTransformStep r = (GapTransformStep) restored;
        assertThat(r.select().minDurationMs()).isEqualTo(3000L);
        assertThat(r.operation()).isInstanceOf(GapOperation.Hold.class);
        assertThat(((GapOperation.Hold) r.operation()).targetMs()).isEqualTo(500L);
    }

    @ParameterizedTest(name = "GapTransformStep with operation {0} via TransformStep")
    @MethodSource("operationVariants")
    void gapTransformStep_allOperations_roundTrip(String label, GapOperation operation) throws Exception {
        GapSelector sel = new GapSelector(null, null, "checkout", null, null);
        GapTransformStep step = new GapTransformStep(sel, operation);

        String json = OM.writeValueAsString((TransformStep) step);
        TransformStep restored = OM.readValue(json, TransformStep.class);

        assertThat(restored).isInstanceOf(GapTransformStep.class);
        assertThat(((GapTransformStep) restored).operation()).isEqualTo(operation);
    }

    @Test
    void gapTransformStep_scale_roundTrip_inList() throws Exception {
        GapSelector sel = new GapSelector(null, null, "checkout", null, null);
        GapOperation op = new GapOperation.Scale(0.1);
        List<TransformStep> steps = List.of(new GapTransformStep(sel, op));

        // Use a typed writer so Jackson serialises with @JsonTypeInfo discriminators
        String json = OM.writerFor(
                        OM.getTypeFactory().constructCollectionType(List.class, TransformStep.class))
                .writeValueAsString(steps);
        List<TransformStep> restored = OM.readValue(json,
                OM.getTypeFactory().constructCollectionType(List.class, TransformStep.class));

        assertThat(restored).hasSize(1);
        GapTransformStep r = (GapTransformStep) restored.getFirst();
        assertThat(((GapOperation.Scale) r.operation()).factor()).isEqualTo(0.1);
    }
}

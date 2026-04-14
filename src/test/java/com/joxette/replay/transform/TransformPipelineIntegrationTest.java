package com.joxette.replay.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.steps.AddComputedFieldStep;
import com.joxette.replay.transform.steps.AddHeaderStep;
import com.joxette.replay.transform.steps.ConditionalStep;
import com.joxette.replay.transform.steps.FanOutStep;
import com.joxette.replay.transform.steps.FilterDropStep;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static com.joxette.replay.transform.Predicate.Operator.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link TransformPipeline} end-to-end execution.
 *
 * <p>Covers multi-step ordering, {@code filter_drop} (first step, mid-pipeline),
 * {@code fan_out} (expansion + downstream steps), {@code conditional} (correct
 * branch selection, nested recursion), {@code when} guards mid-pipeline, compound
 * predicates (AND / OR / NOT), and {@link TransformContext} state
 * ({@code REPLAY_SEQUENCE}, {@code x-replay-id} provenance header).
 *
 * <p>No DuckDB is required — {@link ReplayMessage}s are constructed directly from
 * {@link CassetteRecord}s and passed through the pipeline via
 * {@link TransformPipeline#apply(ReplayMessage, String)}.
 */
class TransformPipelineIntegrationTest {

    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DEC = Base64.getUrlDecoder();
    private static final ObjectMapper   OM  = new ObjectMapper();

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String b64(String s) {
        return ENC.encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String b64Value) {
        return new String(DEC.decode(b64Value), StandardCharsets.UTF_8);
    }

    private static ReplayMessage msg(int partition, String topic, String b64Value) {
        Instant ts = Instant.parse("2024-06-01T12:00:00Z");
        // key is null: steps that use JsonStepHelper.buildWrapper (e.g. AddComputedFieldStep)
        // decode the key as base64url; a non-null placeholder like "k" is not valid base64url
        // and would cause the step to silently no-op.
        return new ReplayMessage(
                new CassetteRecord(topic, partition, 0L, ts, ts, null, b64Value, null, null));
    }

    private static ReplayMessage msg(String topic) {
        return msg(0, topic, null);
    }

    // =========================================================================
    // Multi-step ordering
    // =========================================================================

    @Test
    void multiStep_pipelineAppliesAllFiveStepsInOrder() {
        List<TransformStep> steps = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            steps.add(new AddHeaderStep("x-step-" + i, "value-" + i, false));
        }
        var pipeline = new TransformPipeline(steps, null);

        List<ReplayMessage> result = pipeline.apply(msg("orders"), "rid");

        assertThat(result).hasSize(1);
        List<CassetteRecord.Header> headers = result.get(0).headers;
        assertThat(headers).hasSize(5);
        for (int i = 1; i <= 5; i++) {
            assertThat(headers.get(i - 1).key()).isEqualTo("x-step-" + i);
            assertThat(headers.get(i - 1).value()).isEqualTo("value-" + i);
        }
    }

    // =========================================================================
    // filter_drop
    // =========================================================================

    @Test
    void filterDrop_asFirstStep_returnsEmptyAndSkipsRemainingSteps() {
        List<TransformStep> steps = List.of(
                new FilterDropStep(new Predicate.Leaf("$.partition", EQ, 0)),
                new AddHeaderStep("x-should-not", "appear", false),
                new AddHeaderStep("x-also-not", "appear", false)
        );
        var pipeline = new TransformPipeline(steps, null);

        assertThat(pipeline.apply(msg(0, "orders", null), "rid")).isEmpty();
    }

    @Test
    void filterDrop_midPipeline_dropsAndSkipsSubsequentSteps() {
        // Step 1 runs (adds header), step 2 drops the message, step 3 never runs.
        List<TransformStep> steps = List.of(
                new AddHeaderStep("x-before-drop", "yes", false),
                new FilterDropStep(new Predicate.Leaf("$.partition", EQ, 0)),
                new AddHeaderStep("x-after-drop", "should-not-appear", false)
        );
        var pipeline = new TransformPipeline(steps, null);

        assertThat(pipeline.apply(msg(0, "orders", null), "rid")).isEmpty();
    }

    // =========================================================================
    // fan_out
    // =========================================================================

    @Test
    void fanOut_withThreeTopics_returnsThreeCopies() {
        var pipeline = new TransformPipeline(
                List.of(new FanOutStep(List.of("topic-a", "topic-b", "topic-c"))), null);

        List<ReplayMessage> result = pipeline.apply(msg("original-topic"), "rid");

        assertThat(result).hasSize(3);
        assertThat(result).extracting(m -> m.topic)
                .containsExactly("topic-a", "topic-b", "topic-c");
    }

    @Test
    void fanOut_followedByStep_appliesStepIndependentlyToEachCopy() {
        List<TransformStep> steps = List.of(
                new FanOutStep(List.of("topic-a", "topic-b", "topic-c")),
                new AddHeaderStep("x-post-fanout", "tagged", false)
        );
        var pipeline = new TransformPipeline(steps, null);

        List<ReplayMessage> result = pipeline.apply(msg("original"), "rid");

        assertThat(result).hasSize(3);
        assertThat(result).allSatisfy(m ->
                assertThat(m.headers)
                        .anyMatch(h -> "x-post-fanout".equals(h.key())
                                    && "tagged".equals(h.value())));
    }

    // =========================================================================
    // conditional
    // =========================================================================

    @Test
    void conditional_executesCorrectBranchOnly() {
        var condition = new Predicate.Leaf("$.topic", EQ, "orders");
        var thenStep  = new AddHeaderStep("x-branch", "then", false);
        var elseStep  = new AddHeaderStep("x-branch", "else", false);
        var step      = new ConditionalStep(condition, List.of(thenStep), List.of(elseStep));
        var pipeline  = new TransformPipeline(List.of(step), null);

        // Topic matches — then branch executes, else does not
        List<ReplayMessage> forOrders = pipeline.apply(msg("orders"), "rid");
        assertThat(forOrders).hasSize(1);
        assertThat(forOrders.get(0).headers)
                .anyMatch(h -> "x-branch".equals(h.key()) && "then".equals(h.value()));
        assertThat(forOrders.get(0).headers).noneMatch(h -> "else".equals(h.value()));

        // Topic doesn't match — else branch executes, then does not
        List<ReplayMessage> forPayments = pipeline.apply(msg("payments"), "rid");
        assertThat(forPayments).hasSize(1);
        assertThat(forPayments.get(0).headers)
                .anyMatch(h -> "x-branch".equals(h.key()) && "else".equals(h.value()));
        assertThat(forPayments.get(0).headers).noneMatch(h -> "then".equals(h.value()));
    }

    @Test
    void conditional_nestedConditional_recursesCorrectly() {
        // Inner conditional: partition == 0 → add x-inner=yes
        var innerCond = new Predicate.Leaf("$.partition", EQ, 0);
        var innerStep = new AddHeaderStep("x-inner", "yes", false);
        var inner     = new ConditionalStep(innerCond, List.of(innerStep), List.of());

        // Outer conditional: topic == "orders" → run inner conditional
        var outerCond = new Predicate.Leaf("$.topic", EQ, "orders");
        var outer     = new ConditionalStep(outerCond, List.of(inner), List.of());
        var pipeline  = new TransformPipeline(List.of(outer), null);

        // topic=orders, partition=0 → outer true → inner true → x-inner added
        List<ReplayMessage> r1 = pipeline.apply(msg(0, "orders", null), "rid");
        assertThat(r1).hasSize(1);
        assertThat(r1.get(0).headers)
                .anyMatch(h -> "x-inner".equals(h.key()) && "yes".equals(h.value()));

        // topic=orders, partition=1 → outer true → inner false → x-inner absent
        List<ReplayMessage> r2 = pipeline.apply(msg(1, "orders", null), "rid");
        assertThat(r2).hasSize(1);
        assertThat(r2.get(0).headers).noneMatch(h -> "x-inner".equals(h.key()));
    }

    // =========================================================================
    // when guard
    // =========================================================================

    @Test
    void whenGuard_skipsStepMidPipelineWithoutAffectingSurroundingSteps() {
        // Guard requires partition==99; test message has partition=0, so guard won't match.
        List<TransformStep> steps = List.of(
                new AddHeaderStep("x-step1", "yes", false),
                new GuardedStep(
                        new Predicate.Leaf("$.partition", EQ, 99),
                        new AddHeaderStep("x-step2", "should-not-appear", false)),
                new AddHeaderStep("x-step3", "yes", false)
        );
        var pipeline = new TransformPipeline(steps, null);

        List<ReplayMessage> result = pipeline.apply(msg(0, "orders", null), "rid");

        assertThat(result).hasSize(1);
        List<CassetteRecord.Header> headers = result.get(0).headers;
        assertThat(headers).anyMatch(h -> "x-step1".equals(h.key()) && "yes".equals(h.value()));
        assertThat(headers).anyMatch(h -> "x-step3".equals(h.key()) && "yes".equals(h.value()));
        assertThat(headers).noneMatch(h -> "x-step2".equals(h.key()));
    }

    // =========================================================================
    // Compound predicates
    // =========================================================================

    @Test
    void compoundAnd_filterDropRequiresBothConditions() {
        // Drops only when partition==0 AND topic=="orders"
        var predicate = new Predicate.And(List.of(
                new Predicate.Leaf("$.partition", EQ, 0),
                new Predicate.Leaf("$.topic", EQ, "orders")));
        var pipeline = new TransformPipeline(
                List.of(new FilterDropStep(predicate)), null);

        // Both conditions match → dropped
        assertThat(pipeline.apply(msg(0, "orders", null), "rid")).isEmpty();

        // Only partition matches → passes through
        assertThat(pipeline.apply(msg(0, "payments", null), "rid")).hasSize(1);

        // Only topic matches → passes through
        assertThat(pipeline.apply(msg(1, "orders", null), "rid")).hasSize(1);
    }

    @Test
    void compoundOr_whenGuardRunsStepIfEitherMatches() {
        // Guard fires when partition==0 OR topic=="priority"
        var orPredicate = new Predicate.Or(List.of(
                new Predicate.Leaf("$.partition", EQ, 0),
                new Predicate.Leaf("$.topic", EQ, "priority")));
        var guardedStep = new GuardedStep(
                orPredicate, new AddHeaderStep("x-tagged", "yes", false));
        var pipeline = new TransformPipeline(List.of(guardedStep), null);

        // Partition 0 satisfies first condition → step runs
        List<ReplayMessage> r1 = pipeline.apply(msg(0, "other", null), "rid");
        assertThat(r1.get(0).headers)
                .anyMatch(h -> "x-tagged".equals(h.key()) && "yes".equals(h.value()));

        // Topic "priority" satisfies second condition → step runs
        List<ReplayMessage> r2 = pipeline.apply(msg(5, "priority", null), "rid");
        assertThat(r2.get(0).headers)
                .anyMatch(h -> "x-tagged".equals(h.key()) && "yes".equals(h.value()));

        // Neither condition matches → step skipped, header absent
        List<ReplayMessage> r3 = pipeline.apply(msg(5, "other", null), "rid");
        assertThat(r3.get(0).headers).noneMatch(h -> "x-tagged".equals(h.key()));
    }

    @Test
    void compoundNot_conditionalRoutesToElseBranchWhenConditionNegated() {
        // Condition: NOT(topic=="orders") — true for any topic that is not "orders"
        var notCond  = new Predicate.Not(new Predicate.Leaf("$.topic", EQ, "orders"));
        var thenStep = new AddHeaderStep("x-branch", "non-orders", false);
        var elseStep = new AddHeaderStep("x-branch", "orders", false);
        var step     = new ConditionalStep(notCond, List.of(thenStep), List.of(elseStep));
        var pipeline = new TransformPipeline(List.of(step), null);

        // topic="payments" → NOT(EQ orders) → true → then branch
        List<ReplayMessage> r1 = pipeline.apply(msg("payments"), "rid");
        assertThat(r1.get(0).headers)
                .anyMatch(h -> "x-branch".equals(h.key()) && "non-orders".equals(h.value()));

        // topic="orders" → NOT(EQ orders) → false → else branch
        List<ReplayMessage> r2 = pipeline.apply(msg("orders"), "rid");
        assertThat(r2.get(0).headers)
                .anyMatch(h -> "x-branch".equals(h.key()) && "orders".equals(h.value()));
    }

    // =========================================================================
    // TransformContext — REPLAY_SEQUENCE and replayId
    // =========================================================================

    @Test
    void replaySequence_incrementsCorrectlyAcrossMessagesInSession() throws Exception {
        var step     = new AddComputedFieldStep("$.value.seq", "REPLAY_SEQUENCE");
        var pipeline = new TransformPipeline(List.of(step), null);

        for (int expected = 0; expected < 3; expected++) {
            List<ReplayMessage> result = pipeline.apply(msg(0, "t", b64("{}")), "rid");
            assertThat(result).hasSize(1);
            long actualSeq = OM.readTree(decode(result.get(0).value)).get("seq").asLong();
            assertThat(actualSeq).isEqualTo(expected);
        }
    }

    @Test
    void replayId_isInjectedConsistentlyAcrossAllMessages() {
        var injector = new ReplayMetadataInjector();
        var pipeline = new TransformPipeline(List.of(), injector);
        String replayId = "test-session-abc-123";

        for (int i = 0; i < 3; i++) {
            List<ReplayMessage> result = pipeline.apply(msg("orders"), replayId);
            assertThat(result).hasSize(1);
            assertThat(result.get(0).headers)
                    .anyMatch(h -> "x-replay-id".equals(h.key()) && replayId.equals(h.value()));
        }
    }
}

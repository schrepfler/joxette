package com.joxette.replay.transform.steps;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformPipeline;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

import static com.joxette.replay.transform.steps.FilterDropStep.Operator.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ConditionalStep}:
 * <ul>
 *   <li>Condition true path — then_steps applied</li>
 *   <li>Condition false path — else_steps applied (or pass-through when absent)</li>
 *   <li>Nested conditional inside then/else</li>
 *   <li>filter_drop inside a branch drops the message</li>
 *   <li>$.headers[key] extraction in condition</li>
 * </ul>
 */
class ConditionalStepTest {

    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();

    private static String b64(String s) {
        return ENC.encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static ReplayMessage msg(String topic) {
        Instant ts = Instant.parse("2024-06-01T12:00:00Z");
        CassetteRecord r = new CassetteRecord(topic, 0, 0L, ts, ts, "k", null, null, null);
        return new ReplayMessage(r);
    }

    private static ReplayMessage msgWithValue(String topic, String json) {
        Instant ts = Instant.parse("2024-06-01T12:00:00Z");
        CassetteRecord r = new CassetteRecord(topic, 0, 0L, ts, ts, "k", b64(json), null, null);
        return new ReplayMessage(r);
    }

    private static ReplayMessage msgWithHeader(String headerKey, String headerValue) {
        Instant ts = Instant.parse("2024-06-01T12:00:00Z");
        CassetteRecord r = new CassetteRecord("t", 0, 0L, ts, ts, "k", null,
                List.of(new CassetteRecord.Header(headerKey, headerValue)), null);
        return new ReplayMessage(r);
    }

    /** Runs a single-step pipeline with no injector. */
    private static List<ReplayMessage> apply(ConditionalStep step, ReplayMessage m) {
        return new TransformPipeline(List.of(step), null).apply(m, "rid");
    }

    // =========================================================================
    // Condition true — then_steps applied
    // =========================================================================

    @Test
    void conditionTrue_thenStepsApplied() {
        // Condition: topic EQ "orders" → add header x-routed=yes
        var condition  = new FilterDropStep("$.topic", EQ, "orders");
        var thenHeader = new AddHeaderStep("x-routed", "yes", false);
        var step       = new ConditionalStep(condition, List.of(thenHeader), List.of());

        List<ReplayMessage> result = apply(step, msg("orders"));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers)
                .anyMatch(h -> "x-routed".equals(h.key()) && "yes".equals(h.value()));
    }

    @Test
    void conditionTrue_elseStepsNotApplied() {
        var condition  = new FilterDropStep("$.topic", EQ, "orders");
        var thenHeader = new AddHeaderStep("x-branch", "then", false);
        var elseHeader = new AddHeaderStep("x-branch", "else", false);
        var step       = new ConditionalStep(condition, List.of(thenHeader), List.of(elseHeader));

        List<ReplayMessage> result = apply(step, msg("orders"));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers).hasSize(1);
        assertThat(result.get(0).headers.get(0).value()).isEqualTo("then");
    }

    // =========================================================================
    // Condition false — else_steps applied
    // =========================================================================

    @Test
    void conditionFalse_elseStepsApplied() {
        var condition  = new FilterDropStep("$.topic", EQ, "orders");
        var thenHeader = new AddHeaderStep("x-branch", "then", false);
        var elseHeader = new AddHeaderStep("x-branch", "else", false);
        var step       = new ConditionalStep(condition, List.of(thenHeader), List.of(elseHeader));

        List<ReplayMessage> result = apply(step, msg("payments"));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers).hasSize(1);
        assertThat(result.get(0).headers.get(0).value()).isEqualTo("else");
    }

    @Test
    void conditionFalse_noElseSteps_messagePasses() {
        // No else_steps → message passes through unchanged
        var condition  = new FilterDropStep("$.topic", EQ, "orders");
        var thenHeader = new AddHeaderStep("x-routed", "yes", false);
        var step       = new ConditionalStep(condition, List.of(thenHeader), List.of());

        List<ReplayMessage> result = apply(step, msg("payments"));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers).isEmpty();
    }

    // =========================================================================
    // Nested conditional
    // =========================================================================

    @Test
    void nestedConditional_innerThenExecutes() {
        // Outer condition: topic EQ "orders"
        //   Inner condition: partition EQ 0  → add x-inner=yes
        var innerCondition   = new FilterDropStep("$.partition", EQ, 0);
        var innerThenHeader  = new AddHeaderStep("x-inner", "yes", false);
        var innerConditional = new ConditionalStep(innerCondition, List.of(innerThenHeader), List.of());

        var outerCondition   = new FilterDropStep("$.topic", EQ, "orders");
        var step             = new ConditionalStep(outerCondition, List.of(innerConditional), List.of());

        List<ReplayMessage> result = apply(step, msg("orders"));  // partition=0

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers)
                .anyMatch(h -> "x-inner".equals(h.key()) && "yes".equals(h.value()));
    }

    @Test
    void nestedConditional_innerElseExecutes() {
        // Outer condition: topic EQ "orders"
        //   Inner condition: partition EQ 99 (won't match, partition=0)
        //   Inner else_steps: add x-inner=else
        var innerCondition   = new FilterDropStep("$.partition", EQ, 99);
        var innerThenHeader  = new AddHeaderStep("x-inner", "then", false);
        var innerElseHeader  = new AddHeaderStep("x-inner", "else", false);
        var innerConditional = new ConditionalStep(innerCondition,
                List.of(innerThenHeader), List.of(innerElseHeader));

        var outerCondition = new FilterDropStep("$.topic", EQ, "orders");
        var step           = new ConditionalStep(outerCondition, List.of(innerConditional), List.of());

        List<ReplayMessage> result = apply(step, msg("orders"));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers)
                .anyMatch(h -> "x-inner".equals(h.key()) && "else".equals(h.value()));
    }

    // =========================================================================
    // filter_drop inside a branch drops the message
    // =========================================================================

    @Test
    void dropInThenBranch_dropsMessage() {
        // Condition: topic EQ "orders" → filter_drop (always drops)
        var condition = new FilterDropStep("$.topic", EQ, "orders");
        var dropStep  = new FilterDropStep("$.topic", EQ, "orders"); // same predicate = always drops
        var step      = new ConditionalStep(condition, List.of(dropStep), List.of());

        List<ReplayMessage> result = apply(step, msg("orders"));

        assertThat(result).isEmpty();
    }

    @Test
    void dropInElseBranch_dropsMessage() {
        // Condition: topic EQ "orders" (won't match for "payments")
        // else_steps: drop all messages with partition 0
        var condition = new FilterDropStep("$.topic", EQ, "orders");
        var dropStep  = new FilterDropStep("$.partition", EQ, 0);
        var step      = new ConditionalStep(condition, List.of(), List.of(dropStep));

        List<ReplayMessage> result = apply(step, msg("payments")); // partition=0

        assertThat(result).isEmpty();
    }

    @Test
    void dropInBranch_doesNotAffectNonMatchingMessages() {
        // Condition: topic EQ "orders"
        // then_steps: drop messages with partition EQ 0
        // Message: topic=payments — condition is false, else is empty → passes through
        var condition = new FilterDropStep("$.topic", EQ, "orders");
        var dropStep  = new FilterDropStep("$.partition", EQ, 0);
        var step      = new ConditionalStep(condition, List.of(dropStep), List.of());

        List<ReplayMessage> result = apply(step, msg("payments"));

        assertThat(result).hasSize(1);
    }

    // =========================================================================
    // $.headers[key] extraction in condition
    // =========================================================================

    @Test
    void headerCondition_thenBranchWhenHeaderMatches() {
        // Redact if x-env is NOT prod (i.e., header x-env = "staging" → condition true)
        var condition  = new FilterDropStep("$.headers[x-env]", NEQ, "prod");
        var thenHeader = new AddHeaderStep("x-redacted", "true", false);
        var step       = new ConditionalStep(condition, List.of(thenHeader), List.of());

        // Header x-env = staging → NEQ prod → true → then_steps run
        List<ReplayMessage> result = apply(step, msgWithHeader("x-env", "staging"));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers)
                .anyMatch(h -> "x-redacted".equals(h.key()) && "true".equals(h.value()));
    }

    @Test
    void headerCondition_elseBranchWhenHeaderDoesNotMatch() {
        // x-env = prod → NEQ prod → false → else_steps (empty) → pass-through
        var condition = new FilterDropStep("$.headers[x-env]", NEQ, "prod");
        var thenHeader = new AddHeaderStep("x-redacted", "true", false);
        var step      = new ConditionalStep(condition, List.of(thenHeader), List.of());

        List<ReplayMessage> result = apply(step, msgWithHeader("x-env", "prod"));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers).noneMatch(h -> "x-redacted".equals(h.key()));
    }

    @Test
    void headerCondition_missingHeader_treatedAsNull() {
        // IS_NOT_NULL on a missing header → null → false → else_steps
        var condition  = new FilterDropStep("$.headers[x-env]", IS_NOT_NULL, null);
        var thenHeader = new AddHeaderStep("x-present", "yes", false);
        var step       = new ConditionalStep(condition, List.of(thenHeader), List.of());

        // No headers on the message
        List<ReplayMessage> result = apply(step, msg("t"));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers).noneMatch(h -> "x-present".equals(h.key()));
    }

    // =========================================================================
    // Condition on nested value field
    // =========================================================================

    @Test
    void valueFieldCondition_thenBranchApplied() {
        String json = "{\"env\":\"staging\"}";
        var condition  = new FilterDropStep("$.value.env", NEQ, "prod");
        var thenHeader = new AddHeaderStep("x-env-tag", "non-prod", false);
        var step       = new ConditionalStep(condition, List.of(thenHeader), List.of());

        List<ReplayMessage> result = apply(step, msgWithValue("t", json));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers)
                .anyMatch(h -> "x-env-tag".equals(h.key()) && "non-prod".equals(h.value()));
    }
}

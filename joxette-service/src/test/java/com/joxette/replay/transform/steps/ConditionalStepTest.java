package com.joxette.replay.transform.steps;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.Predicate;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformPipeline;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;

import static com.joxette.replay.transform.Predicate.Operator.*;
import static org.assertj.core.api.Assertions.assertThat;

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

    private static Predicate cond(String field, Predicate.Operator op, Object value) {
        return new Predicate.Leaf(field, op, value);
    }

    private static FilterDropStep fds(String field, Predicate.Operator op, Object value) {
        return new FilterDropStep(new Predicate.Leaf(field, op, value));
    }

    private static List<ReplayMessage> apply(ConditionalStep step, ReplayMessage m) {
        return new TransformPipeline(List.of(step), null).apply(m, "rid");
    }

    // =========================================================================
    // Condition branch matrix — (condition true/false) × (else steps present/absent)
    // =========================================================================

    @ParameterizedTest(name = "conditionMatches={0}, hasElseSteps={1} → branch={2}")
    @CsvSource({
        "true,  false, then",
        "true,  true,  then",
        "false, true,  else",
        "false, false, none"
    })
    void conditionBranchMatrix(boolean conditionMatches, boolean hasElseSteps, String expectedBranch) {
        var condition = cond("$.topic", EQ, "orders");
        var thenStep  = new AddHeaderStep("x-branch", "then", false);
        var elseStep  = new AddHeaderStep("x-branch", "else", false);
        var step = hasElseSteps
                ? new ConditionalStep(condition, List.of(thenStep), List.of(elseStep))
                : new ConditionalStep(condition, List.of(thenStep), List.of());

        String topic = conditionMatches ? "orders" : "payments";
        var result   = apply(step, msg(topic));

        assertThat(result).hasSize(1);
        if ("none".equals(expectedBranch)) {
            assertThat(result.get(0).headers).isEmpty();
        } else {
            assertThat(result.get(0).headers).hasSize(1);
            assertThat(result.get(0).headers.get(0).value()).isEqualTo(expectedBranch);
        }
    }

    // =========================================================================
    // Nested conditional
    // =========================================================================

    @Test
    void nestedConditional_innerThenExecutes() {
        var innerCondition   = cond("$.partition", EQ, 0);
        var innerThenHeader  = new AddHeaderStep("x-inner", "yes", false);
        var innerConditional = new ConditionalStep(innerCondition, List.of(innerThenHeader), List.of());

        var outerCondition = cond("$.topic", EQ, "orders");
        var step           = new ConditionalStep(outerCondition, List.of(innerConditional), List.of());

        List<ReplayMessage> result = apply(step, msg("orders"));  // partition=0

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers)
                .anyMatch(h -> "x-inner".equals(h.key()) && "yes".equals(h.value()));
    }

    @Test
    void nestedConditional_innerElseExecutes() {
        var innerCondition   = cond("$.partition", EQ, 99);
        var innerThenHeader  = new AddHeaderStep("x-inner", "then", false);
        var innerElseHeader  = new AddHeaderStep("x-inner", "else", false);
        var innerConditional = new ConditionalStep(innerCondition,
                List.of(innerThenHeader), List.of(innerElseHeader));

        var outerCondition = cond("$.topic", EQ, "orders");
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
        var condition = cond("$.topic", EQ, "orders");
        var dropStep  = fds("$.topic", EQ, "orders");
        var step      = new ConditionalStep(condition, List.of(dropStep), List.of());

        List<ReplayMessage> result = apply(step, msg("orders"));

        assertThat(result).isEmpty();
    }

    @Test
    void dropInElseBranch_dropsMessage() {
        var condition = cond("$.topic", EQ, "orders");
        var dropStep  = fds("$.partition", EQ, 0);
        var step      = new ConditionalStep(condition, List.of(), List.of(dropStep));

        List<ReplayMessage> result = apply(step, msg("payments")); // partition=0

        assertThat(result).isEmpty();
    }

    @Test
    void dropInBranch_doesNotAffectNonMatchingMessages() {
        var condition = cond("$.topic", EQ, "orders");
        var dropStep  = fds("$.partition", EQ, 0);
        var step      = new ConditionalStep(condition, List.of(dropStep), List.of());

        List<ReplayMessage> result = apply(step, msg("payments"));

        assertThat(result).hasSize(1);
    }

    // =========================================================================
    // $.headers[key] extraction in condition
    // =========================================================================

    static Stream<Arguments> headerConditionCases() {
        return Stream.of(
                Arguments.of(
                        msgWithHeader("x-env", "staging"),
                        cond("$.headers[x-env]", NEQ, "prod"),
                        true,
                        "header present, NEQ prod (staging) → then branch applied"
                ),
                Arguments.of(
                        msgWithHeader("x-env", "prod"),
                        cond("$.headers[x-env]", NEQ, "prod"),
                        false,
                        "header present but equals prod → condition false → pass-through"
                ),
                Arguments.of(
                        msg("t"),
                        cond("$.headers[x-env]", IS_NOT_NULL, null),
                        false,
                        "missing header → IS_NOT_NULL false → pass-through"
                )
        );
    }

    @ParameterizedTest(name = "{3}")
    @MethodSource("headerConditionCases")
    void headerCondition(ReplayMessage message, Predicate condition, boolean expectThenBranch, String displayName) {
        var thenHeader = new AddHeaderStep("x-redacted", "true", false);
        var step       = new ConditionalStep(condition, List.of(thenHeader), List.of());

        var result = apply(step, message);

        assertThat(result).hasSize(1);
        if (expectThenBranch) {
            assertThat(result.get(0).headers)
                    .anyMatch(h -> "x-redacted".equals(h.key()) && "true".equals(h.value()));
        } else {
            assertThat(result.get(0).headers).noneMatch(h -> "x-redacted".equals(h.key()));
        }
    }

    // =========================================================================
    // Condition on nested value field
    // =========================================================================

    @Test
    void valueFieldCondition_thenBranchApplied() {
        String json = "{\"env\":\"staging\"}";
        var condition  = cond("$.value.env", NEQ, "prod");
        var thenHeader = new AddHeaderStep("x-env-tag", "non-prod", false);
        var step       = new ConditionalStep(condition, List.of(thenHeader), List.of());

        List<ReplayMessage> result = apply(step, msgWithValue("t", json));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers)
                .anyMatch(h -> "x-env-tag".equals(h.key()) && "non-prod".equals(h.value()));
    }

    // =========================================================================
    // Compound condition (and/or)
    // =========================================================================

    @Test
    void compoundAndCondition_thenBranchWhenAllMatch() {
        var condition = new Predicate.And(List.of(
                new Predicate.Leaf("$.topic", EQ, "orders"),
                new Predicate.Leaf("$.partition", EQ, 0)));
        var thenHeader = new AddHeaderStep("x-matched", "yes", false);
        var step       = new ConditionalStep(condition, List.of(thenHeader), List.of());

        List<ReplayMessage> result = apply(step, msg("orders")); // partition=0

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers)
                .anyMatch(h -> "x-matched".equals(h.key()) && "yes".equals(h.value()));
    }
}

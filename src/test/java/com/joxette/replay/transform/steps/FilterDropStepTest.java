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
 * Unit tests for {@link FilterDropStep} — each operator, top-level field extraction,
 * nested value JSON extraction, and pass-through when predicate does not match.
 */
class FilterDropStepTest {

    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();

    private static String b64(String s) {
        return ENC.encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static CassetteRecord record(int partition, long offset,
                                          String topic, String value) {
        Instant ts = Instant.parse("2024-06-01T12:00:00Z");
        return new CassetteRecord(topic, partition, offset, ts, ts, "k", value, null, null);
    }

    private static ReplayMessage msg(int partition, long offset,
                                      String topic, String value) {
        return new ReplayMessage(record(partition, offset, topic, value));
    }

    private static List<ReplayMessage> apply(FilterDropStep step, ReplayMessage m) {
        return new TransformPipeline(List.of(step), null).apply(m, "rid");
    }

    // =========================================================================
    // EQ / NEQ
    // =========================================================================

    @Test
    void eq_dropsWhenPartitionMatches() {
        var step   = new FilterDropStep("$.partition", EQ, 3);
        assertThat(apply(step, msg(3, 0L, "t", null))).isEmpty();
    }

    @Test
    void eq_passesWhenPartitionDiffers() {
        var step   = new FilterDropStep("$.partition", EQ, 3);
        assertThat(apply(step, msg(2, 0L, "t", null))).hasSize(1);
    }

    @Test
    void neq_dropsWhenPartitionDiffers() {
        var step = new FilterDropStep("$.partition", NEQ, 3);
        assertThat(apply(step, msg(5, 0L, "t", null))).isEmpty();
    }

    @Test
    void neq_passesWhenPartitionEquals() {
        var step = new FilterDropStep("$.partition", NEQ, 3);
        assertThat(apply(step, msg(3, 0L, "t", null))).hasSize(1);
    }

    // =========================================================================
    // GT / GTE / LT / LTE
    // =========================================================================

    @Test
    void gt_dropsWhenOffsetAboveThreshold() {
        var step = new FilterDropStep("$.offset", GT, 100);
        assertThat(apply(step, msg(0, 200L, "t", null))).isEmpty();
    }

    @Test
    void gt_passesWhenOffsetAtThreshold() {
        var step = new FilterDropStep("$.offset", GT, 100);
        assertThat(apply(step, msg(0, 100L, "t", null))).hasSize(1);
    }

    @Test
    void gte_dropsWhenOffsetAtThreshold() {
        var step = new FilterDropStep("$.offset", GTE, 100);
        assertThat(apply(step, msg(0, 100L, "t", null))).isEmpty();
    }

    @Test
    void lt_dropsWhenOffsetBelowThreshold() {
        var step = new FilterDropStep("$.offset", LT, 50);
        assertThat(apply(step, msg(0, 10L, "t", null))).isEmpty();
    }

    @Test
    void lte_dropsWhenOffsetAtThreshold() {
        var step = new FilterDropStep("$.offset", LTE, 50);
        assertThat(apply(step, msg(0, 50L, "t", null))).isEmpty();
    }

    @Test
    void lte_passesWhenOffsetAboveThreshold() {
        var step = new FilterDropStep("$.offset", LTE, 50);
        assertThat(apply(step, msg(0, 51L, "t", null))).hasSize(1);
    }

    // =========================================================================
    // CONTAINS
    // =========================================================================

    @Test
    void contains_dropsWhenTopicContainsSubstring() {
        var step = new FilterDropStep("$.topic", CONTAINS, "staging");
        assertThat(apply(step, msg(0, 0L, "orders-staging", null))).isEmpty();
    }

    @Test
    void contains_passesWhenTopicDoesNotContainSubstring() {
        var step = new FilterDropStep("$.topic", CONTAINS, "staging");
        assertThat(apply(step, msg(0, 0L, "orders-prod", null))).hasSize(1);
    }

    // =========================================================================
    // MATCHES (regex, pre-compiled)
    // =========================================================================

    @Test
    void matches_precompilesPattern() {
        var step = new FilterDropStep("$.topic", MATCHES, "^orders-.*");
        assertThat(step.compiledPattern()).isNotNull();
    }

    @Test
    void matches_dropsWhenTopicMatchesRegex() {
        var step = new FilterDropStep("$.topic", MATCHES, "^orders-.*");
        assertThat(apply(step, msg(0, 0L, "orders-prod", null))).isEmpty();
    }

    @Test
    void matches_passesWhenTopicDoesNotMatchRegex() {
        var step = new FilterDropStep("$.topic", MATCHES, "^orders-.*");
        assertThat(apply(step, msg(0, 0L, "payments-prod", null))).hasSize(1);
    }

    @Test
    void matches_compiledPatternIsNullForNonMatchesOperator() {
        var step = new FilterDropStep("$.topic", EQ, "orders");
        assertThat(step.compiledPattern()).isNull();
    }

    // =========================================================================
    // IS_NULL / IS_NOT_NULL
    // =========================================================================

    @Test
    void isNull_dropsWhenKeyIsNull() {
        Instant ts  = Instant.parse("2024-01-01T00:00:00Z");
        var     rec = new CassetteRecord("t", 0, 0L, ts, ts, null, null, null, null);
        var     step = new FilterDropStep("$.key", IS_NULL, null);
        assertThat(apply(step, new ReplayMessage(rec))).isEmpty();
    }

    @Test
    void isNull_passesWhenKeyIsPresent() {
        var step = new FilterDropStep("$.key", IS_NULL, null);
        assertThat(apply(step, msg(0, 0L, "t", null))).hasSize(1);
    }

    @Test
    void isNotNull_dropsWhenKeyPresent() {
        var step = new FilterDropStep("$.key", IS_NOT_NULL, null);
        assertThat(apply(step, msg(0, 0L, "t", null))).isEmpty();
    }

    // =========================================================================
    // Nested $.value.* field extraction
    // =========================================================================

    @Test
    void nestedValue_dropsWhenFieldMatches() {
        String json = "{\"status\":\"cancelled\"}";
        var    step = new FilterDropStep("$.value.status", EQ, "cancelled");
        assertThat(apply(step, msg(0, 0L, "t", b64(json)))).isEmpty();
    }

    @Test
    void nestedValue_passesWhenFieldDiffers() {
        String json = "{\"status\":\"completed\"}";
        var    step = new FilterDropStep("$.value.status", EQ, "cancelled");
        assertThat(apply(step, msg(0, 0L, "t", b64(json)))).hasSize(1);
    }

    @Test
    void nestedValue_returnsNullForMissingPath() {
        String json = "{\"a\":1}";
        var    step = new FilterDropStep("$.value.no_such", IS_NOT_NULL, null);
        // IS_NOT_NULL drops when field is non-null; missing path → null → does not drop
        assertThat(apply(step, msg(0, 0L, "t", b64(json)))).hasSize(1);
    }

    @Test
    void nestedValue_handlesNullMessageValue() {
        var step = new FilterDropStep("$.value.status", EQ, "cancelled");
        // value is null → extracted is null → "cancelled" does not equal null
        assertThat(apply(step, msg(0, 0L, "t", null))).hasSize(1);
    }

    // =========================================================================
    // $.topic top-level field
    // =========================================================================

    @Test
    void topicField_dropsExactMatch() {
        var step = new FilterDropStep("$.topic", EQ, "audit.log");
        assertThat(apply(step, msg(0, 0L, "audit.log", null))).isEmpty();
    }

    @Test
    void topicField_passesNonMatch() {
        var step = new FilterDropStep("$.topic", EQ, "audit.log");
        assertThat(apply(step, msg(0, 0L, "orders.events", null))).hasSize(1);
    }
}

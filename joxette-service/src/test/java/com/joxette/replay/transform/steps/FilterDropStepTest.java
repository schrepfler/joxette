package com.joxette.replay.transform.steps;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.Predicate;
import com.joxette.replay.transform.Predicate.Operator;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformPipeline;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

import static com.joxette.replay.transform.Predicate.Operator.*;
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

    /** Convenience factory for simple leaf-predicate filter steps. */
    private static FilterDropStep fds(String field, Operator op, Object value) {
        return new FilterDropStep(new Predicate.Leaf(field, op, value));
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
        assertThat(apply(fds("$.partition", EQ, 3), msg(3, 0L, "t", null))).isEmpty();
    }

    @Test
    void eq_passesWhenPartitionDiffers() {
        assertThat(apply(fds("$.partition", EQ, 3), msg(2, 0L, "t", null))).hasSize(1);
    }

    @Test
    void neq_dropsWhenPartitionDiffers() {
        assertThat(apply(fds("$.partition", NEQ, 3), msg(5, 0L, "t", null))).isEmpty();
    }

    @Test
    void neq_passesWhenPartitionEquals() {
        assertThat(apply(fds("$.partition", NEQ, 3), msg(3, 0L, "t", null))).hasSize(1);
    }

    // =========================================================================
    // GT / GTE / LT / LTE
    // =========================================================================

    @Test
    void gt_dropsWhenOffsetAboveThreshold() {
        assertThat(apply(fds("$.offset", GT, 100), msg(0, 200L, "t", null))).isEmpty();
    }

    @Test
    void gt_passesWhenOffsetAtThreshold() {
        assertThat(apply(fds("$.offset", GT, 100), msg(0, 100L, "t", null))).hasSize(1);
    }

    @Test
    void gte_dropsWhenOffsetAtThreshold() {
        assertThat(apply(fds("$.offset", GTE, 100), msg(0, 100L, "t", null))).isEmpty();
    }

    @Test
    void lt_dropsWhenOffsetBelowThreshold() {
        assertThat(apply(fds("$.offset", LT, 50), msg(0, 10L, "t", null))).isEmpty();
    }

    @Test
    void lte_dropsWhenOffsetAtThreshold() {
        assertThat(apply(fds("$.offset", LTE, 50), msg(0, 50L, "t", null))).isEmpty();
    }

    @Test
    void lte_passesWhenOffsetAboveThreshold() {
        assertThat(apply(fds("$.offset", LTE, 50), msg(0, 51L, "t", null))).hasSize(1);
    }

    // =========================================================================
    // CONTAINS
    // =========================================================================

    @Test
    void contains_dropsWhenTopicContainsSubstring() {
        assertThat(apply(fds("$.topic", CONTAINS, "staging"), msg(0, 0L, "orders-staging", null))).isEmpty();
    }

    @Test
    void contains_passesWhenTopicDoesNotContainSubstring() {
        assertThat(apply(fds("$.topic", CONTAINS, "staging"), msg(0, 0L, "orders-prod", null))).hasSize(1);
    }

    // =========================================================================
    // MATCHES (regex)
    // =========================================================================

    @Test
    void matches_dropsWhenTopicMatchesRegex() {
        assertThat(apply(fds("$.topic", MATCHES, "^orders-.*"), msg(0, 0L, "orders-prod", null))).isEmpty();
    }

    @Test
    void matches_passesWhenTopicDoesNotMatchRegex() {
        assertThat(apply(fds("$.topic", MATCHES, "^orders-.*"), msg(0, 0L, "payments-prod", null))).hasSize(1);
    }

    // =========================================================================
    // IS_NULL / IS_NOT_NULL
    // =========================================================================

    @Test
    void isNull_dropsWhenKeyIsNull() {
        Instant ts  = Instant.parse("2024-01-01T00:00:00Z");
        var     rec = new CassetteRecord("t", 0, 0L, ts, ts, null, null, null, null);
        assertThat(apply(fds("$.key", IS_NULL, null), new ReplayMessage(rec))).isEmpty();
    }

    @Test
    void isNull_passesWhenKeyIsPresent() {
        assertThat(apply(fds("$.key", IS_NULL, null), msg(0, 0L, "t", null))).hasSize(1);
    }

    @Test
    void isNotNull_dropsWhenKeyPresent() {
        assertThat(apply(fds("$.key", IS_NOT_NULL, null), msg(0, 0L, "t", null))).isEmpty();
    }

    // =========================================================================
    // Nested $.value.* field extraction
    // =========================================================================

    @Test
    void nestedValue_dropsWhenFieldMatches() {
        String json = "{\"status\":\"cancelled\"}";
        assertThat(apply(fds("$.value.status", EQ, "cancelled"), msg(0, 0L, "t", b64(json)))).isEmpty();
    }

    @Test
    void nestedValue_passesWhenFieldDiffers() {
        String json = "{\"status\":\"completed\"}";
        assertThat(apply(fds("$.value.status", EQ, "cancelled"), msg(0, 0L, "t", b64(json)))).hasSize(1);
    }

    @Test
    void nestedValue_returnsNullForMissingPath() {
        String json = "{\"a\":1}";
        // IS_NOT_NULL drops when field is non-null; missing path → null → does not drop
        assertThat(apply(fds("$.value.no_such", IS_NOT_NULL, null), msg(0, 0L, "t", b64(json)))).hasSize(1);
    }

    @Test
    void nestedValue_handlesNullMessageValue() {
        // value is null → extracted is null → "cancelled" does not equal null
        assertThat(apply(fds("$.value.status", EQ, "cancelled"), msg(0, 0L, "t", null))).hasSize(1);
    }

    // =========================================================================
    // $.topic top-level field
    // =========================================================================

    @Test
    void topicField_dropsExactMatch() {
        assertThat(apply(fds("$.topic", EQ, "audit.log"), msg(0, 0L, "audit.log", null))).isEmpty();
    }

    @Test
    void topicField_passesNonMatch() {
        assertThat(apply(fds("$.topic", EQ, "audit.log"), msg(0, 0L, "orders.events", null))).hasSize(1);
    }

    // =========================================================================
    // Compound predicates (and/or/not)
    // =========================================================================

    @Test
    void compoundAnd_dropsWhenBothMatch() {
        var predicate = new Predicate.And(List.of(
                new Predicate.Leaf("$.partition", EQ, 0),
                new Predicate.Leaf("$.topic", EQ, "orders")));
        var step = new FilterDropStep(predicate);
        assertThat(apply(step, msg(0, 0L, "orders", null))).isEmpty();
    }

    @Test
    void compoundAnd_passesWhenOnlyOneBranchMatches() {
        var predicate = new Predicate.And(List.of(
                new Predicate.Leaf("$.partition", EQ, 0),
                new Predicate.Leaf("$.topic", EQ, "orders")));
        var step = new FilterDropStep(predicate);
        // partition=0 matches but topic=payments doesn't → AND is false → pass through
        assertThat(apply(step, msg(0, 0L, "payments", null))).hasSize(1);
    }

    @Test
    void compoundOr_dropsWhenEitherMatches() {
        var predicate = new Predicate.Or(List.of(
                new Predicate.Leaf("$.partition", EQ, 0),
                new Predicate.Leaf("$.topic", EQ, "orders")));
        var step = new FilterDropStep(predicate);
        assertThat(apply(step, msg(5, 0L, "orders", null))).isEmpty();
    }

    @Test
    void compoundNot_dropsWhenInnerDoesNotMatch() {
        var predicate = new Predicate.Not(new Predicate.Leaf("$.topic", EQ, "orders"));
        var step = new FilterDropStep(predicate);
        // NOT(topic=orders) → drops topic=payments
        assertThat(apply(step, msg(0, 0L, "payments", null))).isEmpty();
        // NOT(topic=orders) → passes topic=orders
        assertThat(apply(step, msg(0, 0L, "orders", null))).hasSize(1);
    }

    // =========================================================================
    // Convenience accessors for SqlPushdownAnalyzer compatibility
    // =========================================================================

    @Test
    void leafPredicateAccessors_returnFieldOperatorValue() {
        var step = fds("$.partition", EQ, 3);
        assertThat(step.field()).isEqualTo("$.partition");
        assertThat(step.operator()).isEqualTo(EQ);
        assertThat(step.value()).isEqualTo(3);
    }

    @Test
    void compoundPredicateAccessors_returnNull() {
        var step = new FilterDropStep(new Predicate.And(List.of(
                new Predicate.Leaf("$.partition", EQ, 0),
                new Predicate.Leaf("$.topic", EQ, "t"))));
        assertThat(step.field()).isNull();
        assertThat(step.operator()).isNull();
        assertThat(step.value()).isNull();
    }
}

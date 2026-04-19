package com.joxette.replay.transform.steps;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.Predicate;
import com.joxette.replay.transform.Predicate.Operator;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformPipeline;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

import static com.joxette.replay.transform.Predicate.Operator.*;
import static org.assertj.core.api.Assertions.assertThat;

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
    // EQ / NEQ / GT / GTE / LT / LTE — numeric fields
    // =========================================================================

    @ParameterizedTest(name = "{1} on {0}: threshold={2}, partition={3}, offset={4} => shouldDrop={5}")
    @CsvSource({
        "$.partition, EQ,  3,   3,   0, true",
        "$.partition, EQ,  3,   2,   0, false",
        "$.partition, NEQ, 3,   5,   0, true",
        "$.partition, NEQ, 3,   3,   0, false",
        "$.offset,    GT,  100, 0, 200, true",
        "$.offset,    GT,  100, 0, 100, false",
        "$.offset,    GTE, 100, 0, 100, true",
        "$.offset,    LT,  50,  0,  10, true",
        "$.offset,    LTE, 50,  0,  50, true",
        "$.offset,    LTE, 50,  0,  51, false"
    })
    void numericComparisonOperators(String field, String opName, int threshold,
                                     int partition, long offset, boolean shouldDrop) {
        var result = apply(fds(field, Operator.valueOf(opName), threshold),
                           msg(partition, offset, "t", null));
        if (shouldDrop) assertThat(result).isEmpty();
        else            assertThat(result).hasSize(1);
    }

    // =========================================================================
    // CONTAINS / MATCHES — string fields
    // =========================================================================

    @ParameterizedTest(name = "{0} ''{1}'' on topic ''{2}'' => shouldDrop={3}")
    @CsvSource({
        "CONTAINS, staging,    orders-staging, true",
        "CONTAINS, staging,    orders-prod,    false",
        "MATCHES,  ^orders-.*, orders-prod,    true",
        "MATCHES,  ^orders-.*, payments-prod,  false"
    })
    void stringOperators(String opName, String pattern, String topic, boolean shouldDrop) {
        var result = apply(fds("$.topic", Operator.valueOf(opName), pattern),
                           msg(0, 0L, topic, null));
        if (shouldDrop) assertThat(result).isEmpty();
        else            assertThat(result).hasSize(1);
    }

    // =========================================================================
    // IS_NULL / IS_NOT_NULL
    // =========================================================================

    @ParameterizedTest(name = "{0} with nullKey={1} => shouldDrop={2}")
    @CsvSource({
        "IS_NULL,     true,  true",
        "IS_NULL,     false, false",
        "IS_NOT_NULL, false, true"
    })
    void nullCheckOperators(String opName, boolean useNullKey, boolean shouldDrop) {
        ReplayMessage message;
        if (useNullKey) {
            Instant ts = Instant.parse("2024-01-01T00:00:00Z");
            message = new ReplayMessage(new CassetteRecord("t", 0, 0L, ts, ts, null, null, null, null));
        } else {
            message = msg(0, 0L, "t", null);
        }
        var result = apply(fds("$.key", Operator.valueOf(opName), null), message);
        if (shouldDrop) assertThat(result).isEmpty();
        else            assertThat(result).hasSize(1);
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

package com.joxette.replay.transform;

import com.joxette.replay.CassetteRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

import static com.joxette.replay.transform.Predicate.Operator.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PredicateEvaluator} — exercises the full sealed
 * {@link Predicate} hierarchy (Leaf, And, Or, Not) and all operators directly,
 * without going through the pipeline.
 */
class PredicateEvaluatorTest {

    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();
    private static final Instant        TS  = Instant.parse("2024-06-01T12:00:00Z");

    private static String b64(String s) {
        return ENC.encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    /** Message with all top-level fields set; value is JSON-encoded if non-null. */
    private static ReplayMessage msg(String topic, int partition, long offset,
                                     String key, String valueJson) {
        String enc = valueJson != null ? b64(valueJson) : null;
        return new ReplayMessage(new CassetteRecord(topic, partition, offset, TS, TS, key, enc, null, null));
    }

    /** Message with a single header; all other fields default. */
    private static ReplayMessage msgWithHeader(String headerKey, String headerValue) {
        return new ReplayMessage(new CassetteRecord(
                "t", 0, 0L, TS, TS, "k", null,
                List.of(new CassetteRecord.Header(headerKey, headerValue)),
                null));
    }

    private static boolean eval(Predicate p, ReplayMessage m) {
        return PredicateEvaluator.evaluate(p, m);
    }

    private static Predicate leaf(String field, Predicate.Operator op, Object value) {
        return new Predicate.Leaf(field, op, value);
    }

    // =========================================================================
    // EQ / NEQ — top-level fields
    // =========================================================================

    @ParameterizedTest
    @CsvSource({
        "$.topic, EQ,  orders,   orders, true",
        "$.topic, EQ,  payments, orders, false",
        "$.topic, NEQ, payments, orders, true",
        "$.topic, NEQ, orders,   orders, false"
    })
    void eqNeq_topLevel(String field, String op, String testValue, String compareValue, boolean expected) {
        assertThat(eval(leaf(field, Predicate.Operator.valueOf(op), compareValue),
                msg(testValue, 0, 0L, "k", null))).isEqualTo(expected);
    }

    // =========================================================================
    // EQ / NEQ — nested JSON paths
    // =========================================================================

    @Test
    void eq_nestedPath_trueWhenFieldMatches() {
        assertThat(eval(leaf("$.value.status", EQ, "created"),
                msg("t", 0, 0L, "k", "{\"status\":\"created\"}"))).isTrue();
    }

    @Test
    void eq_nestedPath_falseWhenFieldDiffers() {
        assertThat(eval(leaf("$.value.status", EQ, "cancelled"),
                msg("t", 0, 0L, "k", "{\"status\":\"created\"}"))).isFalse();
    }

    @Test
    void neq_nestedPath_trueWhenFieldDiffers() {
        assertThat(eval(leaf("$.value.status", NEQ, "cancelled"),
                msg("t", 0, 0L, "k", "{\"status\":\"created\"}"))).isTrue();
    }

    @Test
    void eq_deeplyNestedPath_trueWhenFieldMatches() {
        assertThat(eval(leaf("$.value.order.status", EQ, "paid"),
                msg("t", 0, 0L, "k", "{\"order\":{\"status\":\"paid\"}}"))).isTrue();
    }

    // =========================================================================
    // GT / GTE / LT / LTE — top-level and nested $.value.* numeric comparisons
    // =========================================================================

    @ParameterizedTest
    @CsvSource({
        "$.partition,    GT,  5,   3,   true",
        "$.partition,    GT,  5,   5,   false",
        "$.offset,       GTE, 100, 100, true",
        "$.offset,       GTE, 99,  100, false",
        "$.partition,    LT,  3,   10,  true",
        "$.partition,    LT,  5,   5,   false",
        "$.offset,       LTE, 50,  50,  true",
        "$.offset,       LTE, 51,  50,  false",
        "$.value.amount, GT,  150, 99,  true",
        "$.value.amount, LTE, 150, 150, true"
    })
    void numericComparisons(String field, String op, int testValue, int compareValue, boolean expected) {
        ReplayMessage m = switch (field) {
            case "$.partition" -> msg("t", testValue, 0L, "k", null);
            case "$.offset"    -> msg("t", 0, (long) testValue, "k", null);
            default            -> msg("t", 0, 0L, "k", "{\"amount\":" + testValue + "}");
        };
        assertThat(eval(leaf(field, Predicate.Operator.valueOf(op), compareValue), m)).isEqualTo(expected);
    }

    // =========================================================================
    // CONTAINS
    // =========================================================================

    @Test
    void contains_trueWhenTopicContainsSubstring() {
        assertThat(eval(leaf("$.topic", CONTAINS, "staging"),
                msg("orders-staging", 0, 0L, "k", null))).isTrue();
    }

    @Test
    void contains_falseWhenTopicDoesNotContainSubstring() {
        assertThat(eval(leaf("$.topic", CONTAINS, "staging"),
                msg("orders-prod", 0, 0L, "k", null))).isFalse();
    }

    // =========================================================================
    // MATCHES (regex)
    // =========================================================================

    @Test
    void matches_trueWhenTopicMatchesRegex() {
        assertThat(eval(leaf("$.topic", MATCHES, "^orders\\..*"),
                msg("orders.events", 0, 0L, "k", null))).isTrue();
    }

    @Test
    void matches_falseWhenTopicDoesNotMatchRegex() {
        assertThat(eval(leaf("$.topic", MATCHES, "^orders\\..*"),
                msg("payments.events", 0, 0L, "k", null))).isFalse();
    }

    // =========================================================================
    // IS_NULL / IS_NOT_NULL
    // =========================================================================

    @Test
    void isNull_trueWhenKeyIsNull() {
        var rec = new CassetteRecord("t", 0, 0L, TS, TS, null, null, null, null);
        assertThat(eval(leaf("$.key", IS_NULL, null), new ReplayMessage(rec))).isTrue();
    }

    @Test
    void isNull_falseWhenKeyPresent() {
        assertThat(eval(leaf("$.key", IS_NULL, null), msg("t", 0, 0L, "k", null))).isFalse();
    }

    @Test
    void isNotNull_trueWhenKeyPresent() {
        assertThat(eval(leaf("$.key", IS_NOT_NULL, null), msg("t", 0, 0L, "k", null))).isTrue();
    }

    @Test
    void isNotNull_falseWhenKeyIsNull() {
        var rec = new CassetteRecord("t", 0, 0L, TS, TS, null, null, null, null);
        assertThat(eval(leaf("$.key", IS_NOT_NULL, null), new ReplayMessage(rec))).isFalse();
    }

    // =========================================================================
    // Missing field treated as null
    // =========================================================================

    @Test
    void missingNestedField_isNull_true() {
        assertThat(eval(leaf("$.value.nonexistent", IS_NULL, null),
                msg("t", 0, 0L, "k", "{\"a\":1}"))).isTrue();
    }

    @Test
    void missingNestedField_eq_false() {
        assertThat(eval(leaf("$.value.nonexistent", EQ, "x"),
                msg("t", 0, 0L, "k", "{\"a\":1}"))).isFalse();
    }

    @Test
    void nullMessageValue_nestedPathIsNull_true() {
        assertThat(eval(leaf("$.value.status", IS_NULL, null),
                msg("t", 0, 0L, "k", null))).isTrue();
    }

    // =========================================================================
    // Type mismatch returns false
    // =========================================================================

    @Test
    void typeMismatch_stringFieldEqNumericValue_false() {
        // $.topic is "orders"; comparing EQ against integer 42 → "orders" != "42"
        assertThat(eval(leaf("$.topic", EQ, 42), msg("orders", 0, 0L, "k", null))).isFalse();
    }

    // =========================================================================
    // Header field: $.headers[x-env]
    // =========================================================================

    @Test
    void headerField_presentEqMatches_true() {
        assertThat(eval(leaf("$.headers[x-env]", EQ, "prod"),
                msgWithHeader("x-env", "prod"))).isTrue();
    }

    @Test
    void headerField_presentEqMismatch_false() {
        assertThat(eval(leaf("$.headers[x-env]", EQ, "prod"),
                msgWithHeader("x-env", "staging"))).isFalse();
    }

    @Test
    void headerField_absent_isNull_true() {
        // No headers on the message → extracted is null → IS_NULL matches
        assertThat(eval(leaf("$.headers[x-env]", IS_NULL, null),
                msg("t", 0, 0L, "k", null))).isTrue();
    }

    @Test
    void headerField_present_isNull_false() {
        assertThat(eval(leaf("$.headers[x-env]", IS_NULL, null),
                msgWithHeader("x-env", "prod"))).isFalse();
    }

    // =========================================================================
    // AndPredicate
    // =========================================================================

    @Test
    void and_allTrue_isTrue() {
        var p = new Predicate.And(List.of(
                leaf("$.topic", EQ, "orders"),
                leaf("$.partition", EQ, 0)));
        assertThat(eval(p, msg("orders", 0, 0L, "k", null))).isTrue();
    }

    @Test
    void and_firstFalse_isFalse() {
        // First predicate fails; second would pass but AND short-circuits
        var p = new Predicate.And(List.of(
                leaf("$.topic", EQ, "payments"),   // false — "orders" ≠ "payments"
                leaf("$.partition", EQ, 0)));       // true — not evaluated
        assertThat(eval(p, msg("orders", 0, 0L, "k", null))).isFalse();
    }

    @Test
    void and_lastFalse_isFalse() {
        var p = new Predicate.And(List.of(
                leaf("$.topic", EQ, "orders"),    // true
                leaf("$.partition", EQ, 99)));     // false — partition is 0
        assertThat(eval(p, msg("orders", 0, 0L, "k", null))).isFalse();
    }

    // =========================================================================
    // OrPredicate
    // =========================================================================

    @Test
    void or_firstTrue_isTrue() {
        // First predicate passes; second is not evaluated (short-circuit)
        var p = new Predicate.Or(List.of(
                leaf("$.topic", EQ, "orders"),     // true — short-circuits
                leaf("$.topic", EQ, "payments"))); // not evaluated
        assertThat(eval(p, msg("orders", 0, 0L, "k", null))).isTrue();
    }

    @Test
    void or_secondTrue_isTrue() {
        var p = new Predicate.Or(List.of(
                leaf("$.topic", EQ, "payments"),   // false
                leaf("$.partition", EQ, 0)));       // true
        assertThat(eval(p, msg("orders", 0, 0L, "k", null))).isTrue();
    }

    @Test
    void or_allFalse_isFalse() {
        var p = new Predicate.Or(List.of(
                leaf("$.topic", EQ, "payments"),
                leaf("$.partition", EQ, 99)));
        assertThat(eval(p, msg("orders", 0, 0L, "k", null))).isFalse();
    }

    // =========================================================================
    // NotPredicate
    // =========================================================================

    @Test
    void not_trueBecomesFalse() {
        var p = new Predicate.Not(leaf("$.topic", EQ, "orders"));
        assertThat(eval(p, msg("orders", 0, 0L, "k", null))).isFalse();
    }

    @Test
    void not_falseBecomesTrue() {
        var p = new Predicate.Not(leaf("$.topic", EQ, "orders"));
        assertThat(eval(p, msg("payments", 0, 0L, "k", null))).isTrue();
    }

    @Test
    void not_wrappingAndPredicate_correct() {
        var inner = new Predicate.And(List.of(
                leaf("$.topic", EQ, "orders"),
                leaf("$.partition", EQ, 0)));

        // NOT(AND(true, true)) → NOT(true) → false
        assertThat(eval(new Predicate.Not(inner), msg("orders", 0, 0L, "k", null))).isFalse();
        // NOT(AND(false, true)) → NOT(false) → true
        assertThat(eval(new Predicate.Not(inner), msg("payments", 0, 0L, "k", null))).isTrue();
    }

    // =========================================================================
    // Compound nesting — three levels deep
    // =========================================================================

    @Test
    void compoundNesting_andOrNotLeaf_threeLevels() {
        // AND( OR(topic=orders, topic=payments), NOT(partition=99) )
        var or  = new Predicate.Or(List.of(
                leaf("$.topic", EQ, "orders"),
                leaf("$.topic", EQ, "payments")));
        var not = new Predicate.Not(leaf("$.partition", EQ, 99));
        var and = new Predicate.And(List.of(or, not));

        // topic=orders, partition=0 → OR=true, NOT(false)=true → AND=true
        assertThat(eval(and, msg("orders", 0, 0L, "k", null))).isTrue();
        // topic=audit, partition=0 → OR=false → AND=false
        assertThat(eval(and, msg("audit", 0, 0L, "k", null))).isFalse();
        // topic=orders, partition=99 → OR=true, NOT(true)=false → AND=false
        assertThat(eval(and, msg("orders", 99, 0L, "k", null))).isFalse();
    }

    @Test
    void compoundNesting_andNotOrLeaf_threeLevels() {
        // AND( NOT(OR(topic=orders, topic=payments)), partition=0 )
        var or    = new Predicate.Or(List.of(
                leaf("$.topic", EQ, "orders"),
                leaf("$.topic", EQ, "payments")));
        var notOr = new Predicate.Not(or);
        var and   = new Predicate.And(List.of(notOr, leaf("$.partition", EQ, 0)));

        // topic=audit, partition=0 → NOT(OR=false)=true, partition=0 → AND=true
        assertThat(eval(and, msg("audit", 0, 0L, "k", null))).isTrue();
        // topic=orders, partition=0 → NOT(OR=true)=false → AND=false
        assertThat(eval(and, msg("orders", 0, 0L, "k", null))).isFalse();
        // topic=audit, partition=1 → NOT(false)=true, partition=1≠0 → AND=false
        assertThat(eval(and, msg("audit", 1, 0L, "k", null))).isFalse();
    }
}

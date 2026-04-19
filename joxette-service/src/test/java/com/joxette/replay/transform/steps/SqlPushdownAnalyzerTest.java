package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.Predicate;
import com.joxette.replay.transform.TransformPipeline;
import com.joxette.replay.transform.TransformStep;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.joxette.replay.transform.Predicate.Operator.*;
import static org.assertj.core.api.Assertions.assertThat;

class SqlPushdownAnalyzerTest {

    private static final Set<String> ENTITY_ELIGIBLE = Set.of(
            "$.topic", "$.partition", "$.offset", "$.timestamp", "$.key", "$.recorded_at");

    private static final Set<String> TOPIC_ELIGIBLE = Set.of(
            "$.partition", "$.offset", "$.timestamp", "$.key", "$.recorded_at");

    private static FilterDropStep fds(String field, Predicate.Operator op, Object value) {
        return new FilterDropStep(new Predicate.Leaf(field, op, value));
    }

    private static String sql(org.jooq.Condition c) {
        return DSL.using(SQLDialect.DEFAULT).render(c);
    }

    // =========================================================================
    // Group 1: Eligible pushdown cases
    // =========================================================================

    static Stream<Arguments> eligiblePushdownCases() {
        return Stream.of(
            // EQ negated to <> in SQL
            Arguments.of(EQ,          "$.partition", 3,           TOPIC_ELIGIBLE,  List.of("kafka_partition", "<>")),
            // GTE drops when >= 100 → SQL WHERE < 100
            Arguments.of(GTE,         "$.offset",    100,         TOPIC_ELIGIBLE,  List.of("kafka_offset", "100")),
            // EQ on $.topic available only in the entity-cassette service
            Arguments.of(EQ,          "$.topic",     "audit.log", ENTITY_ELIGIBLE, List.of("topic")),
            // IS_NULL drops when null → SQL WHERE IS NOT NULL
            Arguments.of(IS_NULL,     "$.key",       null,        TOPIC_ELIGIBLE,  List.of("kafka_key", "is not null")),
            // IS_NOT_NULL drops when non-null → SQL WHERE IS NULL
            Arguments.of(IS_NOT_NULL, "$.key",       null,        TOPIC_ELIGIBLE,  List.of("kafka_key", "is null")),
            // CONTAINS on string column
            Arguments.of(CONTAINS,    "$.topic",     "staging",   ENTITY_ELIGIBLE, List.of("topic", "staging")),
            // MATCHES on string column — negated regexp in SQL
            Arguments.of(MATCHES,     "$.key",       "^order-.*", TOPIC_ELIGIBLE,  List.of("kafka_key", "not"))
        );
    }

    @ParameterizedTest(name = "[{index}] {0} on {1} is pushed to SQL WHERE")
    @MethodSource("eligiblePushdownCases")
    void eligible_pushedToSqlWhereClause(Predicate.Operator operator, String field, Object value,
                                          Set<String> eligibleFields, List<String> expectedSqlParts) {
        var fds = fds(field, operator, value);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), eligibleFields);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        expectedSqlParts.forEach(part -> assertThat(where).containsIgnoringCase(part));
    }

    // =========================================================================
    // Group 2: Ineligible cases (remain in Java)
    // =========================================================================

    static Stream<Arguments> ineligibleFilterDropCases() {
        return Stream.of(
            Arguments.of(EQ,       "$.value.status", "cancelled", ENTITY_ELIGIBLE,
                         "nested value field is not a top-level column"),
            Arguments.of(EQ,       "$.topic",        "orders",    TOPIC_ELIGIBLE,
                         "topic has no column in the general cassette table"),
            Arguments.of(MATCHES,  "$.partition",    "^3$",       TOPIC_ELIGIBLE,
                         "MATCHES is only pushed down for string columns"),
            Arguments.of(CONTAINS, "$.offset",       "5",         TOPIC_ELIGIBLE,
                         "CONTAINS is only pushed down for string columns")
        );
    }

    @ParameterizedTest(name = "[{index}] {0} on {1} stays in Java — {4}")
    @MethodSource("ineligibleFilterDropCases")
    void ineligible_remainsInJava(Predicate.Operator operator, String field, Object value,
                                   Set<String> eligibleFields, String reason) {
        var fds = fds(field, operator, value);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), eligibleFields);

        assertThat(result.remainingSteps()).containsExactly(fds);
        assertThat(sql(result.pushdownCondition())).isEqualToIgnoringCase("true");
    }

    @Test
    void ineligible_nonFilterDropStepRemainsInJava() {
        var redirect = new RedirectTopicStep("orders-staging");
        var result = SqlPushdownAnalyzer.analyze(List.of(redirect), ENTITY_ELIGIBLE);

        assertThat(result.remainingSteps()).containsExactly(redirect);
        assertThat(sql(result.pushdownCondition())).isEqualToIgnoringCase("true");
    }

    // =========================================================================
    // Mixed eligible + ineligible
    // =========================================================================

    @Test
    void mixed_eligiblePushedDownIneligibleRemains() {
        var fdsPartition  = fds("$.partition", EQ, 3);
        var fdsValueField = fds("$.value.status", EQ, "done");
        var redirect      = new RedirectTopicStep("orders-staging");

        var result = SqlPushdownAnalyzer.analyze(
                List.of(fdsPartition, fdsValueField, redirect), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps())
                .containsExactly(fdsValueField, redirect);
        assertThat(sql(result.pushdownCondition())).containsIgnoringCase("kafka_partition");
    }

    @Test
    void mixed_orderOfRemainingStepsPreserved() {
        var a = new RedirectTopicStep("a");
        var b = fds("$.value.x", EQ, "y");
        var c = new RemoveHeaderStep("x-trace");
        var d = fds("$.partition", EQ, 5);
        var e = new AddHeaderStep("x-env", "test", false);

        var result = SqlPushdownAnalyzer.analyze(
                List.of(a, b, c, d, e), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps())
                .extracting(Object::getClass)
                .containsExactly(
                        RedirectTopicStep.class,
                        FilterDropStep.class,
                        RemoveHeaderStep.class,
                        AddHeaderStep.class);
    }

    @Test
    void multipleEligibleStepsCombinedAsAnd() {
        var fds1 = fds("$.partition", EQ, 3);
        var fds2 = fds("$.offset", LT, 10);

        var result = SqlPushdownAnalyzer.analyze(List.of(fds1, fds2), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        assertThat(where).containsIgnoringCase("kafka_partition");
        assertThat(where).containsIgnoringCase("kafka_offset");
        assertThat(where).containsIgnoringCase("and");
    }

    // =========================================================================
    // Compound predicate pushdown (and / or / not)
    // =========================================================================

    @Test
    void compoundAnd_allEligibleLeavesPushedDown() {
        var predicate = new Predicate.And(List.of(
                new Predicate.Leaf("$.partition", EQ, 3),
                new Predicate.Leaf("$.offset", LT, 100)));
        var fds    = new FilterDropStep(predicate);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        assertThat(where).containsIgnoringCase("kafka_partition");
        assertThat(where).containsIgnoringCase("kafka_offset");
        assertThat(where).containsAnyOf("not", "NOT");
    }

    @Test
    void compoundOr_allEligibleLeavesPushedDown() {
        var predicate = new Predicate.Or(List.of(
                new Predicate.Leaf("$.partition", EQ, 0),
                new Predicate.Leaf("$.partition", EQ, 1)));
        var fds    = new FilterDropStep(predicate);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        assertThat(where).containsIgnoringCase("kafka_partition");
        assertThat(where).containsAnyOf("not", "NOT");
    }

    @Test
    void compoundNot_eligibleInnerLeafPushedDown() {
        // NOT(EQ(partition, 3)) → drop when partition != 3 → keep when partition = 3
        var predicate = new Predicate.Not(new Predicate.Leaf("$.partition", EQ, 3));
        var fds    = new FilterDropStep(predicate);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        assertThat(sql(result.pushdownCondition())).containsIgnoringCase("kafka_partition");
    }

    @Test
    void compoundAnd_mixedEligibleIneligibleRemainsInJava() {
        var predicate = new Predicate.And(List.of(
                new Predicate.Leaf("$.partition", EQ, 3),
                new Predicate.Leaf("$.value.status", EQ, "done")));
        var fds    = new FilterDropStep(predicate);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).containsExactly(fds);
        assertThat(sql(result.pushdownCondition())).isEqualToIgnoringCase("true");
    }

    @Test
    void compoundOr_withHeaderFieldRemainsInJava() {
        var predicate = new Predicate.Or(List.of(
                new Predicate.Leaf("$.partition", EQ, 0),
                new Predicate.Leaf("$.headers[x-env]", EQ, "staging")));
        var fds    = new FilterDropStep(predicate);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).containsExactly(fds);
    }

    @Test
    void compoundNested_allTopLevelLeavesPushedDown() {
        var inner = new Predicate.Or(List.of(
                new Predicate.Leaf("$.topic", EQ, "a"),
                new Predicate.Leaf("$.topic", EQ, "b")));
        var predicate = new Predicate.And(List.of(
                new Predicate.Leaf("$.partition", EQ, 0),
                inner));
        var fds    = new FilterDropStep(predicate);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), ENTITY_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        assertThat(where).containsIgnoringCase("kafka_partition");
        assertThat(where).containsIgnoringCase("topic");
    }

    @Test
    void compoundNested_deeplyIneligibleLeafBubblesUp() {
        var deepIneligible = new Predicate.Or(List.of(
                new Predicate.Leaf("$.value.x", EQ, "y"),
                new Predicate.Leaf("$.topic", EQ, "t")));
        var predicate = new Predicate.And(List.of(
                new Predicate.Leaf("$.partition", EQ, 0),
                new Predicate.Not(deepIneligible)));
        var fds    = new FilterDropStep(predicate);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), ENTITY_ELIGIBLE);

        assertThat(result.remainingSteps()).containsExactly(fds);
    }

    @Test
    void compoundAndEligible_combinedWithOtherEligibleLeafStep() {
        var compound = new FilterDropStep(new Predicate.And(List.of(
                new Predicate.Leaf("$.partition", EQ, 2),
                new Predicate.Leaf("$.offset", GTE, 50))));
        var leafStep = fds("$.key", IS_NOT_NULL, null);

        var result = SqlPushdownAnalyzer.analyze(List.of(compound, leafStep), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        assertThat(where).containsIgnoringCase("kafka_partition");
        assertThat(where).containsIgnoringCase("kafka_key");
    }

    // =========================================================================
    // Empty / IDENTITY cases
    // =========================================================================

    @Test
    void emptySteps_noConditionAndNoRemainingSteps() {
        var result = SqlPushdownAnalyzer.analyze(List.of(), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        assertThat(sql(result.pushdownCondition())).isEqualToIgnoringCase("true");
    }

    @Test
    void identityPipeline_hasNoSteps() {
        var result = SqlPushdownAnalyzer.analyze(
                TransformPipeline.IDENTITY.steps(), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        assertThat(sql(result.pushdownCondition())).isEqualToIgnoringCase("true");
    }
}

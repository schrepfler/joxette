package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.TransformPipeline;
import com.joxette.replay.transform.TransformStep;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static com.joxette.replay.transform.steps.FilterDropStep.Operator.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SqlPushdownAnalyzer}.
 *
 * <p>Tests verify:
 * <ul>
 *   <li>Eligible steps are pushed down and removed from remaining steps.</li>
 *   <li>Ineligible steps (nested {@code $.value.*}, non-pushdown operators on
 *       numeric columns) remain in Java.</li>
 *   <li>The generated WHERE fragment is semantically correct (drop when matches
 *       → negate in SQL).</li>
 *   <li>The order of remaining steps is preserved.</li>
 *   <li>The IDENTITY pipeline case (empty steps) is handled.</li>
 * </ul>
 */
class SqlPushdownAnalyzerTest {

    /** Eligible fields for the entity cassette service (includes $.topic). */
    private static final Set<String> ENTITY_ELIGIBLE = Set.of(
            "$.topic", "$.partition", "$.offset", "$.timestamp", "$.key", "$.recorded_at");

    /** Eligible fields for the general cassette service (no $.topic column). */
    private static final Set<String> TOPIC_ELIGIBLE = Set.of(
            "$.partition", "$.offset", "$.timestamp", "$.key", "$.recorded_at");

    private static String sql(org.jooq.Condition c) {
        return DSL.using(SQLDialect.DEFAULT).render(c);
    }

    // =========================================================================
    // Basic pushdown
    // =========================================================================

    @Test
    void eligible_partitionEqPushedDown() {
        var fds = new FilterDropStep("$.partition", EQ, 3);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        // EQ → negate → !=
        assertThat(where).containsIgnoringCase("kafka_partition")
                         .containsAnyOf("!=", "<>", "not");
    }

    @Test
    void eligible_offsetGtePushedDown() {
        var fds = new FilterDropStep("$.offset", GTE, 100);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        // GTE drops when >= 100 → SQL WHERE < 100
        assertThat(where).containsIgnoringCase("kafka_offset");
        assertThat(where).contains("100");
    }

    @Test
    void eligible_topicEqPushedDownForEntityService() {
        var fds = new FilterDropStep("$.topic", EQ, "audit.log");
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), ENTITY_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        assertThat(where).containsIgnoringCase("topic");
    }

    @Test
    void eligible_isNullPushedDown() {
        var fds = new FilterDropStep("$.key", IS_NULL, null);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        // IS_NULL drops when null → SQL WHERE IS NOT NULL
        assertThat(where).containsIgnoringCase("kafka_key")
                         .containsIgnoringCase("is not null");
    }

    @Test
    void eligible_isNotNullPushedDown() {
        var fds = new FilterDropStep("$.key", IS_NOT_NULL, null);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        assertThat(where).containsIgnoringCase("kafka_key")
                         .containsIgnoringCase("is null");
    }

    // =========================================================================
    // Ineligible cases (remain in Java)
    // =========================================================================

    @Test
    void ineligible_nestedValueFieldRemainsInJava() {
        var fds = new FilterDropStep("$.value.status", EQ, "cancelled");
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), ENTITY_ELIGIBLE);

        assertThat(result.remainingSteps()).containsExactly(fds);
        // pushdown condition should be identity (no extra WHERE fragment)
        assertThat(sql(result.pushdownCondition())).isEqualToIgnoringCase("true");
    }

    @Test
    void ineligible_topicFieldRemainsInJavaForGeneralCassetteService() {
        // $.topic is not in TOPIC_ELIGIBLE (no column in general cassette tables)
        var fds = new FilterDropStep("$.topic", EQ, "orders");
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).containsExactly(fds);
    }

    @Test
    void ineligible_matchesOnNumericColumnRemainsInJava() {
        // MATCHES only pushed down for string columns; partition is numeric
        var fds = new FilterDropStep("$.partition", MATCHES, "^3$");
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).containsExactly(fds);
    }

    @Test
    void ineligible_containsOnNumericColumnRemainsInJava() {
        var fds = new FilterDropStep("$.offset", CONTAINS, "5");
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).containsExactly(fds);
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
        var fdsPartition  = new FilterDropStep("$.partition", EQ, 3);           // eligible
        var fdsValueField = new FilterDropStep("$.value.status", EQ, "done");   // ineligible (nested)
        var redirect      = new RedirectTopicStep("orders-staging");             // ineligible (not FilterDrop)

        var result = SqlPushdownAnalyzer.analyze(
                List.of(fdsPartition, fdsValueField, redirect), TOPIC_ELIGIBLE);

        // Only the partition step was pushed down
        assertThat(result.remainingSteps())
                .containsExactly(fdsValueField, redirect);

        String where = sql(result.pushdownCondition());
        assertThat(where).containsIgnoringCase("kafka_partition");
    }

    @Test
    void mixed_orderOfRemainingStepsPreserved() {
        var a = new RedirectTopicStep("a");
        var b = new FilterDropStep("$.value.x", EQ, "y");   // ineligible
        var c = new RemoveHeaderStep("x-trace");
        var d = new FilterDropStep("$.partition", EQ, 5);   // eligible → pushed down
        var e = new AddHeaderStep("x-env", "test", false);

        var result = SqlPushdownAnalyzer.analyze(
                List.of(a, b, c, d, e), TOPIC_ELIGIBLE);

        // d was pushed down; a, b, c, e remain in order
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
        var fds1 = new FilterDropStep("$.partition", EQ, 3);
        var fds2 = new FilterDropStep("$.offset", LT, 10);

        var result = SqlPushdownAnalyzer.analyze(List.of(fds1, fds2), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        assertThat(where).containsIgnoringCase("kafka_partition");
        assertThat(where).containsIgnoringCase("kafka_offset");
        assertThat(where).containsIgnoringCase("and");
    }

    // =========================================================================
    // String column CONTAINS / MATCHES pushdown
    // =========================================================================

    @Test
    void containsOnStringColumnPushedDown() {
        var fds = new FilterDropStep("$.topic", CONTAINS, "staging");
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), ENTITY_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        assertThat(where).containsIgnoringCase("topic");
        assertThat(where).containsIgnoringCase("staging");
    }

    @Test
    void matchesOnStringColumnPushedDown() {
        var fds = new FilterDropStep("$.key", MATCHES, "^order-.*");
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        assertThat(where).containsIgnoringCase("kafka_key");
        // Should negate the regexp_matches call
        assertThat(where).containsAnyOf("not", "NOT");
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

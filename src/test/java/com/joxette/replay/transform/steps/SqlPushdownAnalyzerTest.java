package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.Predicate;
import com.joxette.replay.transform.TransformPipeline;
import com.joxette.replay.transform.TransformStep;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static com.joxette.replay.transform.Predicate.Operator.*;
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

    private static FilterDropStep fds(String field, Predicate.Operator op, Object value) {
        return new FilterDropStep(new Predicate.Leaf(field, op, value));
    }

    private static String sql(org.jooq.Condition c) {
        return DSL.using(SQLDialect.DEFAULT).render(c);
    }

    // =========================================================================
    // Basic pushdown
    // =========================================================================

    @Test
    void eligible_partitionEqPushedDown() {
        var fds = fds("$.partition", EQ, 3);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        // EQ → negate → !=
        assertThat(where).containsIgnoringCase("kafka_partition")
                         .containsAnyOf("!=", "<>", "not");
    }

    @Test
    void eligible_offsetGtePushedDown() {
        var fds = fds("$.offset", GTE, 100);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        // GTE drops when >= 100 → SQL WHERE < 100
        assertThat(where).containsIgnoringCase("kafka_offset");
        assertThat(where).contains("100");
    }

    @Test
    void eligible_topicEqPushedDownForEntityService() {
        var fds = fds("$.topic", EQ, "audit.log");
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), ENTITY_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        assertThat(where).containsIgnoringCase("topic");
    }

    @Test
    void eligible_isNullPushedDown() {
        var fds = fds("$.key", IS_NULL, null);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        // IS_NULL drops when null → SQL WHERE IS NOT NULL
        assertThat(where).containsIgnoringCase("kafka_key")
                         .containsIgnoringCase("is not null");
    }

    @Test
    void eligible_isNotNullPushedDown() {
        var fds = fds("$.key", IS_NOT_NULL, null);
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
        var fds = fds("$.value.status", EQ, "cancelled");
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), ENTITY_ELIGIBLE);

        assertThat(result.remainingSteps()).containsExactly(fds);
        // pushdown condition should be identity (no extra WHERE fragment)
        assertThat(sql(result.pushdownCondition())).isEqualToIgnoringCase("true");
    }

    @Test
    void ineligible_topicFieldRemainsInJavaForGeneralCassetteService() {
        // $.topic is not in TOPIC_ELIGIBLE (no column in general cassette tables)
        var fds = fds("$.topic", EQ, "orders");
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).containsExactly(fds);
    }

    @Test
    void ineligible_matchesOnNumericColumnRemainsInJava() {
        // MATCHES only pushed down for string columns; partition is numeric
        var fds = fds("$.partition", MATCHES, "^3$");
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).containsExactly(fds);
    }

    @Test
    void ineligible_containsOnNumericColumnRemainsInJava() {
        var fds = fds("$.offset", CONTAINS, "5");
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
        var fdsPartition  = fds("$.partition", EQ, 3);           // eligible
        var fdsValueField = fds("$.value.status", EQ, "done");   // ineligible (nested)
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
        var b = fds("$.value.x", EQ, "y");   // ineligible
        var c = new RemoveHeaderStep("x-trace");
        var d = fds("$.partition", EQ, 5);   // eligible → pushed down
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
    // String column CONTAINS / MATCHES pushdown
    // =========================================================================

    @Test
    void containsOnStringColumnPushedDown() {
        var fds = fds("$.topic", CONTAINS, "staging");
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), ENTITY_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        assertThat(where).containsIgnoringCase("topic");
        assertThat(where).containsIgnoringCase("staging");
    }

    @Test
    void matchesOnStringColumnPushedDown() {
        var fds = fds("$.key", MATCHES, "^order-.*");
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).isEmpty();
        String where = sql(result.pushdownCondition());
        assertThat(where).containsIgnoringCase("kafka_key");
        // Should negate the regexp_matches call
        assertThat(where).containsAnyOf("not", "NOT");
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
        // Positive condition: (partition=3 AND offset<100), negated for the keep clause
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
        String where = sql(result.pushdownCondition());
        assertThat(where).containsIgnoringCase("kafka_partition");
    }

    @Test
    void compoundAnd_mixedEligibleIneligibleRemainsInJava() {
        // One leaf references $.value.status which is not a top-level column → whole AND ineligible
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
        // $.headers[x-env] is not a top-level column → ineligible
        var predicate = new Predicate.Or(List.of(
                new Predicate.Leaf("$.partition", EQ, 0),
                new Predicate.Leaf("$.headers[x-env]", EQ, "staging")));
        var fds    = new FilterDropStep(predicate);
        var result = SqlPushdownAnalyzer.analyze(List.of(fds), TOPIC_ELIGIBLE);

        assertThat(result.remainingSteps()).containsExactly(fds);
    }

    @Test
    void compoundNested_allTopLevelLeavesPushedDown() {
        // AND( EQ(partition,0), OR( EQ(topic,"a"), EQ(topic,"b") ) ) — all leaves eligible
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
        // Deeply nested: AND( EQ(partition,0), NOT( OR( EQ($.value.x,"y"), EQ(topic,"t") ) ) )
        // $.value.x is ineligible → whole compound stays in Java
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

        // Both steps pushed down; remaining should be empty
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

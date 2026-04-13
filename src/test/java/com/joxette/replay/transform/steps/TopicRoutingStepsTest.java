package com.joxette.replay.transform.steps;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformPipeline;
import com.joxette.replay.transform.TransformStep;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

import com.joxette.replay.transform.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RedirectTopicStep} and {@link FanOutStep} dispatched
 * through {@link TransformPipeline}.
 */
class TopicRoutingStepsTest {

    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();

    private static String b64(String s) {
        return ENC.encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static CassetteRecord record() {
        Instant ts = Instant.parse("2024-01-01T00:00:00Z");
        return new CassetteRecord("orders", 0, 1L, ts, ts, "key-1", null, null, null);
    }

    private static TransformPipeline pipeline(TransformStep... steps) {
        return new TransformPipeline(List.of(steps), null);
    }

    // =========================================================================
    // RedirectTopicStep
    // =========================================================================

    @Test
    void redirectTopic_setsTopic() {
        var step   = new RedirectTopicStep("orders-staging");
        var result = pipeline(step).apply(new ReplayMessage(record()), "r1");

        assertThat(result).hasSize(1);
        assertThat(result.getFirst().topic).isEqualTo("orders-staging");
    }

    @Test
    void redirectTopic_templateResolvesPartitionField() {
        var step   = new RedirectTopicStep("orders-p${$.partition}");
        var result = pipeline(step).apply(new ReplayMessage(record()), "r1");

        assertThat(result.getFirst().topic).isEqualTo("orders-p0");
    }

    @Test
    void redirectTopic_templateResolvesValueField() {
        Instant ts  = Instant.parse("2024-01-01T00:00:00Z");
        String  val = b64("{\"env\":\"staging\"}");
        var rec  = new CassetteRecord("orders", 0, 1L, ts, ts, "k", val, null, null);
        var step   = new RedirectTopicStep("orders-${$.value.env}");
        var result = pipeline(step).apply(new ReplayMessage(rec), "r1");

        assertThat(result.getFirst().topic).isEqualTo("orders-staging");
    }

    @Test
    void redirectTopic_doesNotAffectOtherFields() {
        var original = record();
        var msg      = new ReplayMessage(original);
        var step     = new RedirectTopicStep("new-topic");
        var result   = pipeline(step).apply(msg, "r1");

        assertThat(result.getFirst().partition).isEqualTo(0);
        assertThat(result.getFirst().offset).isEqualTo(1L);
        assertThat(result.getFirst().key).isEqualTo("key-1");
    }

    // =========================================================================
    // FanOutStep
    // =========================================================================

    @Test
    void fanOut_producesOneCopyPerTopic() {
        var step   = new FanOutStep(List.of("topic-a", "topic-b", "topic-c"));
        var result = pipeline(step).apply(new ReplayMessage(record()), "r1");

        assertThat(result).hasSize(3);
        assertThat(result).extracting(m -> m.topic)
                .containsExactlyInAnyOrder("topic-a", "topic-b", "topic-c");
    }

    @Test
    void fanOut_copiesAreIndependent() {
        var step   = new FanOutStep(List.of("topic-a", "topic-b"));
        var result = pipeline(step).apply(new ReplayMessage(record()), "r1");

        // Mutating one copy should not affect the other
        result.getFirst().topic = "mutated";
        assertThat(result.getLast().topic).isNotEqualTo("mutated");
    }

    @Test
    void fanOut_copiesPreserveOtherFields() {
        var step   = new FanOutStep(List.of("topic-a", "topic-b"));
        var result = pipeline(step).apply(new ReplayMessage(record()), "r1");

        assertThat(result).allSatisfy(m -> {
            assertThat(m.partition).isEqualTo(0);
            assertThat(m.offset).isEqualTo(1L);
            assertThat(m.key).isEqualTo("key-1");
        });
    }

    @Test
    void fanOut_emptyTopicListProducesNoOutput() {
        var step   = new FanOutStep(List.of());
        var result = pipeline(step).apply(new ReplayMessage(record()), "r1");

        assertThat(result).isEmpty();
    }

    @Test
    void fanOut_followedByHeaderStepAppliedToAllCopies() {
        var fanOut    = new FanOutStep(List.of("topic-a", "topic-b"));
        var addHeader = new AddHeaderStep("x-env", "test", false);
        var result    = pipeline(fanOut, addHeader).apply(new ReplayMessage(record()), "r1");

        assertThat(result).hasSize(2);
        assertThat(result).allSatisfy(m ->
            assertThat(m.headers).anyMatch(h -> "x-env".equals(h.key()) && "test".equals(h.value())));
    }

    @Test
    void fanOut_followedByFilterDropRemovesSomeCopies() {
        var fanOut     = new FanOutStep(List.of("keep-topic", "drop-topic"));
        // Drop messages whose topic equals "drop-topic"
        var filterDrop = new FilterDropStep(new Predicate.Leaf("$.topic", Predicate.Operator.EQ, "drop-topic"));
        var result     = pipeline(fanOut, filterDrop).apply(new ReplayMessage(record()), "r1");

        assertThat(result).hasSize(1);
        assertThat(result.getFirst().topic).isEqualTo("keep-topic");
    }

    // =========================================================================
    // Single-topic fan-out (edge case)
    // =========================================================================

    @Test
    void fanOut_singleTopicBehavesLikeRedirect() {
        var step   = new FanOutStep(List.of("orders-staging"));
        var result = pipeline(step).apply(new ReplayMessage(record()), "r1");

        assertThat(result).hasSize(1);
        assertThat(result.getFirst().topic).isEqualTo("orders-staging");
    }
}

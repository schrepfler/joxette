package com.joxette.replay.transform;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.steps.AddHeaderStep;
import com.joxette.replay.transform.steps.ConditionalStep;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static com.joxette.replay.transform.Predicate.Operator.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the per-step {@code when} guard on {@link TransformStep}.
 *
 * <p>The guard is surfaced via {@link GuardedStep}: a transparent wrapper produced by
 * {@link TransformStepDeserializer} at parse time (or constructed directly here). The
 * pipeline evaluates the guard before dispatching to the delegate; a false guard causes
 * the message to pass through unchanged.
 *
 * <p>{@link AddHeaderStep} is used as the observable effect throughout — presence or
 * absence of the injected header proves whether the delegate ran.
 */
class WhenGuardTest {

    private static final Instant TS = Instant.parse("2024-06-01T12:00:00Z");

    private static ReplayMessage msg(String topic) {
        CassetteRecord r = new CassetteRecord(topic, 0, 0L, TS, TS, "k", null, null, null);
        return new ReplayMessage(r);
    }

    /** Guard that matches when topic = "orders". */
    private static Predicate truePred() {
        return new Predicate.Leaf("$.topic", EQ, "orders");
    }

    /** Guard that never matches (topic = "never-matches"). */
    private static Predicate falsePred() {
        return new Predicate.Leaf("$.topic", EQ, "never-matches");
    }

    private static List<ReplayMessage> run(List<TransformStep> steps, ReplayMessage m) {
        return new TransformPipeline(steps, null).apply(m, "rid");
    }

    // =========================================================================
    // when = null — step always runs
    // =========================================================================

    @Test
    void whenNull_stepAlwaysRuns() {
        // A plain step (not wrapped in GuardedStep) has when()=null → runs unconditionally
        var step   = new AddHeaderStep("x-ran", "yes", false);
        var result = run(List.of(step), msg("orders"));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers)
                .anyMatch(h -> "x-ran".equals(h.key()) && "yes".equals(h.value()));
    }

    @Test
    void whenNull_stepRunsForAnyTopic() {
        // Unguarded step executes even when topic does not match any predicate
        var step   = new AddHeaderStep("x-ran", "yes", false);
        var result = run(List.of(step), msg("completely-different-topic"));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers).anyMatch(h -> "x-ran".equals(h.key()));
    }

    // =========================================================================
    // when = true predicate — step runs, message mutated
    // =========================================================================

    @Test
    void whenTrue_stepRunsAndMutatesMessage() {
        var guarded = new GuardedStep(truePred(), new AddHeaderStep("x-ran", "yes", false));
        var result  = run(List.of(guarded), msg("orders")); // truePred matches

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers)
                .anyMatch(h -> "x-ran".equals(h.key()) && "yes".equals(h.value()));
    }

    // =========================================================================
    // when = false predicate — step skipped, message unchanged
    // =========================================================================

    @Test
    void whenFalse_stepSkippedAndMessageUnchanged() {
        var guarded = new GuardedStep(falsePred(), new AddHeaderStep("x-ran", "yes", false));
        var result  = run(List.of(guarded), msg("orders")); // falsePred does not match

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers).isEmpty(); // no mutation whatsoever
    }

    // =========================================================================
    // when = compound AND with one false leg — step skipped
    // =========================================================================

    @Test
    void whenCompoundAndOneFalse_stepSkipped() {
        // AND(topic=orders, partition=99) — msg has partition=0, so AND is false
        var guard = new Predicate.And(List.of(
                new Predicate.Leaf("$.topic", EQ, "orders"),
                new Predicate.Leaf("$.partition", EQ, 99)));
        var guarded = new GuardedStep(guard, new AddHeaderStep("x-ran", "yes", false));
        var result  = run(List.of(guarded), msg("orders")); // partition=0, not 99

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers).noneMatch(h -> "x-ran".equals(h.key()));
    }

    // =========================================================================
    // Two-step pipeline: step1 when=false, step2 when=true — only step2 mutates
    // =========================================================================

    @Test
    void twoStep_step1WhenFalse_step2WhenTrue_onlyStep2Mutates() {
        var step1 = new GuardedStep(falsePred(), new AddHeaderStep("x-step1", "v1", false));
        var step2 = new GuardedStep(truePred(),  new AddHeaderStep("x-step2", "v2", false));
        var result = run(List.of(step1, step2), msg("orders"));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers).noneMatch(h -> "x-step1".equals(h.key()));
        assertThat(result.get(0).headers)
                .anyMatch(h -> "x-step2".equals(h.key()) && "v2".equals(h.value()));
    }

    // =========================================================================
    // ConditionalStep with when=false — entire branch skipped
    // =========================================================================

    @Test
    void conditionalStepWithWhenFalse_branchNotExecuted() {
        // ConditionalStep condition matches (topic=orders), but the outer when guard is false
        // → the ConditionalStep itself is skipped, neither then- nor else-branch runs
        var thenStep    = new AddHeaderStep("x-conditional", "ran", false);
        var conditional = new ConditionalStep(
                new Predicate.Leaf("$.topic", EQ, "orders"), // always true for our msg
                List.of(thenStep), List.of());
        var guarded = new GuardedStep(falsePred(), conditional); // outer when=false

        var result = run(List.of(guarded), msg("orders"));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers).noneMatch(h -> "x-conditional".equals(h.key()));
    }

    @Test
    void conditionalStepWithWhenTrue_branchExecutes() {
        // Inverse: when the outer guard passes, the ConditionalStep's condition is evaluated
        // and the matching branch runs normally
        var thenStep    = new AddHeaderStep("x-conditional", "ran", false);
        var conditional = new ConditionalStep(
                new Predicate.Leaf("$.topic", EQ, "orders"),
                List.of(thenStep), List.of());
        var guarded = new GuardedStep(truePred(), conditional); // outer when=true

        var result = run(List.of(guarded), msg("orders"));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).headers)
                .anyMatch(h -> "x-conditional".equals(h.key()) && "ran".equals(h.value()));
    }
}

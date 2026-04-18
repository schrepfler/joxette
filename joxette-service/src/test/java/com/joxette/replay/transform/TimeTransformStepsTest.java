package com.joxette.replay.transform;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.EntityRecord;
import com.joxette.replay.transform.steps.TimeCompressStep;
import com.joxette.replay.transform.steps.TimeFreezeStep;
import com.joxette.replay.transform.steps.TimeShiftStep;
import com.joxette.replay.transform.steps.WallTimeStep;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the four time transformation steps:
 * {@code wall_time}, {@code time_shift}, {@code time_compress}, {@code time_freeze}.
 */
class TimeTransformStepsTest {

    private static final Instant BASE     = Instant.parse("2025-01-15T10:30:00Z");
    private static final Instant RECORDED = Instant.parse("2025-01-15T10:30:00.456Z");

    // =========================================================================
    // Test helpers
    // =========================================================================

    private static ReplayMessage msg() {
        return msg(BASE, RECORDED);
    }

    private static ReplayMessage msg(Instant ts, Instant recordedAt) {
        var r = new CassetteRecord("orders", 0, 1L, ts, recordedAt, "key-1", null, null, null);
        return new ReplayMessage(r);
    }

    private static ReplayMessage entityMsg(Instant ts) {
        var r = new EntityRecord("entity-1", "orderEvent", "orders", 0, 1L, ts, RECORDED,
                "key-1", null, null);
        return new ReplayMessage(r);
    }

    /** Pipeline with null injector (IDENTITY-like injector but with real steps). */
    private static TransformPipeline pipelineOf(TransformStep... steps) {
        return new TransformPipeline(List.of(steps), null);
    }

    // =========================================================================
    // WallTimeStep
    // =========================================================================

    @Test
    void wallTime_replacesTimestamp() {
        var msg = msg();
        Instant before = Instant.now();

        pipelineOf(new WallTimeStep("$.timestamp")).apply(msg, "r1");

        assertThat(msg.timestamp).isAfterOrEqualTo(before);
        assertThat(msg.recordedAt).isEqualTo(RECORDED);  // unchanged
    }

    @Test
    void wallTime_replacesRecordedAt() {
        var msg = msg();
        Instant before = Instant.now();

        pipelineOf(new WallTimeStep("$.recorded_at")).apply(msg, "r1");

        assertThat(msg.recordedAt).isAfterOrEqualTo(before);
        assertThat(msg.timestamp).isEqualTo(BASE);  // unchanged
    }

    @Test
    void wallTime_defaultsToTimestamp_whenNullTarget() {
        // WallTimeStep normalizes null → "$.timestamp"
        var msg = msg();
        Instant before = Instant.now();

        pipelineOf(new WallTimeStep(null)).apply(msg, "r1");

        assertThat(msg.timestamp).isAfterOrEqualTo(before);
        assertThat(msg.recordedAt).isEqualTo(RECORDED);  // unchanged
    }

    @Test
    void wallTime_allTimestamps_replacesTimestampAndRecordedAt() {
        var msg = msg();
        Instant before = Instant.now();

        pipelineOf(new WallTimeStep("ALL_TIMESTAMPS")).apply(msg, "r1");

        assertThat(msg.timestamp).isAfterOrEqualTo(before);
        assertThat(msg.recordedAt).isAfterOrEqualTo(before);
    }

    @Test
    void wallTime_sameInstantForBothFields_allTimestamps() {
        var msg = msg();

        pipelineOf(new WallTimeStep("ALL_TIMESTAMPS")).apply(msg, "r1");

        // Both fields replaced in the same apply() call — may differ by at most a few ns
        // from consecutive Instant.now() calls in the same step invocation
        assertThat(Duration.between(msg.timestamp, msg.recordedAt).abs())
                .isLessThan(Duration.ofSeconds(1));
    }

    @Test
    void wallTime_worksForEntityRecord() {
        var msg = entityMsg(BASE);
        Instant before = Instant.now();

        pipelineOf(new WallTimeStep("$.timestamp")).apply(msg, "r1");

        assertThat(msg.timestamp).isAfterOrEqualTo(before);
    }

    // =========================================================================
    // TimeShiftStep
    // =========================================================================

    @Test
    void timeShift_forwardByOneDay() {
        var msg = msg();

        pipelineOf(new TimeShiftStep("$.timestamp", Duration.ofDays(1).toMillis()))
                .apply(msg, "r1");

        assertThat(msg.timestamp).isEqualTo(BASE.plus(Duration.ofDays(1)));
        assertThat(msg.recordedAt).isEqualTo(RECORDED);  // unchanged
    }

    @Test
    void timeShift_backwardByOneHour() {
        var msg = msg();

        pipelineOf(new TimeShiftStep("$.timestamp", -Duration.ofHours(1).toMillis()))
                .apply(msg, "r1");

        assertThat(msg.timestamp).isEqualTo(BASE.minus(Duration.ofHours(1)));
    }

    @Test
    void timeShift_zeroIsNoop() {
        var msg = msg();

        pipelineOf(new TimeShiftStep("$.timestamp", 0L)).apply(msg, "r1");

        assertThat(msg.timestamp).isEqualTo(BASE);
    }

    @Test
    void timeShift_allTimestamps_shiftsBothEnvelopeFields() {
        var msg = msg();
        long deltaMs = Duration.ofMinutes(5).toMillis();

        pipelineOf(new TimeShiftStep("ALL_TIMESTAMPS", deltaMs)).apply(msg, "r1");

        assertThat(msg.timestamp).isEqualTo(BASE.plus(Duration.ofMinutes(5)));
        assertThat(msg.recordedAt).isEqualTo(RECORDED.plus(Duration.ofMinutes(5)));
    }

    @Test
    void timeShift_allTimestamps_shiftsTimestampHeaders() {
        var headers = new ArrayList<CassetteRecord.Header>();
        headers.add(new CassetteRecord.Header("x-event-time", "2025-01-15T10:00:00Z"));
        headers.add(new CassetteRecord.Header("x-trace-id",   "abc-123"));  // non-timestamp
        var r = new CassetteRecord("t", 0, 1L, BASE, RECORDED, null, null, headers, null);
        var msg = new ReplayMessage(r);

        pipelineOf(new TimeShiftStep("ALL_TIMESTAMPS", 3_600_000L)).apply(msg, "r1");

        assertThat(msg.headers.get(0).value()).isEqualTo("2025-01-15T11:00:00Z");
        assertThat(msg.headers.get(1).value()).isEqualTo("abc-123");  // unchanged
    }

    @Test
    void timeShift_recordedAt_onlyShiftsRecordedAt() {
        var msg = msg();

        pipelineOf(new TimeShiftStep("$.recorded_at", 60_000L)).apply(msg, "r1");

        assertThat(msg.timestamp).isEqualTo(BASE);  // unchanged
        assertThat(msg.recordedAt).isEqualTo(RECORDED.plus(Duration.ofMinutes(1)));
    }

    @Test
    void timeShift_unknownTarget_isNoop() {
        var msg = msg();

        pipelineOf(new TimeShiftStep("$.some_other_field", 99_999L)).apply(msg, "r1");

        assertThat(msg.timestamp).isEqualTo(BASE);
        assertThat(msg.recordedAt).isEqualTo(RECORDED);
    }

    // =========================================================================
    // TimeCompressStep
    // =========================================================================

    @Test
    void timeCompress_firstMessageNeverSleeps() {
        var ctx = new TransformContext();

        pipelineOf(new TimeCompressStep("$.timestamp", 2.0))
                .apply(msg(), "r1", ctx);

        assertThat(ctx.getPendingSleep()).isEqualTo(Duration.ZERO);
        assertThat(ctx.getCompressAnchorMsgTs()).isEqualTo(BASE);
    }

    @Test
    void timeCompress_anchorSetOnFirstMessage() {
        var ctx = new TransformContext();

        pipelineOf(new TimeCompressStep("$.timestamp", 4.0))
                .apply(msg(BASE, RECORDED), "r1", ctx);

        assertThat(ctx.getCompressAnchorMsgTs()).isEqualTo(BASE);
        assertThat(ctx.getCompressAnchorWallTs()).isNotNull();
    }

    @Test
    void timeCompress_secondMessage_computesPositiveSleep() {
        var ctx = new TransformContext();
        var pipeline = pipelineOf(new TimeCompressStep("$.timestamp", 2.0));

        pipeline.apply(msg(BASE, RECORDED), "r1", ctx);
        // 2-hour event gap at factor=2.0 → expect ~1h sleep
        pipeline.apply(msg(BASE.plus(Duration.ofHours(2)), RECORDED), "r1", ctx);

        // Execution takes <1ms, so pendingSleep ≈ 3600s
        long sleepMs = ctx.getPendingSleep().toMillis();
        assertThat(sleepMs)
                .isGreaterThan(3_599_000L)
                .isLessThan(3_601_000L);
    }

    @Test
    void timeCompress_doesNotModifyMessageTimestamp() {
        var ctx = new TransformContext();

        pipelineOf(new TimeCompressStep("$.timestamp", 3.0))
                .apply(msg(), "r1", ctx);

        // time_compress controls pacing only — message timestamp is untouched
        assertThat(msg().timestamp).isEqualTo(BASE);
    }

    @Test
    void timeCompress_noSleepWhenGapIsZero() {
        var ctx = new TransformContext();
        var pipeline = pipelineOf(new TimeCompressStep("$.timestamp", 2.0));

        pipeline.apply(msg(BASE, RECORDED), "r1", ctx);
        pipeline.apply(msg(BASE, RECORDED), "r1", ctx);  // same timestamp

        assertThat(ctx.getPendingSleep()).isEqualTo(Duration.ZERO);
    }

    @Test
    void timeCompress_noSleepWhenGapIsNegative() {
        var ctx = new TransformContext();
        var pipeline = pipelineOf(new TimeCompressStep("$.timestamp", 2.0));

        pipeline.apply(msg(BASE.plus(Duration.ofMinutes(10)), RECORDED), "r1", ctx);
        // second message has earlier timestamp than first — negative gap → no sleep
        pipeline.apply(msg(BASE, RECORDED), "r1", ctx);

        assertThat(ctx.getPendingSleep()).isEqualTo(Duration.ZERO);
    }

    @Test
    void timeCompress_anchorFieldRecordedAt_usesRecordedAt() {
        var ctx = new TransformContext();
        var pipeline = pipelineOf(new TimeCompressStep("$.recorded_at", 2.0));

        pipeline.apply(msg(BASE, RECORDED), "r1", ctx);

        assertThat(ctx.getCompressAnchorMsgTs()).isEqualTo(RECORDED);
    }

    // =========================================================================
    // TimeFreezeStep
    // =========================================================================

    @Test
    void timeFreeze_fixedInstant_replacesTimestamp() {
        var msg = msg();
        String frozen = "2024-06-01T00:00:00Z";

        pipelineOf(new TimeFreezeStep("$.timestamp", frozen)).apply(msg, "r1");

        assertThat(msg.timestamp).isEqualTo(Instant.parse(frozen));
        assertThat(msg.recordedAt).isEqualTo(RECORDED);  // unchanged
    }

    @Test
    void timeFreeze_now_usesReplayStartedAt() {
        Instant replayStart = Instant.parse("2026-01-01T08:00:00Z");
        var ctx = new TransformContext(replayStart);
        var msg = msg();

        pipelineOf(new TimeFreezeStep("$.timestamp", "NOW"))
                .apply(msg, "r1", ctx);

        assertThat(msg.timestamp).isEqualTo(replayStart);
    }

    @Test
    void timeFreeze_nowCaseInsensitive() {
        Instant replayStart = Instant.parse("2026-03-15T12:00:00Z");
        var ctx = new TransformContext(replayStart);
        var msg = msg();

        pipelineOf(new TimeFreezeStep("$.timestamp", "now"))
                .apply(msg, "r1", ctx);

        assertThat(msg.timestamp).isEqualTo(replayStart);
    }

    @Test
    void timeFreeze_allTimestamps_freezesBothFields() {
        var msg = msg();
        String frozen = "2024-01-01T00:00:00Z";

        pipelineOf(new TimeFreezeStep("ALL_TIMESTAMPS", frozen)).apply(msg, "r1");

        assertThat(msg.timestamp).isEqualTo(Instant.parse(frozen));
        assertThat(msg.recordedAt).isEqualTo(Instant.parse(frozen));
    }

    @Test
    void timeFreeze_allTimestamps_freezesTimestampHeaders() {
        var headers = new ArrayList<CassetteRecord.Header>();
        headers.add(new CassetteRecord.Header("x-event-time", "2023-05-01T09:00:00Z"));
        headers.add(new CassetteRecord.Header("x-trace-id",   "xyz"));  // non-timestamp
        var r = new CassetteRecord("t", 0, 1L, BASE, RECORDED, null, null, headers, null);
        var msg = new ReplayMessage(r);

        String frozen = "2024-01-01T00:00:00Z";
        pipelineOf(new TimeFreezeStep("ALL_TIMESTAMPS", frozen)).apply(msg, "r1");

        assertThat(msg.headers.get(0).value()).isEqualTo(frozen);
        assertThat(msg.headers.get(1).value()).isEqualTo("xyz");  // unchanged
    }

    @Test
    void timeFreeze_isStableAcrossMultipleMessages() {
        String frozen = "2025-03-01T12:00:00Z";
        var pipeline = pipelineOf(new TimeFreezeStep("$.timestamp", frozen));

        for (int i = 0; i < 5; i++) {
            var msg = msg(BASE.plusSeconds(i * 60L), RECORDED);
            pipeline.apply(msg, "r1");
            assertThat(msg.timestamp).isEqualTo(Instant.parse(frozen));
        }
    }

    @Test
    void timeFreeze_recordedAt_target() {
        var msg = msg();
        String frozen = "2020-01-01T00:00:00Z";

        pipelineOf(new TimeFreezeStep("$.recorded_at", frozen)).apply(msg, "r1");

        assertThat(msg.timestamp).isEqualTo(BASE);  // unchanged
        assertThat(msg.recordedAt).isEqualTo(Instant.parse(frozen));
    }
}

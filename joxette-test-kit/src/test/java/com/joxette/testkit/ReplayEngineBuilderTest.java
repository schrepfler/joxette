package com.joxette.testkit;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.PartitionStrategy;
import com.joxette.replay.ReplayProgress;
import com.joxette.replay.ReplayToTopicRequest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the test-kit can drive {@link com.joxette.replay.ReplayEngine} with zero
 * Joxette-service classes on the classpath. The whole test runs in-process against
 * an {@link InMemoryCassetteSource} and a {@link CapturingRecordSink} — no DuckDB,
 * no Spring, no Kafka broker.
 */
class ReplayEngineBuilderTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    // =========================================================================
    // Speed multiplier variations
    // =========================================================================

    /**
     * (speed, sourceSpanMs, expectedMinMs, expectedMaxMs)
     *
     * Source cassette has 3 records with two equal gaps totalling sourceSpanMs.
     * Expected wall-clock ≈ sourceSpanMs / speed; bounds are generous for CI.
     */
    static Stream<Arguments> speedVariants() {
        return Stream.of(
                Arguments.of(0.5,  400L, 400L, 5000L),
                Arguments.of(1.0,  400L, 200L, 3000L),
                Arguments.of(2.0,  400L, 100L, 1500L),
                Arguments.of(10.0, 400L,  10L, 1000L)
        );
    }

    @ParameterizedTest(name = "speed={0}x sourceSpan={1}ms → [{2}ms,{3}ms]")
    @MethodSource("speedVariants")
    void replayHonoursSpeedMultiplier(double speed, long sourceSpanMs,
                                      long expectedMinMs, long expectedMaxMs) throws Exception {
        var cassette = new InMemoryCassetteSource()
                .add(record("topic", 0, 0L, T0,                              "k0", "dmFsdWUtMA"))
                .add(record("topic", 0, 1L, T0.plusMillis(sourceSpanMs / 2), "k1", "dmFsdWUtMQ"))
                .add(record("topic", 0, 2L, T0.plusMillis(sourceSpanMs),     "k2", "dmFsdWUtMg"));

        var sink = new CapturingRecordSink();
        var engine = ReplayEngineBuilder.create().cassetteSource(cassette).sink(sink).build();
        var req = new ReplayToTopicRequest("out", null, null, null, null, null, null, null, PartitionStrategy.DEFAULT);

        long startedAt = System.nanoTime();
        List<ReplayProgress> progressEvents = new ArrayList<>();
        engine.replayTopic("topic", req, speed, progressEvents::add);
        long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000;

        assertEquals(3, sink.sentCount(), "all three records produced");
        assertEquals(List.of("k0", "k1", "k2"),
                sink.sent().stream().map(CapturingRecordSink.SentRecord::key).toList());

        assertTrue(elapsedMs >= expectedMinMs,
                "expected ≥ " + expectedMinMs + "ms at speed " + speed + "x (actual " + elapsedMs + "ms)");
        assertTrue(elapsedMs < expectedMaxMs,
                "expected < " + expectedMaxMs + "ms at speed " + speed + "x (actual " + elapsedMs + "ms)");

        assertEquals("completed", progressEvents.getLast().status());
        assertEquals(3, progressEvents.getLast().sentCount());
    }

    // =========================================================================
    // Cassette size edge cases
    // =========================================================================

    static Stream<Arguments> cassetteSizeEdgeCases() {
        return Stream.of(
                // Empty: completed immediately, no records sent, no in_progress events
                Arguments.of("empty cassette",
                        List.<CassetteRecord>of(), 0),

                // Single record: no inter-message delay (prevTs is null for first record)
                Arguments.of("single record",
                        List.of(record("topic", 0, 0L, T0, "k0", "dmFsdWUtMA")), 1),

                // Multiple records: all three delivered in order
                Arguments.of("multiple records",
                        List.of(
                                record("topic", 0, 0L, T0,                 "k0", "dmFsdWUtMA"),
                                record("topic", 0, 1L, T0.plusMillis(200), "k1", "dmFsdWUtMQ"),
                                record("topic", 0, 2L, T0.plusMillis(400), "k2", "dmFsdWUtMg")
                        ), 3)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cassetteSizeEdgeCases")
    void replayCompletesCorrectly_forDifferentCassetteSizes(
            String name, List<CassetteRecord> records, int expectedCount) throws Exception {

        var cassette = new InMemoryCassetteSource().addAll(records);
        var sink = new CapturingRecordSink();
        var engine = ReplayEngineBuilder.create().cassetteSource(cassette).sink(sink).build();
        var req = new ReplayToTopicRequest("out", null, null, null, null, null, null, null, PartitionStrategy.DEFAULT);

        List<ReplayProgress> progressEvents = new ArrayList<>();
        engine.replayTopic("topic", req, 2.0, progressEvents::add);

        assertEquals(expectedCount, sink.sentCount(), "sent count for: " + name);
        // All cases have < 100 records, so only the final "completed" event is emitted
        assertEquals(1, progressEvents.size(), "only completed event expected for: " + name);
        assertEquals("completed", progressEvents.getLast().status());
        assertEquals(expectedCount, progressEvents.getLast().sentCount());
    }

    // =========================================================================
    // Helper
    // =========================================================================

    private static CassetteRecord record(
            String topic, int partition, long offset,
            Instant timestamp, String key, String base64Value
    ) {
        return new CassetteRecord(
                topic, partition, offset, timestamp, timestamp,
                key, base64Value, List.of(), null
        );
    }
}

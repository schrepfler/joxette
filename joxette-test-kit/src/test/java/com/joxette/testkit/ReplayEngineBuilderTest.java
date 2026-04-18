package com.joxette.testkit;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.ReplayProgress;
import com.joxette.replay.ReplayToTopicRequest;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the test-kit can drive {@link com.joxette.replay.ReplayEngine} with zero
 * Joxette-service classes on the classpath. The whole test runs in-process against
 * an {@link InMemoryCassetteSource} and a {@link CapturingRecordSink} — no DuckDB,
 * no Spring, no Kafka broker.
 */
class ReplayEngineBuilderTest {

    @Test
    void replaysTopicCassetteInOrder_withInterMessageDelay() throws Exception {
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");

        var cassette = new InMemoryCassetteSource()
                .add(record("topic", 0, 0L, t0,                     "k0", "dmFsdWUtMA"))
                .add(record("topic", 0, 1L, t0.plusMillis(200),     "k1", "dmFsdWUtMQ"))
                .add(record("topic", 0, 2L, t0.plusMillis(400),     "k2", "dmFsdWUtMg"));

        var sink = new CapturingRecordSink();
        var engine = ReplayEngineBuilder.create()
                .cassetteSource(cassette)
                .sink(sink)
                .build();

        var req = new ReplayToTopicRequest("out", null, null, null, null, null, null);

        long startedAt = System.nanoTime();
        List<ReplayProgress> progressEvents = new ArrayList<>();
        engine.replayTopic("topic", req, 2.0 /* speed */, progressEvents::add);
        long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000;

        // Correctness
        assertEquals(3, sink.sentCount(), "all three records produced");
        assertEquals(List.of("k0", "k1", "k2"),
                sink.sent().stream().map(CapturingRecordSink.SentRecord::key).toList());

        // Timing: at speed=2.0 the 400ms source span should elapse in ~200ms wall clock.
        // Generous lower bound (100ms) to stay flake-free under CI noise.
        assertTrue(elapsedMs >= 100, "inter-message delay applied (actual " + elapsedMs + "ms)");
        assertTrue(elapsedMs < 1500, "delay honoured speed multiplier (actual " + elapsedMs + "ms)");

        // Final progress event is "completed"
        assertEquals("completed", progressEvents.getLast().status());
        assertEquals(3, progressEvents.getLast().sentCount());
    }

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

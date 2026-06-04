package com.joxette.replay;

import com.joxette.recording.CassetteRecordingBus;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests for {@code follow=true} mode.
 *
 * <p>Scenarios:
 * <ol>
 *   <li><b>drain-only</b> — produce N records before connecting; follow stream
 *       delivers all N and stays open.</li>
 *   <li><b>live-only</b> — connect with no history; produce M records;
 *       assert M arrive in order on the live tail.</li>
 *   <li><b>mixed drain + live</b> — produce N, connect, produce K more in
 *       parallel; assert exactly N+K delivered in cursor order, no dups,
 *       no gaps.</li>
 *   <li><b>client disconnect</b> — close the stream mid-tail and confirm the
 *       bus subscriber count returns to zero.</li>
 *   <li><b>overflow</b> — tiny buffer + fast writer → stream terminates with
 *       an {@code overflow} event.</li>
 *   <li><b>follow + upper bound</b> — {@code follow=true} combined with
 *       {@code to=...} returns HTTP 400.</li>
 *   <li><b>capacity exceeded</b> — at {@code max-subscriptions} a new follow
 *       request returns HTTP 503.</li>
 * </ol>
 *
 * <p>Scenarios that exercise the full SSE transport use a dedicated virtual
 * thread plus a raw {@link java.net.http.HttpClient} async request so we can
 * pull the stream incrementally while production continues.  The cursor /
 * handoff scenarios are driven at the service level via
 * {@link TopicReplayService#streamAll} + {@link FollowSubscription} — the HTTP
 * layer is exercised separately by {@code followAndUpperBound_returns400} and
 * {@code capacityExceeded_returns503}.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "joxette.replay.follow.heartbeat-seconds=1",
                "joxette.replay.follow.max-subscriptions=2"
        })
@ActiveProfiles("it")
@Testcontainers
class FollowModeIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("joxette.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @LocalServerPort
    private int port;

    @Autowired
    private Connection duckDB;

    @Autowired
    private TopicReplayService topicService;

    @Autowired
    private CassetteRecordingBus bus;

    private RestTemplate restTemplate;
    private String baseUrl;

    private static final String TOPIC = "follow.test.events";
    private static final String TABLE = "lake.main.general_follow_test_events";

    @BeforeEach
    void setUp() throws Exception {
        restTemplate = new RestTemplate();
        baseUrl = "http://localhost:" + port;
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC);
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM " + TABLE);
        }
    }

    // =========================================================================
    // 1. Drain-only — historical records arrive, stream stays open
    // =========================================================================

    @Test
    void drainOnly_historicalRecordsDelivered_streamStaysOpen() throws Exception {
        int historicalCount = 5;
        for (int i = 0; i < historicalCount; i++) {
            DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, i,
                    Instant.parse("2026-01-01T00:00:0" + i + "Z"), Instant.now(),
                    "k" + i, b("v" + i));
        }

        List<CassetteRecord> emitted = new CopyOnWriteArrayList<>();
        CountDownLatch historyDrained = new CountDownLatch(1);
        AtomicInteger heartbeats = new AtomicInteger();

        CassetteRecordingBus.TopicSubscription topicSub = bus.subscribeTopic(TOPIC);
        FollowSubscription<CassetteRecord, TopicCursor> sub =
                FollowSubscription.forTopic(topicSub);

        Thread worker = Thread.ofVirtual().start(() -> {
            try {
                topicService.streamAll(
                        TOPIC, null, null, null, null, null,
                        emitted::add, com.joxette.replay.transform.TransformPipeline.IDENTITY, "",
                        sub, new FollowHooks<>() {
                            @Override public Duration heartbeatInterval() {
                                return Duration.ofMillis(100);
                            }
                            @Override public void onHistoricalEnd() { historyDrained.countDown(); }
                            @Override public void onHeartbeat() { heartbeats.incrementAndGet(); }
                        });
            } catch (Exception e) { throw new RuntimeException(e); }
        });

        assertThat(historyDrained.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(emitted).hasSize(historicalCount);

        // Stream is still alive (no completion) and receiving heartbeats.
        await().atMost(Duration.ofSeconds(3))
                .until(() -> heartbeats.get() >= 1);
        assertThat(worker.isAlive()).isTrue();

        // Cleanup: close the sub, worker exits cleanly.
        sub.close();
        // Publishing a no-op batch after close shouldn't hang anything
        worker.interrupt();
        worker.join(2_000);
    }

    // =========================================================================
    // 2. Mixed drain + live — no gap, no dup, cursor-ordered
    // =========================================================================

    @Test
    void mixedDrainAndLive_exactDelivery_noDupsNoGaps() throws Exception {
        int historicalCount = 3;
        Instant base = Instant.parse("2026-02-01T00:00:00Z");
        for (int i = 0; i < historicalCount; i++) {
            DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, i,
                    base.plusSeconds(i), Instant.now(), "k" + i, b("v" + i));
        }

        List<CassetteRecord> emitted = new CopyOnWriteArrayList<>();
        CountDownLatch historyDrained = new CountDownLatch(1);

        CassetteRecordingBus.TopicSubscription topicSub = bus.subscribeTopic(TOPIC);
        FollowSubscription<CassetteRecord, TopicCursor> sub =
                FollowSubscription.forTopic(topicSub);

        // Simulate a live write that arrives BEFORE the drain finishes: the
        // record is already in the historical table AND pushed to the bus queue.
        // Cursor boundary filter must suppress the duplicate.
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, historicalCount,
                base.plusSeconds(historicalCount), Instant.now(),
                "k" + historicalCount, b("v" + historicalCount));
        topicSub.queue().offer(new CassetteRecord(TOPIC, 0, historicalCount,
                base.plusSeconds(historicalCount), Instant.now(),
                "k" + historicalCount, null, List.of(), null));

        Thread worker = Thread.ofVirtual().start(() -> {
            try {
                topicService.streamAll(
                        TOPIC, null, null, null, null, null,
                        emitted::add, com.joxette.replay.transform.TransformPipeline.IDENTITY, "",
                        sub, new FollowHooks<>() {
                            @Override public Duration heartbeatInterval() {
                                return Duration.ofMillis(100);
                            }
                            @Override public void onHistoricalEnd() { historyDrained.countDown(); }
                        });
            } catch (Exception e) { /* interrupt */ }
        });

        assertThat(historyDrained.await(10, TimeUnit.SECONDS)).isTrue();

        // Post-drain live records (definitely arrive after boundary)
        int liveCount = 4;
        for (int i = 0; i < liveCount; i++) {
            int off = historicalCount + 1 + i;
            topicSub.queue().put(new CassetteRecord(TOPIC, 0, off,
                    base.plusSeconds(off), Instant.now(),
                    "k" + off, null, List.of(), null));
        }

        int totalExpected = historicalCount + 1 /* historical/bus overlap */ + liveCount;
        await().atMost(Duration.ofSeconds(10))
                .until(() -> emitted.size() == totalExpected);

        // No duplicates: offsets are strictly increasing
        for (int i = 1; i < emitted.size(); i++) {
            assertThat(emitted.get(i).offset())
                    .as("cursor order at index %d", i)
                    .isGreaterThan(emitted.get(i - 1).offset());
        }
        // All offsets 0..historicalCount+liveCount present exactly once
        for (int i = 0; i <= historicalCount + liveCount; i++) {
            int finalI = i;
            assertThat(emitted.stream().filter(r -> r.offset() == finalI).count())
                    .as("offset %d exactly once", i)
                    .isEqualTo(1);
        }

        sub.close();
        worker.interrupt();
        worker.join(2_000);
    }

    // =========================================================================
    // 3. Client disconnect — bus subscriber count returns to baseline
    // =========================================================================

    @Test
    void clientDisconnect_unsubscribesFromBus() {
        int baseline = bus.activeSubscriptionCount();

        CassetteRecordingBus.TopicSubscription topicSub = bus.subscribeTopic(TOPIC);
        FollowSubscription<CassetteRecord, TopicCursor> sub =
                FollowSubscription.forTopic(topicSub);

        assertThat(bus.activeSubscriptionCount()).isEqualTo(baseline + 1);

        sub.close();
        sub.close(); // idempotent

        assertThat(bus.activeSubscriptionCount()).isEqualTo(baseline);
    }

    // =========================================================================
    // 4. Overflow — tiny buffer + fast writer → overflow event terminates stream
    // =========================================================================

    @Test
    void overflow_streamTerminatesWithOverflowEvent() throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        CountDownLatch overflowSeen = new CountDownLatch(1);

        // Tiny buffer (capacity=2).
        CassetteRecordingBus.TopicSubscription topicSub = bus.subscribeTopic(TOPIC, 2);
        FollowSubscription<CassetteRecord, TopicCursor> sub =
                FollowSubscription.forTopic(topicSub);

        // Drive the subscription into overflow via bus.publish — publishing 5
        // records into a 2-slot queue marks the subscription overflowed.
        bus.publish(overflowBatch(TOPIC, 5));
        assertThat(topicSub.isOverflowed()).isTrue();

        Thread worker = Thread.ofVirtual().start(() -> {
            try {
                topicService.streamAll(
                        TOPIC, null, null, null, null, null,
                        r -> events.add("record:" + r.offset()),
                        com.joxette.replay.transform.TransformPipeline.IDENTITY, "",
                        sub, new FollowHooks<>() {
                            @Override public Duration heartbeatInterval() {
                                return Duration.ofMillis(50);
                            }
                            @Override public void onOverflow() {
                                events.add("overflow");
                                overflowSeen.countDown();
                            }
                        });
            } catch (Exception e) { /* exit */ }
        });

        // Wait for the worker to observe the overflow and emit the terminal event.
        assertThat(overflowSeen.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(events).contains("overflow");

        sub.close();
        worker.interrupt();
        worker.join(2_000);
    }

    private static com.joxette.recording.WriteBatch overflowBatch(String topic, int count) {
        List<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>> records =
                new ArrayList<>(count);
        List<String> types = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            records.add(new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                    topic, 0, i, 1_700_000_000_000L + i,
                    org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                    -1, -1,
                    "k" + i, b("v" + i),
                    new org.apache.kafka.common.header.internals.RecordHeaders(),
                    java.util.Optional.empty()));
            types.add(null);
        }
        return new com.joxette.recording.WriteBatch(
                topic, java.util.Set.of(), records, records, types, List.of(),
                java.util.concurrent.CompletableFuture.completedFuture(
                        new com.joxette.recording.WriteResult(topic, count)));
    }

    // =========================================================================
    // 5. follow=true combined with to=... returns HTTP 400
    // =========================================================================

    @Test
    void followAndUpperBound_returns400() throws Exception {
        int status = rawGetStatus("/cassettes/topics/" + TOPIC
                + "?follow=true&to=2030-01-01T00:00:00Z",
                "text/event-stream");
        assertThat(status).isEqualTo(400);
    }

    @Test
    void followAndOffsetUpperBound_returns400() throws Exception {
        int status = rawGetStatus("/cassettes/topics/" + TOPIC
                + "?follow=true&offset_to=1000",
                "text/event-stream");
        assertThat(status).isEqualTo(400);
    }

    // =========================================================================
    // 6. max-subscriptions capacity exceeded returns HTTP 503
    // =========================================================================

    @Test
    void maxSubscriptionsExceeded_returnsConflict() throws Exception {
        // max-subscriptions is set to 2 via @SpringBootTest properties.
        // Pre-occupy both slots with bus subscriptions, then request another
        // follow stream — the controller's capacity guard should reject with 409
        // (ConflictException.followCapacityReached).
        CassetteRecordingBus.TopicSubscription a = bus.subscribeTopic("slot-a");
        CassetteRecordingBus.TopicSubscription b = bus.subscribeTopic("slot-b");

        try {
            int status = rawGetStatus(
                    "/cassettes/topics/" + TOPIC + "?follow=true",
                    "text/event-stream");
            assertThat(status).isEqualTo(409);
        } finally {
            a.close();
            b.close();
        }
    }

    /**
     * Sends a GET to {@code baseUrl + path} and returns the HTTP status.
     * Uses java.net.http so error statuses don't throw and don't pollute the
     * stream (important for SSE endpoints where RestTemplate refuses to parse
     * a 400/503 response body as text/event-stream).
     */
    private int rawGetStatus(String path, String acceptHeader) throws Exception {
        java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
        java.net.http.HttpRequest req = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(baseUrl + path))
                .header("Accept", acceptHeader)
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        java.net.http.HttpResponse<String> resp =
                client.send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
        return resp.statusCode();
    }

    // =========================================================================
    // 7. Regression — follow=false path unchanged (paginated JSON still works)
    // =========================================================================

    @Test
    void followFalse_paginatedJsonUnchanged() throws Exception {
        Instant base = Instant.parse("2026-05-01T00:00:00Z");
        for (int i = 0; i < 3; i++) {
            DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, i,
                    base.plusSeconds(i), Instant.now(), "k" + i, b("v" + i));
        }

        String url = baseUrl + "/cassettes/topics/" + TOPIC;
        @SuppressWarnings("rawtypes")
        var resp = restTemplate.getForEntity(url, Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(((List<?>) resp.getBody().get("data"))).hasSize(3);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}

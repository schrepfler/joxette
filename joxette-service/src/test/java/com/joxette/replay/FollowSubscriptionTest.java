package com.joxette.replay;

import com.joxette.config.JoxetteProperties;
import com.joxette.recording.CassetteRecordingBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the cursor-boundary filter in
 * {@link FollowSubscription#drainBuffered} and {@link FollowSubscription#awaitNext}.
 *
 * <p>The historical drain records {@code lastEmittedCursor}; once drain
 * completes the bus-buffered records must be filtered so only those strictly
 * after the boundary are forwarded.  Records with a cursor equal to or before
 * the boundary were observed by the drain and would be duplicates if emitted.
 */
class FollowSubscriptionTest {

    private static final Instant T0 = Instant.parse("2026-01-01T10:00:00Z");

    private CassetteRecordingBus bus;

    @BeforeEach
    void setUp() {
        bus = new CassetteRecordingBus(new JoxetteProperties());
    }

    // -----------------------------------------------------------------------
    // Topic cursor — @ParameterizedTest with @MethodSource (project convention)
    // -----------------------------------------------------------------------

    static Stream<Arguments> topicBoundaryCases() {
        // lastEmittedCursor is fixed at (T0+10s, partition=1, offset=100).
        // For each case we build a single buffered record and assert whether
        // drainBuffered forwards it (true) or skips it (false).
        Instant boundaryTs = T0.plusSeconds(10);
        return Stream.of(
                Arguments.of("strictly before — timestamp earlier",
                        T0.plusSeconds(5), 1, 100L, false),
                Arguments.of("strictly before — same ts, lower partition",
                        boundaryTs, 0, 100L, false),
                Arguments.of("strictly before — same ts+partition, lower offset",
                        boundaryTs, 1, 99L, false),
                Arguments.of("equal to boundary — should be skipped (already drained)",
                        boundaryTs, 1, 100L, false),
                Arguments.of("strictly after — higher offset",
                        boundaryTs, 1, 101L, true),
                Arguments.of("strictly after — higher partition",
                        boundaryTs, 2, 0L, true),
                Arguments.of("strictly after — later timestamp",
                        T0.plusSeconds(11), 0, 0L, true)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("topicBoundaryCases")
    void topic_drainBuffered_cursorBoundaryFilter(String name,
                                                   Instant ts,
                                                   int partition,
                                                   long offset,
                                                   boolean expectEmitted) {
        CassetteRecordingBus.TopicSubscription topicSub = bus.subscribeTopic("t");
        FollowSubscription<CassetteRecord, TopicCursor> sub =
                FollowSubscription.forTopic(topicSub);

        // Simulate a historical drain that ended at (T0+10s, p=1, offset=100)
        CassetteRecord boundaryRecord = cassette(T0.plusSeconds(10), 1, 100L);
        sub.onEmitted(boundaryRecord);

        // Push one buffered record as if it arrived during the drain
        topicSub.queue().offer(cassette(ts, partition, offset));

        List<CassetteRecord> emitted = new ArrayList<>();
        sub.drainBuffered(emitted::add);

        assertThat(emitted).hasSize(expectEmitted ? 1 : 0);
    }

    // -----------------------------------------------------------------------
    // Entity cursor
    // -----------------------------------------------------------------------

    static Stream<Arguments> entityBoundaryCases() {
        // boundary = (T0+10s, T0+11s, "src", p=1, offset=100)
        Instant ts = T0.plusSeconds(10);
        Instant ra = T0.plusSeconds(11);
        return Stream.of(
                Arguments.of("strictly before — earlier ts",
                        T0.plusSeconds(5), ra, "src", 1, 100L, false),
                Arguments.of("strictly before — same ts, earlier ra",
                        ts, T0.plusSeconds(10), "src", 1, 100L, false),
                Arguments.of("strictly before — same ts+ra, earlier topic",
                        ts, ra, "aaa", 1, 100L, false),
                Arguments.of("strictly before — same ts+ra+topic, lower partition",
                        ts, ra, "src", 0, 100L, false),
                Arguments.of("strictly before — same ts+ra+topic+partition, lower offset",
                        ts, ra, "src", 1, 99L, false),
                Arguments.of("equal to boundary — skipped",
                        ts, ra, "src", 1, 100L, false),
                Arguments.of("strictly after — higher offset",
                        ts, ra, "src", 1, 101L, true),
                Arguments.of("strictly after — later topic",
                        ts, ra, "zzz", 0, 0L, true),
                Arguments.of("strictly after — later ra",
                        ts, T0.plusSeconds(12), "src", 1, 100L, true),
                Arguments.of("strictly after — later ts",
                        T0.plusSeconds(11), ra, "src", 1, 100L, true)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("entityBoundaryCases")
    void entity_drainBuffered_cursorBoundaryFilter(String name,
                                                    Instant ts, Instant ra,
                                                    String topic, int partition, long offset,
                                                    boolean expectEmitted) {
        CassetteRecordingBus.EntitySubscription entitySub =
                bus.subscribeEntity("order", "order-1");
        FollowSubscription<EntityRecord, EntityCursor> sub =
                FollowSubscription.forEntity(entitySub);

        EntityRecord boundaryRecord = entity(
                T0.plusSeconds(10), T0.plusSeconds(11), "src", 1, 100L);
        sub.onEmitted(boundaryRecord);

        entitySub.queue().offer(entity(ts, ra, topic, partition, offset));

        List<EntityRecord> emitted = new ArrayList<>();
        sub.drainBuffered(emitted::add);

        assertThat(emitted).hasSize(expectEmitted ? 1 : 0);
    }

    // -----------------------------------------------------------------------
    // No boundary set (follow was opened but no history drained)
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("drainBuffered with no prior emission forwards every buffered record")
    void drainBuffered_noBoundary_forwardsEverything() {
        CassetteRecordingBus.TopicSubscription topicSub = bus.subscribeTopic("t");
        FollowSubscription<CassetteRecord, TopicCursor> sub =
                FollowSubscription.forTopic(topicSub);

        topicSub.queue().offer(cassette(T0, 0, 0));
        topicSub.queue().offer(cassette(T0.plusSeconds(1), 0, 1));
        topicSub.queue().offer(cassette(T0.plusSeconds(2), 0, 2));

        List<CassetteRecord> emitted = new ArrayList<>();
        sub.drainBuffered(emitted::add);
        assertThat(emitted).hasSize(3);
    }

    // -----------------------------------------------------------------------
    // awaitNext suppresses duplicates silently
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("awaitNext returns null on timeout")
    void awaitNext_timeout_returnsNull() throws Exception {
        CassetteRecordingBus.TopicSubscription topicSub = bus.subscribeTopic("t");
        FollowSubscription<CassetteRecord, TopicCursor> sub =
                FollowSubscription.forTopic(topicSub);

        CassetteRecord r = sub.awaitNext(Duration.ofMillis(20));
        assertThat(r).isNull();
    }

    @Test
    @DisplayName("awaitNext returns the next live record and advances the cursor")
    void awaitNext_receivesRecord() throws Exception {
        CassetteRecordingBus.TopicSubscription topicSub = bus.subscribeTopic("t");
        FollowSubscription<CassetteRecord, TopicCursor> sub =
                FollowSubscription.forTopic(topicSub);

        CassetteRecord r = cassette(T0.plusSeconds(1), 0, 5);
        topicSub.queue().offer(r);

        CassetteRecord got = sub.awaitNext(Duration.ofSeconds(1));
        assertThat(got).isSameAs(r);
        assertThat(sub.lastEmittedCursor()).isEqualTo(
                new TopicCursor(r.timestamp(), r.partition(), r.offset()));
    }

    // -----------------------------------------------------------------------
    // close is idempotent and unsubscribes
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("close is idempotent and unregisters from the bus")
    void close_idempotent() {
        CassetteRecordingBus.TopicSubscription topicSub = bus.subscribeTopic("t");
        FollowSubscription<CassetteRecord, TopicCursor> sub =
                FollowSubscription.forTopic(topicSub);

        assertThat(bus.activeSubscriptionCount()).isEqualTo(1);
        sub.close();
        sub.close();  // must not throw
        assertThat(bus.activeSubscriptionCount()).isZero();
    }

    // -----------------------------------------------------------------------
    // Direction-aware boundary filter
    // -----------------------------------------------------------------------

    /**
     * With {@link Order#DESC}, the drain reads newest→oldest; the boundary
     * high-water mark is pinned by the first (newest) drain emission, so only
     * bus records strictly newer than that pinned cursor are forwarded.
     * The remaining older records arriving on the bus were already observed
     * by the drain and are suppressed.
     */
    @Test
    void desc_topicBoundary_emitsOnlyRecordsNewerThanDrainHighWaterMark() {
        CassetteRecordingBus.TopicSubscription topicSub = bus.subscribeTopic("t");
        FollowSubscription<CassetteRecord, TopicCursor> sub =
                FollowSubscription.forTopic(topicSub, Order.DESC);

        // DESC drain emitted newest→oldest. First (newest) emission sets the
        // pinned high-water mark at (T0+10s, 0, 100). Subsequent older
        // emissions do not move the mark.
        sub.onEmitted(cassette(T0.plusSeconds(10), 0, 100L));
        sub.onEmitted(cassette(T0.plusSeconds(5), 0, 100L));

        // Within drain window → already emitted → skip
        topicSub.queue().offer(cassette(T0.plusSeconds(7), 0, 0L));
        // Equal to high-water mark → duplicate → skip
        topicSub.queue().offer(cassette(T0.plusSeconds(10), 0, 100L));
        // Strictly newer → genuinely live → forward
        topicSub.queue().offer(cassette(T0.plusSeconds(15), 0, 50L));

        List<CassetteRecord> emitted = new ArrayList<>();
        sub.drainBuffered(emitted::add);

        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).timestamp()).isEqualTo(T0.plusSeconds(15));
    }

    @Test
    void desc_entityBoundary_emitsOnlyRecordsNewerThanDrainHighWaterMark() {
        CassetteRecordingBus.EntitySubscription entitySub =
                bus.subscribeEntity("order", "order-1");
        FollowSubscription<EntityRecord, EntityCursor> sub =
                FollowSubscription.forEntity(entitySub, Order.DESC);

        sub.onEmitted(entity(T0.plusSeconds(10), T0.plusSeconds(10), "src", 1, 100L));
        sub.onEmitted(entity(T0.plusSeconds(5), T0.plusSeconds(5), "src", 1, 100L));

        entitySub.queue().offer(entity(T0.plusSeconds(7), T0.plusSeconds(7), "src", 1, 0L));
        entitySub.queue().offer(entity(T0.plusSeconds(10), T0.plusSeconds(10), "src", 1, 100L));
        entitySub.queue().offer(entity(T0.plusSeconds(15), T0.plusSeconds(15), "src", 1, 50L));

        List<EntityRecord> emitted = new ArrayList<>();
        sub.drainBuffered(emitted::add);

        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).timestamp()).isEqualTo(T0.plusSeconds(15));
    }

    // -----------------------------------------------------------------------
    // Fixtures
    // -----------------------------------------------------------------------

    private static CassetteRecord cassette(Instant ts, int partition, long offset) {
        return new CassetteRecord(
                "t", partition, offset, ts, ts,
                null, null, List.of(), null);
    }

    private static EntityRecord entity(Instant ts, Instant recordedAt,
                                        String topic, int partition, long offset) {
        return new EntityRecord(
                "order-1", null, topic, partition, offset, ts, recordedAt,
                null, null, List.of());
    }
}

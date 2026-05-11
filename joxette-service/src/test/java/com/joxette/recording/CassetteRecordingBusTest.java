package com.joxette.recording;

import com.joxette.config.JoxetteProperties;
import com.joxette.replay.CassetteRecord;
import com.joxette.replay.EntityRecord;
import com.joxette.replay.EntityRoute;
import com.joxette.replay.KafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class CassetteRecordingBusTest {

    private static final String TOPIC_A = "topic.a";
    private static final String TOPIC_B = "topic.b";

    private JoxetteProperties props;
    private CassetteRecordingBus bus;

    @BeforeEach
    void setUp() {
        props = new JoxetteProperties();
        bus = new CassetteRecordingBus(props);
    }

    // -----------------------------------------------------------------------
    // No subscribers → no-op
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("publish with no subscribers is a no-op and does not throw")
    void publish_noSubscribers_isNoop() {
        WriteBatch batch = generalBatch(TOPIC_A, 2);
        bus.publish(batch);
        // Future should remain untouched (already completed in fixture, but no side effects here)
        assertThat(batch.generalRecords()).hasSize(2);
    }

    // -----------------------------------------------------------------------
    // Fanout isolation: topic vs entity, topic vs topic
    // -----------------------------------------------------------------------

    static Stream<Arguments> fanoutIsolationCases() {
        return Stream.of(
                Arguments.of(
                        "general publish to TOPIC_A delivers only to TOPIC_A subscribers",
                        generalBatch(TOPIC_A, 3),
                        3,   // expected delivery to TOPIC_A sub
                        0,   // expected delivery to TOPIC_B sub
                        0    // expected delivery to entity sub
                ),
                Arguments.of(
                        "general publish to TOPIC_B delivers only to TOPIC_B subscribers",
                        generalBatch(TOPIC_B, 2),
                        0,
                        2,
                        0
                ),
                Arguments.of(
                        "entity publish delivers only to matching entity subscribers",
                        entityBatch("order", "order-1", "source-topic", 4),
                        0,
                        0,
                        4
                )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fanoutIsolationCases")
    void publish_fansOutToMatchingSubscribersOnly(String name,
                                                   WriteBatch batch,
                                                   int expectedTopicA,
                                                   int expectedTopicB,
                                                   int expectedEntity) {
        CassetteRecordingBus.TopicSubscription subA = bus.subscribeTopic(TOPIC_A);
        CassetteRecordingBus.TopicSubscription subB = bus.subscribeTopic(TOPIC_B);
        CassetteRecordingBus.EntitySubscription subE =
                bus.subscribeEntity("order", "order-1");

        bus.publish(batch);

        assertThat(subA.queue()).hasSize(expectedTopicA);
        assertThat(subB.queue()).hasSize(expectedTopicB);
        assertThat(subE.queue()).hasSize(expectedEntity);
    }

    // -----------------------------------------------------------------------
    // Payload shape smoke-check — confirms the bus derives records the same
    // way the writers would.
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("published topic record carries topic, partition, offset, headers and encoded value")
    void publish_topicRecord_payloadShape() throws Exception {
        CassetteRecordingBus.TopicSubscription sub = bus.subscribeTopic(TOPIC_A);
        WriteBatch batch = generalBatch(TOPIC_A, 1);
        bus.publish(batch);

        CassetteRecord record = sub.queue().poll(1, TimeUnit.SECONDS);
        assertThat(record).isNotNull();
        assertThat(record.topic()).isEqualTo(TOPIC_A);
        assertThat(record.partition()).isZero();
        assertThat(record.offset()).isZero();
        assertThat(record.value()).isNotBlank();         // base64url-encoded
        assertThat(record.headers()).isNotEmpty();
        assertThat(record.headers().get(0).key()).isEqualTo("h0");
    }

    @Test
    @DisplayName("published entity record carries entityId, sourceTopic, and encoded value")
    void publish_entityRecord_payloadShape() throws Exception {
        CassetteRecordingBus.EntitySubscription sub =
                bus.subscribeEntity("order", "order-1");
        WriteBatch batch = entityBatch("order", "order-1", "src", 1);
        bus.publish(batch);

        EntityRecord record = sub.queue().poll(1, TimeUnit.SECONDS);
        assertThat(record).isNotNull();
        assertThat(record.entityId()).isEqualTo("order-1");
        assertThat(record.topic()).isEqualTo("src");
        assertThat(record.value()).isNotBlank();
    }

    // -----------------------------------------------------------------------
    // Overflow semantics
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("full queue marks subscription overflowed and subsequent publishes to it are dropped")
    void publish_fullQueue_marksOverflowed() {
        // capacity=2 subscriber, then publish 5 records in one batch.
        CassetteRecordingBus.TopicSubscription slow = bus.subscribeTopic(TOPIC_A, 2);
        CassetteRecordingBus.TopicSubscription fast = bus.subscribeTopic(TOPIC_A, 1024);

        bus.publish(generalBatch(TOPIC_A, 5));

        assertThat(slow.isOverflowed()).isTrue();
        assertThat(slow.queue()).hasSize(2);   // first two succeeded
        assertThat(fast.isOverflowed()).isFalse();
        assertThat(fast.queue()).hasSize(5);   // unaffected by slow subscriber

        // Subsequent publishes to the overflowed sub are dropped even if space
        // opens up again (by design: the sub is a poisoned stream).
        slow.queue().clear();
        bus.publish(generalBatch(TOPIC_A, 1));
        assertThat(slow.queue()).isEmpty();
        assertThat(fast.queue()).hasSize(6);
    }

    // -----------------------------------------------------------------------
    // Close lifecycle
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("close unregisters the subscription so further publishes do not reach it")
    void close_unregisters() {
        CassetteRecordingBus.TopicSubscription sub = bus.subscribeTopic(TOPIC_A);
        sub.close();

        bus.publish(generalBatch(TOPIC_A, 3));
        assertThat(sub.queue()).isEmpty();
    }

    @Test
    @DisplayName("close is idempotent")
    void close_idempotent() {
        CassetteRecordingBus.TopicSubscription sub = bus.subscribeTopic(TOPIC_A);
        sub.close();
        sub.close();  // must not throw
        bus.publish(generalBatch(TOPIC_A, 1));
        assertThat(sub.queue()).isEmpty();
    }

    @Test
    @DisplayName("closing one subscription leaves sibling subscriptions unaffected")
    void close_siblingSubscriptionUnaffected() {
        CassetteRecordingBus.TopicSubscription a1 = bus.subscribeTopic(TOPIC_A);
        CassetteRecordingBus.TopicSubscription a2 = bus.subscribeTopic(TOPIC_A);
        a1.close();

        bus.publish(generalBatch(TOPIC_A, 2));
        assertThat(a1.queue()).isEmpty();
        assertThat(a2.queue()).hasSize(2);
    }

    // -----------------------------------------------------------------------
    // Default capacity from configuration
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("default subscription capacity is read from JoxetteProperties.replay.follow")
    void subscribe_usesConfiguredDefaultCapacity() {
        props.getReplay().getFollow().setBufferCapacity(3);
        bus = new CassetteRecordingBus(props);

        CassetteRecordingBus.TopicSubscription sub = bus.subscribeTopic(TOPIC_A);
        bus.publish(generalBatch(TOPIC_A, 4));
        assertThat(sub.queue()).hasSize(3);
        assertThat(sub.isOverflowed()).isTrue();
    }

    // -----------------------------------------------------------------------
    // Test fixtures
    // -----------------------------------------------------------------------

    private static WriteBatch generalBatch(String topic, int count) {
        List<ConsumerRecord<String, byte[]>> records = new java.util.ArrayList<>(count);
        List<String> types = new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            RecordHeaders hdrs = new RecordHeaders();
            hdrs.add("h" + i, ("v" + i).getBytes(StandardCharsets.UTF_8));
            ConsumerRecord<String, byte[]> r = new ConsumerRecord<>(
                    topic, 0, i,
                    1_700_000_000_000L + i,
                    org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                    -1, -1,
                    "k" + i,
                    ("payload-" + i).getBytes(StandardCharsets.UTF_8),
                    hdrs,
                    java.util.Optional.empty());
            records.add(r);
            types.add(null);
        }
        return new WriteBatch(topic, records, types, List.of(),
                CompletableFuture.completedFuture(new WriteResult(topic, count)));
    }

    private static WriteBatch entityBatch(String entityType, String entityId,
                                           String sourceTopic, int routeCount) {
        List<EntityRoute> routes = new java.util.ArrayList<>(routeCount);
        for (int i = 0; i < routeCount; i++) {
            routes.add(new EntityRoute(entityType, entityId, 0, "type" + i, sourceTopic));
        }
        KafkaMessage msg = new KafkaMessage(
                sourceTopic, 0, 42L, 1_700_000_000_000L,
                "key", "value".getBytes(StandardCharsets.UTF_8),
                List.of(new KafkaMessage.Header("h", "v".getBytes(StandardCharsets.UTF_8))));
        WriteBatch.EntityWriteItem item = new WriteBatch.EntityWriteItem(routes, msg);
        return new WriteBatch(sourceTopic, List.of(), List.of(), List.of(item),
                CompletableFuture.completedFuture(new WriteResult(sourceTopic, routeCount)));
    }
}

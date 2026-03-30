package com.joxette.replay;

import com.joxette.config.JoxetteProperties;
import com.joxette.config.JoxetteProperties.Bootstrap.EntityEntry;
import com.joxette.config.JoxetteProperties.Bootstrap.EntityEntry.SourceMapping;
import com.joxette.config.JoxetteProperties.Bootstrap.EntityEntry.SourceMapping.EntityIdSpec;
import com.joxette.config.JoxetteProperties.Bootstrap.TopicEntry;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MessageRouterTest {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private JoxetteProperties propertiesFor(
            List<TopicEntry> topics, List<EntityEntry> entities) {
        JoxetteProperties props = new JoxetteProperties();
        props.getBootstrap().setTopics(topics);
        props.getBootstrap().setEntities(entities);
        return props;
    }

    private TopicEntry topicEntry(String topic, String mode) {
        TopicEntry te = new TopicEntry();
        te.setTopic(topic);
        te.setMode(mode);
        return te;
    }

    private EntityEntry entityEntry(String type, int buckets, List<SourceMapping> sources) {
        EntityEntry ee = new EntityEntry();
        ee.setType(type);
        ee.setBuckets(buckets);
        ee.setSources(sources);
        return ee;
    }

    private SourceMapping sourceMapping(String topic, String source, String expression) {
        EntityIdSpec spec = new EntityIdSpec();
        spec.setSource(source);
        spec.setExpression(expression);
        SourceMapping sm = new SourceMapping();
        sm.setTopic(topic);
        sm.setEntityId(spec);
        return sm;
    }

    private KafkaMessage jsonMessage(String topic, String jsonValue) {
        return new KafkaMessage(topic, 0, 0L, System.currentTimeMillis(), null,
                jsonValue.getBytes(StandardCharsets.UTF_8), List.of());
    }

    private KafkaMessage keyMessage(String topic, String key) {
        return new KafkaMessage(topic, 0, 0L, System.currentTimeMillis(), key, null, List.of());
    }

    // -----------------------------------------------------------------------
    // Mode = "general"
    // -----------------------------------------------------------------------

    @Test
    void general_routesToGeneralOnly() {
        JoxetteProperties props = propertiesFor(
                List.of(topicEntry("audit.log", "general")),
                List.of());
        MessageRouter router = new MessageRouter(props, new EntityIdExtractor());

        RouteDecision decision = router.route(jsonMessage("audit.log", "{}"));

        assertThat(decision.routeToGeneral()).isTrue();
        assertThat(decision.entityRoutes()).isEmpty();
    }

    @Test
    void unknownTopic_defaultsToGeneral() {
        JoxetteProperties props = propertiesFor(List.of(), List.of());
        MessageRouter router = new MessageRouter(props, new EntityIdExtractor());

        RouteDecision decision = router.route(jsonMessage("unknown.topic", "{}"));

        assertThat(decision.routeToGeneral()).isTrue();
        assertThat(decision.entityRoutes()).isEmpty();
    }

    // -----------------------------------------------------------------------
    // Mode = "entity_only"
    // -----------------------------------------------------------------------

    @Test
    void entityOnly_doesNotRouteToGeneral() {
        JoxetteProperties props = propertiesFor(
                List.of(topicEntry("payments.events", "entity_only")),
                List.of(entityEntry("order", 256,
                        List.of(sourceMapping("payments.events", "value", "$.payment.order_id")))));
        MessageRouter router = new MessageRouter(props, new EntityIdExtractor());

        RouteDecision decision = router.route(
                jsonMessage("payments.events", "{\"payment\":{\"order_id\":\"ORD-1\"}}"));

        assertThat(decision.routeToGeneral()).isFalse();
        assertThat(decision.entityRoutes()).hasSize(1);
        assertThat(decision.entityRoutes().get(0).entityId()).isEqualTo("ORD-1");
        assertThat(decision.entityRoutes().get(0).entityType()).isEqualTo("order");
    }

    // -----------------------------------------------------------------------
    // Mode = "both"
    // -----------------------------------------------------------------------

    @Test
    void both_routesToGeneralAndEntity() {
        JoxetteProperties props = propertiesFor(
                List.of(topicEntry("orders.events", "both")),
                List.of(entityEntry("order", 256,
                        List.of(sourceMapping("orders.events", "value", "$.order_id")))));
        MessageRouter router = new MessageRouter(props, new EntityIdExtractor());

        RouteDecision decision = router.route(
                jsonMessage("orders.events", "{\"order_id\":\"ORD-5\"}"));

        assertThat(decision.routeToGeneral()).isTrue();
        assertThat(decision.entityRoutes()).hasSize(1);
        assertThat(decision.entityRoutes().get(0).entityId()).isEqualTo("ORD-5");
    }

    // -----------------------------------------------------------------------
    // Entity ID extraction failures
    // -----------------------------------------------------------------------

    @Test
    void missingEntityId_skipsEntityRoute_butStillRoutesGeneral() {
        JoxetteProperties props = propertiesFor(
                List.of(topicEntry("orders.events", "both")),
                List.of(entityEntry("order", 256,
                        List.of(sourceMapping("orders.events", "value", "$.order_id")))));
        MessageRouter router = new MessageRouter(props, new EntityIdExtractor());

        // value has no order_id field
        RouteDecision decision = router.route(
                jsonMessage("orders.events", "{\"status\":\"pending\"}"));

        assertThat(decision.routeToGeneral()).isTrue();
        assertThat(decision.entityRoutes()).isEmpty();
    }

    @Test
    void entityOnly_missingId_producesNoRoutes() {
        JoxetteProperties props = propertiesFor(
                List.of(topicEntry("payments.events", "entity_only")),
                List.of(entityEntry("order", 256,
                        List.of(sourceMapping("payments.events", "value", "$.payment.order_id")))));
        MessageRouter router = new MessageRouter(props, new EntityIdExtractor());

        RouteDecision decision = router.route(jsonMessage("payments.events", "{}"));

        assertThat(decision.routeToGeneral()).isFalse();
        assertThat(decision.entityRoutes()).isEmpty();
    }

    // -----------------------------------------------------------------------
    // Bucket computation
    // -----------------------------------------------------------------------

    @Test
    void computeBucket_isAlwaysNonNegative() {
        // Verify the bucket is in [0, buckets) for a range of inputs
        int buckets = 256;
        for (String id : List.of("", "a", "abc", "0", "MIN_VALUE_test")) {
            int bucket = MessageRouter.computeBucket("order", id, buckets);
            assertThat(bucket).isBetween(0, buckets - 1);
        }
    }

    @Test
    void computeBucket_differentTypesProduceDifferentBuckets() {
        // Same entity_id, different entity_type should generally hash differently
        int b1 = MessageRouter.computeBucket("order", "ORD-1", 256);
        int b2 = MessageRouter.computeBucket("payment", "ORD-1", 256);
        // They could collide by chance, but with the seeded hash they won't for this input
        assertThat(b1).isNotEqualTo(b2);
    }

    @Test
    void computeBucket_isStable() {
        int b1 = MessageRouter.computeBucket("order", "ORD-42", 256);
        int b2 = MessageRouter.computeBucket("order", "ORD-42", 256);
        assertThat(b1).isEqualTo(b2);
    }

    // -----------------------------------------------------------------------
    // Multiple entity sources on the same topic
    // -----------------------------------------------------------------------

    @Test
    void multipleEntityMappings_allResolvedRoutes_areReturned() {
        SourceMapping sm1 = sourceMapping("orders.events", "value", "$.order_id");
        SourceMapping sm2 = sourceMapping("orders.events", "key", null);

        JoxetteProperties props = propertiesFor(
                List.of(topicEntry("orders.events", "both")),
                List.of(
                        entityEntry("order", 256, List.of(sm1)),
                        entityEntry("session", 64, List.of(sm2))));
        MessageRouter router = new MessageRouter(props, new EntityIdExtractor());

        KafkaMessage msg = new KafkaMessage("orders.events", 0, 0L,
                System.currentTimeMillis(), "session-key",
                "{\"order_id\":\"ORD-9\"}".getBytes(StandardCharsets.UTF_8), List.of());

        RouteDecision decision = router.route(msg);

        assertThat(decision.routeToGeneral()).isTrue();
        assertThat(decision.entityRoutes()).hasSize(2);
        assertThat(decision.entityRoutes())
                .extracting(EntityRoute::entityType)
                .containsExactlyInAnyOrder("order", "session");
    }
}

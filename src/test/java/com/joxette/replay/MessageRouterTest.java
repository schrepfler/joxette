package com.joxette.replay;

import com.joxette.management.ConfigRepository;
import com.joxette.management.EntitySourceConfig;
import com.joxette.management.EntityTypeConfig;
import com.joxette.management.TopicConfig;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MessageRouter}.
 *
 * <p>Uses a {@link StubConfigRepository} that is loaded with in-memory data
 * instead of a real DuckDB connection, so no Spring context or container is
 * needed.
 */
class MessageRouterTest {

    // =========================================================================
    // Stub ConfigRepository
    // =========================================================================

    /**
     * Minimal stub that returns pre-loaded topic modes, entity types, and
     * source configs.  Only the methods called by {@link MessageRouter#reload()}
     * are implemented; all others throw {@link UnsupportedOperationException}.
     */
    static class StubConfigRepository extends ConfigRepository {

        private final List<TopicConfig> topics = new ArrayList<>();
        private final List<EntityTypeConfig> entityTypes = new ArrayList<>();
        /** entity_type → sources */
        private final Map<String, List<EntitySourceConfig>> sources = new HashMap<>();

        /** Build with no real DuckDB — we never call the super constructor body. */
        StubConfigRepository() {
            super(null, null);
        }

        void addTopic(String topic, String mode) {
            topics.add(new TopicConfig(topic, mode, false, false, null, "latest"));
        }

        void addEntityType(String type, int buckets) {
            entityTypes.add(new EntityTypeConfig(type, buckets));
        }

        void addSource(String entityType, String topic, String mode,
                       List<EntitySourceConfig.MatcherConfig> matchers) {
            sources.computeIfAbsent(entityType, k -> new ArrayList<>())
                   .add(new EntitySourceConfig(entityType, topic, mode, matchers));
        }

        @Override public List<TopicConfig>      listTopics()               { return List.copyOf(topics); }
        @Override public List<EntityTypeConfig> listEntityTypes()           { return List.copyOf(entityTypes); }
        @Override public List<EntitySourceConfig> listSources(String type)  { return sources.getOrDefault(type, List.of()); }

        // ---- unused methods ----
        @Override public Optional<TopicConfig>      findTopic(String t)       { throw new UnsupportedOperationException(); }
        @Override public TopicConfig                upsertTopic(String t, String m, boolean p) { throw new UnsupportedOperationException(); }
        @Override public boolean                    deleteTopic(String t)      { throw new UnsupportedOperationException(); }
        @Override public boolean                    setPaused(String t, boolean p) { throw new UnsupportedOperationException(); }
        @Override public Optional<EntityTypeConfig> findEntityType(String t)   { throw new UnsupportedOperationException(); }
        @Override public EntityTypeConfig           upsertEntityType(String t, int b) { throw new UnsupportedOperationException(); }
        @Override public boolean                    deleteEntityType(String t) { throw new UnsupportedOperationException(); }
        @Override public EntitySourceConfig         upsertSource(String et, String tp, String m,
                                                                   List<EntitySourceConfig.MatcherConfig> mc) { throw new UnsupportedOperationException(); }
        @Override public boolean                    deleteSource(String et, String tp) { throw new UnsupportedOperationException(); }
    }

    // =========================================================================
    // Builder helpers
    // =========================================================================

    private static MessageRouter routerFor(StubConfigRepository repo) {
        return new MessageRouter(repo, new EntityIdExtractor());
    }

    private static EntitySourceConfig.MatcherConfig matcher(
            String messageType, String source, String expression) {
        return new EntitySourceConfig.MatcherConfig(messageType, source, expression);
    }

    private static KafkaMessage jsonMessage(String topic, String jsonValue) {
        return new KafkaMessage(topic, 0, 0L, System.currentTimeMillis(), null,
                jsonValue.getBytes(StandardCharsets.UTF_8), List.of());
    }

    private static KafkaMessage keyMessage(String topic, String key) {
        return new KafkaMessage(topic, 0, 0L, System.currentTimeMillis(), key, null, List.of());
    }

    // =========================================================================
    // Mode = "general"
    // =========================================================================

    @Test
    void general_routesToGeneralOnly() {
        StubConfigRepository repo = new StubConfigRepository();
        repo.addTopic("audit.log", "general");
        MessageRouter router = routerFor(repo);

        RouteDecision decision = router.route(jsonMessage("audit.log", "{}"));

        assertThat(decision.routeToGeneral()).isTrue();
        assertThat(decision.entityRoutes()).isEmpty();
    }

    @Test
    void unknownTopic_defaultsToGeneral() {
        MessageRouter router = routerFor(new StubConfigRepository());

        RouteDecision decision = router.route(jsonMessage("unknown.topic", "{}"));

        assertThat(decision.routeToGeneral()).isTrue();
        assertThat(decision.entityRoutes()).isEmpty();
    }

    // =========================================================================
    // Mode = "entity_only" with single matcher
    // =========================================================================

    @Test
    void entityOnly_doesNotRouteToGeneral() {
        StubConfigRepository repo = new StubConfigRepository();
        repo.addTopic("payments.events", "entity_only");
        repo.addEntityType("order", 256);
        repo.addSource("order", "payments.events", "entity_only",
                List.of(matcher("payment", "value", "$.payment.order_id")));
        MessageRouter router = routerFor(repo);

        RouteDecision decision = router.route(
                jsonMessage("payments.events", "{\"payment\":{\"order_id\":\"ORD-1\"}}"));

        assertThat(decision.routeToGeneral()).isFalse();
        assertThat(decision.entityRoutes()).hasSize(1);
        assertThat(decision.entityRoutes().get(0).entityId()).isEqualTo("ORD-1");
        assertThat(decision.entityRoutes().get(0).entityType()).isEqualTo("order");
        assertThat(decision.entityRoutes().get(0).messageType()).isEqualTo("payment");
    }

    // =========================================================================
    // Mode = "both"
    // =========================================================================

    @Test
    void both_routesToGeneralAndEntity() {
        StubConfigRepository repo = new StubConfigRepository();
        repo.addTopic("orders.events", "both");
        repo.addEntityType("order", 256);
        repo.addSource("order", "orders.events", "entity_only",
                List.of(matcher("order", "value", "$.order_id")));
        MessageRouter router = routerFor(repo);

        RouteDecision decision = router.route(
                jsonMessage("orders.events", "{\"order_id\":\"ORD-5\"}"));

        assertThat(decision.routeToGeneral()).isTrue();
        assertThat(decision.entityRoutes()).hasSize(1);
        assertThat(decision.entityRoutes().get(0).entityId()).isEqualTo("ORD-5");
        assertThat(decision.entityRoutes().get(0).messageType()).isEqualTo("order");
    }

    // =========================================================================
    // Entity ID extraction failures
    // =========================================================================

    @Test
    void missingEntityId_skipsEntityRoute_butStillRoutesGeneral() {
        StubConfigRepository repo = new StubConfigRepository();
        repo.addTopic("orders.events", "both");
        repo.addEntityType("order", 256);
        repo.addSource("order", "orders.events", "entity_only",
                List.of(matcher("order", "value", "$.order_id")));
        MessageRouter router = routerFor(repo);

        RouteDecision decision = router.route(
                jsonMessage("orders.events", "{\"status\":\"pending\"}"));

        assertThat(decision.routeToGeneral()).isTrue();
        assertThat(decision.entityRoutes()).isEmpty();
    }

    @Test
    void entityOnly_missingId_producesNoRoutes() {
        StubConfigRepository repo = new StubConfigRepository();
        repo.addTopic("payments.events", "entity_only");
        repo.addEntityType("order", 256);
        repo.addSource("order", "payments.events", "entity_only",
                List.of(matcher("payment", "value", "$.payment.order_id")));
        MessageRouter router = routerFor(repo);

        RouteDecision decision = router.route(jsonMessage("payments.events", "{}"));

        assertThat(decision.routeToGeneral()).isFalse();
        assertThat(decision.entityRoutes()).isEmpty();
    }

    // =========================================================================
    // Multiple matchers — first match wins
    // =========================================================================

    @Test
    void multipleMatchers_firstMatchWins() {
        StubConfigRepository repo = new StubConfigRepository();
        repo.addTopic("events", "both");
        repo.addEntityType("fixture", 256);
        repo.addSource("fixture", "events", "entity_only", List.of(
                matcher("fixture",   "value", "$.fixture.id"),
                matcher("marketSet", "value", "$.marketSet.fixtureId")));
        MessageRouter router = routerFor(repo);

        // Message is a marketSet — no $.fixture.id present
        RouteDecision decision = router.route(
                jsonMessage("events", "{\"marketSet\":{\"fixtureId\":\"FIX-99\"}}"));

        assertThat(decision.entityRoutes()).hasSize(1);
        assertThat(decision.entityRoutes().get(0).entityId()).isEqualTo("FIX-99");
        assertThat(decision.entityRoutes().get(0).entityType()).isEqualTo("fixture");
        assertThat(decision.entityRoutes().get(0).messageType()).isEqualTo("marketSet");
    }

    @Test
    void multipleMatchers_firstMatchWins_fixtureMessage() {
        StubConfigRepository repo = new StubConfigRepository();
        repo.addTopic("events", "both");
        repo.addEntityType("fixture", 256);
        repo.addSource("fixture", "events", "entity_only", List.of(
                matcher("fixture",   "value", "$.fixture.id"),
                matcher("marketSet", "value", "$.marketSet.fixtureId")));
        MessageRouter router = routerFor(repo);

        // Message is a fixture — first matcher matches
        RouteDecision decision = router.route(
                jsonMessage("events", "{\"fixture\":{\"id\":\"FIX-7\"}}"));

        assertThat(decision.entityRoutes()).hasSize(1);
        assertThat(decision.entityRoutes().get(0).entityId()).isEqualTo("FIX-7");
        assertThat(decision.entityRoutes().get(0).messageType()).isEqualTo("fixture");
    }

    // =========================================================================
    // Per-mapping mode promotion
    // =========================================================================

    @Test
    void mappingMode_both_promotesGeneralEvenIfTopicIsEntityOnly() {
        StubConfigRepository repo = new StubConfigRepository();
        repo.addTopic("events", "entity_only");
        repo.addEntityType("fixture", 256);
        repo.addSource("fixture", "events", "both",   // mapping-level both
                List.of(matcher("fixture", "value", "$.fixture.id")));
        MessageRouter router = routerFor(repo);

        RouteDecision decision = router.route(
                jsonMessage("events", "{\"fixture\":{\"id\":\"FIX-1\"}}"));

        assertThat(decision.routeToGeneral()).isTrue();
        assertThat(decision.entityRoutes()).hasSize(1);
    }

    // =========================================================================
    // Multiple entity types from the same topic
    // =========================================================================

    @Test
    void multipleEntityTypes_allResolvedRoutes_areReturned() {
        StubConfigRepository repo = new StubConfigRepository();
        repo.addTopic("events", "both");
        repo.addEntityType("fixture", 256);
        repo.addEntityType("session", 64);
        repo.addSource("fixture", "events", "entity_only",
                List.of(matcher("fixture", "value", "$.fixture.id")));
        repo.addSource("session", "events", "entity_only",
                List.of(matcher("session", "key", null)));
        MessageRouter router = routerFor(repo);

        KafkaMessage msg = new KafkaMessage("events", 0, 0L,
                System.currentTimeMillis(), "session-key",
                "{\"fixture\":{\"id\":\"FIX-9\"}}".getBytes(StandardCharsets.UTF_8), List.of());

        RouteDecision decision = router.route(msg);

        assertThat(decision.routeToGeneral()).isTrue();
        assertThat(decision.entityRoutes()).hasSize(2);
        assertThat(decision.entityRoutes())
                .extracting(EntityRoute::entityType)
                .containsExactlyInAnyOrder("fixture", "session");
    }

    // =========================================================================
    // Bucket computation
    // =========================================================================

    @Test
    void computeBucket_isAlwaysNonNegative() {
        int buckets = 256;
        for (String id : List.of("", "a", "abc", "0", "MIN_VALUE_test")) {
            int bucket = MessageRouter.computeBucket("order", id, buckets);
            assertThat(bucket).isBetween(0, buckets - 1);
        }
    }

    @Test
    void computeBucket_differentTypesProduceDifferentBuckets() {
        int b1 = MessageRouter.computeBucket("order",   "ORD-1", 256);
        int b2 = MessageRouter.computeBucket("payment", "ORD-1", 256);
        assertThat(b1).isNotEqualTo(b2);
    }

    @Test
    void computeBucket_isStable() {
        int b1 = MessageRouter.computeBucket("order", "ORD-42", 256);
        int b2 = MessageRouter.computeBucket("order", "ORD-42", 256);
        assertThat(b1).isEqualTo(b2);
    }

    // =========================================================================
    // reload() reflects config changes at runtime
    // =========================================================================

    @Test
    void reload_picksUpNewEntitySource() throws SQLException {
        StubConfigRepository repo = new StubConfigRepository();
        repo.addTopic("orders.events", "both");
        // No entity type yet — should produce no entity routes
        MessageRouter router = routerFor(repo);

        RouteDecision before = router.route(
                jsonMessage("orders.events", "{\"order_id\":\"ORD-1\"}"));
        assertThat(before.entityRoutes()).isEmpty();

        // Now register the entity type + source and reload
        repo.addEntityType("order", 256);
        repo.addSource("order", "orders.events", "entity_only",
                List.of(matcher("order", "value", "$.order_id")));
        router.reload();

        RouteDecision after = router.route(
                jsonMessage("orders.events", "{\"order_id\":\"ORD-1\"}"));
        assertThat(after.entityRoutes()).hasSize(1);
        assertThat(after.entityRoutes().get(0).entityId()).isEqualTo("ORD-1");
    }
}

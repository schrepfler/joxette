package com.joxette.replay;

import com.joxette.management.ConfigRepository;
import com.joxette.management.EntitySourceConfig;
import com.joxette.management.EntityTypeConfig;
import com.joxette.management.IdSource;
import com.joxette.management.TopicMatcherConfig;
import com.joxette.management.TopicMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Routes each {@link KafkaMessage} to its target cassette destinations based
 * on configuration loaded from the {@link ConfigRepository} (DuckDB).
 *
 * <h2>Routing model</h2>
 * <p>Each entity type has one or more source-topic mappings.  Each source
 * mapping has a list of {@link EntitySourceConfig.MatcherConfig}s — one per
 * message variant that can carry the entity's ID (e.g. a {@code fixture}
 * entity may be referenced by {@code marketSet}, {@code resultSet},
 * {@code coverage}, etc. messages, all on the same topic).
 *
 * <p>For each incoming message the router tries every matcher for every entity
 * mapping whose topic matches.  The first matcher that successfully extracts an
 * ID produces an {@link EntityRoute} carrying the matched {@code messageType}.
 * Multiple entity types can independently match the same message.
 *
 * <h2>General-cassette routing</h2>
 * <p>Enabled when:
 * <ul>
 *   <li>the topic-level mode is {@code "general"} or {@code "both"}, OR</li>
 *   <li>any matching source mapping has mode {@code "both"}.</li>
 * </ul>
 *
 * <h2>Bucket assignment</h2>
 * <p>{@code bucket = Math.floorMod(hash(entityType, entityId), buckets)}.
 *
 * <h2>Config lifecycle</h2>
 * <p>Routing tables are loaded from the DB once at startup via
 * {@link #reload()}.  Call {@link #reload()} again after any REST API change
 * to entity types or source mappings (e.g. from {@code EntityController}).
 */
@Component
@DependsOn("managementConfigRepository")
public class MessageRouter {

    private static final Logger log = LoggerFactory.getLogger(MessageRouter.class);

    private final ConfigRepository configRepo;
    private final EntityIdExtractor extractor;

    /** Snapshot of routing state, replaced atomically on reload(). */
    private volatile RoutingTables tables;

    public MessageRouter(ConfigRepository configRepo, EntityIdExtractor extractor) {
        this.configRepo = configRepo;
        this.extractor  = extractor;
        try {
            reload();
        } catch (SQLException e) {
            log.warn("MessageRouter: initial config load from DB failed ({}); starting with empty routing tables", e.getMessage());
            this.tables = new RoutingTables(Map.of(), Map.of(), Map.of(), Map.of());
        }
    }

    /**
     * Reloads all routing configuration from the database.
     *
     * <p>The swap is atomic: a new {@link RoutingTables} object is built fully
     * before replacing the field, so in-flight {@link #route} calls always see
     * a consistent snapshot.
     *
     * @throws SQLException if the DB query fails
     */
    public void reload() throws SQLException {
        // 1. Topic modes
        Map<String, TopicMode> topicModes = new HashMap<>();
        for (var tc : configRepo.listTopics()) {
            topicModes.put(tc.topic(), tc.mode());
        }

        // 2. Entity type → bucket count
        Map<String, Integer> entityBuckets = new HashMap<>();
        for (EntityTypeConfig etc : configRepo.listEntityTypes()) {
            entityBuckets.put(etc.entityType(), etc.buckets());
        }

        // 3. topic → list of EntitySourceEntry (all entity types)
        Map<String, List<EntitySourceEntry>> topicSourceEntries = new HashMap<>();
        for (EntityTypeConfig etc : configRepo.listEntityTypes()) {
            for (EntitySourceConfig src : configRepo.listSources(etc.entityType())) {
                topicSourceEntries
                        .computeIfAbsent(src.topic(), k -> new ArrayList<>())
                        .add(new EntitySourceEntry(etc.entityType(), src.mode(), src.matchers()));
            }
        }

        // 4. topic → ordered list of message-type matchers for general cassette tagging
        Map<String, List<TopicMatcherEntry>> topicMatchers = new HashMap<>();
        for (TopicMatcherConfig m : configRepo.listAllTopicMatchers()) {
            topicMatchers
                    .computeIfAbsent(m.topic(), k -> new ArrayList<>())
                    .add(new TopicMatcherEntry(m.messageType(), m.idSource(), m.idExpression()));
        }

        this.tables = new RoutingTables(
                Map.copyOf(topicModes),
                Map.copyOf(entityBuckets),
                Map.copyOf(topicSourceEntries),
                Map.copyOf(topicMatchers));

        log.info("MessageRouter: loaded {} topic mode(s), {} entity source mapping(s), {} topic matcher(s)",
                topicModes.size(),
                topicSourceEntries.values().stream().mapToInt(List::size).sum(),
                topicMatchers.values().stream().mapToInt(List::size).sum());
    }

    /**
     * Computes a {@link RouteDecision} for {@code message}.
     *
     * <p>For each source-entry whose topic matches, every matcher is tried in
     * declaration order.  The first matcher that yields an entity ID produces
     * one {@link EntityRoute} (with the matched {@code messageType}).  If no
     * matcher matches, the source-entry produces no route for this message.
     *
     * <p>When the {@code entity-router} role is not active on this instance, entity
     * extraction is skipped entirely — only the general-cassette routing decision is
     * computed.  Messages on {@code entity_only} topics are silently dropped on
     * such nodes (no general route, no entity route).
     */
    public RouteDecision route(KafkaMessage message) {
        RoutingTables t = this.tables;

        TopicMode topicMode = t.topicModes().getOrDefault(message.topic(), TopicMode.GENERAL);
        boolean routeToGeneral = topicMode.writesGeneral();

        List<EntityRoute> entityRoutes = new ArrayList<>();
        if (topicMode.writesEntities()) {
            List<EntitySourceEntry> entries =
                    t.topicSourceEntries().getOrDefault(message.topic(), List.of());

            for (EntitySourceEntry entry : entries) {
                // Try each matcher in declaration order; stop at first match
                for (EntitySourceConfig.MatcherConfig matcher : entry.matchers()) {
                    Optional<String> entityId =
                            extractor.extract(message, matcher.idSource(), matcher.idExpression());
                    if (entityId.isPresent()) {
                        int bucketCount = t.entityBuckets().getOrDefault(entry.entityType(), 256);
                        int bucket = computeBucket(entry.entityType(), entityId.get(), bucketCount);
                        entityRoutes.add(new EntityRoute(
                                entry.entityType(),
                                entityId.get(),
                                bucket,
                                matcher.messageType(),
                                message.topic()));
                        break; // one route per entity-source entry per message
                    }
                }
                // Per-mapping mode promotion
                if (entry.mappingMode() == TopicMode.BOTH) {
                    routeToGeneral = true;
                }
            }
        }

        // Determine message_type for general cassette via topic_message_type_matchers.
        // Matchers are tried in insertion order; first match wins.
        GeneralRoute generalRoute = null;
        if (routeToGeneral) {
            String messageType = null;
            for (TopicMatcherEntry m : t.topicMatchers().getOrDefault(message.topic(), List.of())) {
                if (extractor.extract(message, m.idSource(), m.idExpression()).isPresent()) {
                    messageType = m.messageType();
                    break;
                }
            }
            generalRoute = new GeneralRoute(messageType);
        }

        if (generalRoute == null && entityRoutes.isEmpty()) {
            log.trace("No route for message topic='{}' partition={} offset={}",
                    message.topic(), message.partition(), message.offset());
        }
        return new RouteDecision(message, generalRoute, List.copyOf(entityRoutes));
    }

    /**
     * Stable bucket assignment: {@code floorMod(hash(entityType, entityId), buckets)}.
     */
    static int computeBucket(String entityType, String entityId, int buckets) {
        int hash = 31 * entityType.hashCode() + entityId.hashCode();
        return Math.floorMod(hash, buckets);
    }

    /** Internal transfer object grouping matchers for one (entity_type, topic) pair. */
    private record EntitySourceEntry(
            String entityType,
            TopicMode mappingMode,
            List<EntitySourceConfig.MatcherConfig> matchers) {}

    /** Internal transfer object for one topic_message_type_matchers row. */
    private record TopicMatcherEntry(String messageType, IdSource idSource, String idExpression) {}

    /** Immutable snapshot of all routing tables, swapped atomically on reload. */
    private record RoutingTables(
            Map<String, TopicMode> topicModes,
            Map<String, Integer> entityBuckets,
            Map<String, List<EntitySourceEntry>> topicSourceEntries,
            Map<String, List<TopicMatcherEntry>> topicMatchers) {}
}

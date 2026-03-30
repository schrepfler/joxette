package com.joxette.replay;

import com.joxette.config.JoxetteProperties;
import com.joxette.config.JoxetteProperties.Bootstrap.EntityEntry.SourceMapping;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Routes each {@link KafkaMessage} to its target cassette destinations based
 * on the bootstrap configuration.
 *
 * <h2>Routing modes</h2>
 * <dl>
 *   <dt>{@code "general"}</dt>
 *   <dd>Message is written only to {@code lake.cassette}.</dd>
 *   <dt>{@code "entity_only"}</dt>
 *   <dd>Message is written only to the matching entity cassette tables
 *       ({@code lake.entity_{type}}). It is <em>not</em> written to the general
 *       cassette.</dd>
 *   <dt>{@code "both"}</dt>
 *   <dd>Message is written to both the general cassette and every matching
 *       entity cassette.</dd>
 * </dl>
 *
 * <p>Topics not present in the bootstrap configuration default to
 * {@code "general"} mode.
 *
 * <h2>Entity bucket</h2>
 * <p>{@code entity_bucket = Math.floorMod(Objects.hash(entityType, entityId), buckets)}.
 * {@link Math#floorMod} is used so the result is always in {@code [0, buckets)}.
 */
@Component
public class MessageRouter {

    /** topic → mode ("general" | "entity_only" | "both") */
    private final Map<String, String> topicModes;

    /** topic → ordered list of entity mappings that draw IDs from it */
    private final Map<String, List<EntityMapping>> topicEntityMappings;

    /** entity type → bucket count */
    private final Map<String, Integer> entityBuckets;

    private final EntityIdExtractor extractor;

    public MessageRouter(JoxetteProperties properties, EntityIdExtractor extractor) {
        this.extractor = extractor;

        Map<String, String> modes = new HashMap<>();
        for (JoxetteProperties.Bootstrap.TopicEntry te : properties.getBootstrap().getTopics()) {
            modes.put(te.getTopic(), te.getMode());
        }
        this.topicModes = Map.copyOf(modes);

        Map<String, List<EntityMapping>> mappings = new HashMap<>();
        Map<String, Integer> buckets = new HashMap<>();

        for (JoxetteProperties.Bootstrap.EntityEntry ee : properties.getBootstrap().getEntities()) {
            buckets.put(ee.getType(), ee.getBuckets());
            for (SourceMapping sm : ee.getSources()) {
                mappings.computeIfAbsent(sm.getTopic(), k -> new ArrayList<>())
                        .add(new EntityMapping(
                                ee.getType(),
                                sm.getEntityId().getSource(),
                                sm.getEntityId().getExpression()));
            }
        }

        this.topicEntityMappings = Map.copyOf(mappings);
        this.entityBuckets = Map.copyOf(buckets);
    }

    /**
     * Computes a {@link RouteDecision} for {@code message}.
     *
     * <p>Entity ID extraction failures are silently skipped; the returned
     * {@code entityRoutes} list contains only the successfully resolved routes.
     */
    public RouteDecision route(KafkaMessage message) {
        String mode = topicModes.getOrDefault(message.topic(), "general");
        boolean routeToGeneral = !"entity_only".equals(mode);

        List<EntityRoute> entityRoutes = new ArrayList<>();
        if (!"general".equals(mode)) {
            List<EntityMapping> mappings =
                    topicEntityMappings.getOrDefault(message.topic(), List.of());
            for (EntityMapping mapping : mappings) {
                Optional<String> entityId =
                        extractor.extract(message, mapping.source(), mapping.expression());
                entityId.ifPresent(id -> {
                    int bucketCount = entityBuckets.getOrDefault(mapping.entityType(), 256);
                    int bucket = computeBucket(mapping.entityType(), id, bucketCount);
                    entityRoutes.add(new EntityRoute(mapping.entityType(), id, bucket));
                });
            }
        }

        return new RouteDecision(message, routeToGeneral, List.copyOf(entityRoutes));
    }

    /**
     * Stable bucket assignment: {@code floorMod(hash(entityType, entityId), buckets)}.
     *
     * <p>{@link Math#floorMod} guarantees a non-negative result for any
     * hash value, including {@link Integer#MIN_VALUE}.
     */
    static int computeBucket(String entityType, String entityId, int buckets) {
        int hash = 31 * entityType.hashCode() + entityId.hashCode();
        return Math.floorMod(hash, buckets);
    }

    /** Internal transfer object, not part of the public API. */
    private record EntityMapping(String entityType, String source, String expression) {}
}

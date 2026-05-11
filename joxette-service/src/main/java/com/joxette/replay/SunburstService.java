package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Builds a prefix-tree (sunburst) hierarchy from all entity sequences of a
 * given type.
 *
 * <p>Each node in the tree represents one event type at one step in the
 * sequence. The {@link SunburstNode#nodeCount()} is the number of sequences
 * that passed through that node. Arc angle is proportional to nodeCount.
 *
 * <p>Up to {@code maxEntities} entity IDs are included. For each entity the
 * first {@code maxSteps} events are considered.
 */
@Service
public class SunburstService {

    private static final int DEFAULT_MAX_ENTITIES = 500;
    private static final int DEFAULT_MAX_STEPS    = 8;

    private final EntityReplayService entityReplayService;

    public SunburstService(EntityReplayService entityReplayService) {
        this.entityReplayService = entityReplayService;
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    public record SunburstRequest(
            String from,
            String to,
            Integer maxSteps,
            Double minAngleDeg,
            Integer maxEntities,
            String solQuery
    ) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SunburstNode(
            String name,
            int nodeId,
            int nodeCount,
            List<String> seqIds,
            List<SunburstNode> children
    ) {}

    public record SunburstResponse(
            SunburstNode root,
            List<String> eventNames,
            int totalSequences
    ) {}

    public SunburstResponse build(String entityType, SunburstRequest req) throws SQLException {
        int maxSteps    = req.maxSteps()    != null ? req.maxSteps()    : DEFAULT_MAX_STEPS;
        int maxEntities = req.maxEntities() != null ? req.maxEntities() : DEFAULT_MAX_ENTITIES;

        Instant from = req.from() != null ? Instant.parse(req.from()) : null;
        Instant to   = req.to()   != null ? Instant.parse(req.to())   : null;

        // Page through known entities, collecting sequences
        List<Sequence> sequences = new ArrayList<>();
        String cursor = null;
        outer:
        do {
            var page = entityReplayService.listEntities(entityType, 100, cursor, EntityReplayService.EntitySortBy.id);
            for (EntityInfo info : page.data()) {
                if (sequences.size() >= maxEntities) break outer;
                List<EntityRecord> records = new ArrayList<>();
                entityReplayService.streamEntityEvents(entityType, info.entityId(), from, to, records::add);
                if (!records.isEmpty()) {
                    sequences.add(toSequence(info.entityId(), records, maxSteps));
                }
            }
            cursor = page.hasMore() ? page.nextCursor() : null;
        } while (cursor != null && sequences.size() < maxEntities);

        // Build hierarchy
        AtomicInteger nextId = new AtomicInteger(2);
        SunburstNode root = buildHierarchy(sequences, nextId);

        // Collect all distinct event names for the legend
        List<String> eventNames = collectEventNames(root);

        return new SunburstResponse(root, eventNames, sequences.size());
    }

    // -----------------------------------------------------------------------
    // Hierarchy builder
    // -----------------------------------------------------------------------

    private SunburstNode buildHierarchy(List<Sequence> sequences, AtomicInteger nextId) {
        // Mutable build node — converted to records at the end
        BuildNode root = new BuildNode("start", 1);
        for (Sequence seq : sequences) {
            root.nodeCount++;
            BuildNode node = root;
            for (String eventName : seq.events()) {
                node = node.children.computeIfAbsent(eventName,
                        name -> new BuildNode(name, nextId.getAndIncrement()));
                node.nodeCount++;
            }
            node.seqIds.add(seq.entityId());
        }
        return toRecord(root);
    }

    private SunburstNode toRecord(BuildNode n) {
        List<SunburstNode> children = n.children.values().stream()
                .sorted((a, b) -> Integer.compare(b.nodeCount, a.nodeCount)) // most common first
                .map(this::toRecord)
                .toList();
        return new SunburstNode(
                n.name, n.nodeId, n.nodeCount,
                n.seqIds.isEmpty() ? null : List.copyOf(n.seqIds),
                children.isEmpty() ? null : children);
    }

    private static final class BuildNode {
        final String name;
        final int nodeId;
        int nodeCount;
        final List<String> seqIds = new ArrayList<>();
        final Map<String, BuildNode> children = new LinkedHashMap<>();

        BuildNode(String name, int nodeId) {
            this.name   = name;
            this.nodeId = nodeId;
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private record Sequence(String entityId, List<String> events) {}

    private static Sequence toSequence(String entityId, List<EntityRecord> records, int maxSteps) {
        List<String> events = records.stream()
                .limit(maxSteps)
                .map(r -> r.messageType() != null ? r.messageType() : r.topic())
                .toList();
        return new Sequence(entityId, events);
    }

    private List<String> collectEventNames(SunburstNode root) {
        List<String> names = new ArrayList<>();
        collectEventNamesRecursive(root, names);
        return names.stream().distinct().sorted().toList();
    }

    private void collectEventNamesRecursive(SunburstNode node, List<String> acc) {
        if (!node.name().equals("start")) acc.add(node.name());
        if (node.children() != null) {
            for (SunburstNode child : node.children()) {
                collectEventNamesRecursive(child, acc);
            }
        }
    }
}

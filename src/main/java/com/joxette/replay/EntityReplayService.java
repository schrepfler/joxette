package com.joxette.replay;

import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Queries entity cassettes ({@code lake.entity_{type}}) and the
 * {@code lake.known_entities} registry.
 *
 * <h2>Deduplication</h2>
 * <p>The entity cassette PK is {@code (entity_id, timestamp, recorded_at)},
 * so the same {@code (topic, partition, offset)} triple can appear multiple
 * times. A {@code QUALIFY ROW_NUMBER() OVER (PARTITION BY topic, partition,
 * "offset" ORDER BY recorded_at DESC) = 1} clause keeps only the
 * most-recently recorded copy per source Kafka message.
 *
 * <h2>Cursor encoding</h2>
 * <p>The entity replay cursor encodes
 * {@code (timestamp, recorded_at, source_topic, source_partition,
 * source_offset)}, matching the five-column {@code ORDER BY} used after
 * deduplication.
 *
 * <h2>Known-entity pagination</h2>
 * <p>List and search cursors encode only {@code entity_id} (ordering column)
 * as URL-safe base64.
 */
@Service
public class EntityReplayService {

    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[a-z][a-z0-9_]*");
    private static final int STREAM_PAGE_SIZE = 500;

    private final Connection duckDB;

    public EntityReplayService(Connection duckDB) {
        this.duckDB = duckDB;
    }

    // -------------------------------------------------------------------------
    // Entity event replay
    // -------------------------------------------------------------------------

    /**
     * Returns one page of deduplicated events for {@code entityId} from
     * {@code lake.entity_{entityType}}.
     */
    public PagedResponse<EntityRecord> queryEntityEvents(
            String entityType,
            String entityId,
            Instant from, Instant to,
            int limit,
            String cursor
    ) throws SQLException {
        validateEntityType(entityType);
        EntityCursor decoded = cursor != null ? EntityCursor.decode(cursor) : null;

        String table = "lake.entity_" + entityType;
        var sql = new StringBuilder("SELECT entity_id, entity_bucket, topic, partition, \"offset\","
                + " timestamp, recorded_at, key, value, headers\nFROM (\n"
                + "    SELECT entity_id, entity_bucket, topic, partition, \"offset\","
                + " timestamp, recorded_at, key, value, headers\n"
                + "    FROM " + table + "\n"
                + "    WHERE entity_id = ?\n");
        List<Object> params = new ArrayList<>();
        params.add(entityId);

        if (from != null) {
            sql.append("      AND timestamp >= ?\n");
            params.add(Timestamp.from(from));
        }
        if (to != null) {
            sql.append("      AND timestamp <= ?\n");
            params.add(Timestamp.from(to));
        }

        // Cast partition to BIGINT in the PARTITION BY to avoid a DuckDB 1.5 type-binding
        // bug that surfaces when BIGINT parameters are combined with INTEGER columns in
        // QUALIFY window functions.
        sql.append("""
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY topic, CAST(partition AS BIGINT), "offset" ORDER BY recorded_at DESC) = 1
                ) deduped
                """);

        if (decoded != null) {
            sql.append("""
                    WHERE (timestamp > ?)
                       OR (timestamp = ? AND recorded_at > ?)
                       OR (timestamp = ? AND recorded_at = ? AND topic > ?)
                       OR (timestamp = ? AND recorded_at = ? AND topic = ? AND partition > ?)
                       OR (timestamp = ? AND recorded_at = ? AND topic = ? AND partition = ? AND "offset" > ?)
                    """);
            Timestamp ts  = Timestamp.from(decoded.timestamp());
            Timestamp ra  = Timestamp.from(decoded.recordedAt());
            String    tp  = decoded.sourceTopic();
            int       p   = decoded.sourcePartition();
            long      o   = decoded.sourceOffset();
            params.add(ts);
            params.add(ts); params.add(ra);
            params.add(ts); params.add(ra); params.add(tp);
            params.add(ts); params.add(ra); params.add(tp); params.add(p);
            params.add(ts); params.add(ra); params.add(tp); params.add(p); params.add(o);
        }

        sql.append("ORDER BY timestamp ASC, recorded_at ASC, topic ASC, partition ASC, \"offset\" ASC\nLIMIT ?\n");
        params.add(limit + 1);

        List<EntityRecord> records = executeEntityQuery(sql.toString(), params);
        return TopicReplayService.buildPage(records, limit,
                r -> new EntityCursor(r.timestamp(), r.recordedAt(), r.topic(), r.partition(), r.offset()).encode());
    }

    /**
     * Streams all matching entity events through internal cursor pagination.
     */
    public void streamEntityEvents(
            String entityType, String entityId,
            Instant from, Instant to,
            Consumer<EntityRecord> sink
    ) throws SQLException {
        String pageCursor = null;
        do {
            PagedResponse<EntityRecord> page =
                    queryEntityEvents(entityType, entityId, from, to, STREAM_PAGE_SIZE, pageCursor);
            page.data().forEach(sink);
            pageCursor = page.nextCursor();
            if (!page.hasMore()) break;
        } while (true);
    }

    // -------------------------------------------------------------------------
    // Known-entity list / search
    // -------------------------------------------------------------------------

    /**
     * Lists known entities of {@code entityType} from {@code lake.known_entities},
     * ordered by {@code entity_id} ascending.
     */
    public PagedResponse<EntityInfo> listEntities(
            String entityType, int limit, String cursor
    ) throws SQLException {
        String afterId = cursor != null ? decodePlainCursor(cursor) : null;

        var sql = new StringBuilder("""
                SELECT entity_type, entity_id, entity_bucket, first_seen, last_seen
                FROM lake.known_entities
                WHERE entity_type = ?
                """);
        List<Object> params = new ArrayList<>();
        params.add(entityType);

        if (afterId != null) {
            sql.append("  AND entity_id > ?\n");
            params.add(afterId);
        }
        sql.append("ORDER BY entity_id ASC\nLIMIT ?\n");
        params.add(limit + 1);

        List<EntityInfo> entities = executeInfoQuery(sql.toString(), params);
        return TopicReplayService.buildPage(entities, limit,
                e -> encodePlainCursor(e.entityId()));
    }

    /**
     * Searches known entities whose {@code entity_id} contains {@code q} (case-insensitive).
     */
    public PagedResponse<EntityInfo> searchEntities(
            String entityType, String q, int limit, String cursor
    ) throws SQLException {
        String afterId = cursor != null ? decodePlainCursor(cursor) : null;

        var sql = new StringBuilder("""
                SELECT entity_type, entity_id, entity_bucket, first_seen, last_seen
                FROM lake.known_entities
                WHERE entity_type = ?
                  AND entity_id ILIKE ?
                """);
        List<Object> params = new ArrayList<>();
        params.add(entityType);
        params.add("%" + escapeLike(q) + "%");

        if (afterId != null) {
            sql.append("  AND entity_id > ?\n");
            params.add(afterId);
        }
        sql.append("ORDER BY entity_id ASC\nLIMIT ?\n");
        params.add(limit + 1);

        List<EntityInfo> entities = executeInfoQuery(sql.toString(), params);
        return TopicReplayService.buildPage(entities, limit,
                e -> encodePlainCursor(e.entityId()));
    }

    // -------------------------------------------------------------------------
    // Entity stats
    // -------------------------------------------------------------------------

    public EntityStats getEntityStats(String entityType, String entityId) throws SQLException {
        validateEntityType(entityType);
        String table = "lake.entity_" + entityType;

        // Aggregate stats from the entity cassette
        String aggSql = """
                SELECT COUNT(*) AS cnt,
                       MIN(timestamp) AS first_msg,
                       MAX(timestamp) AS last_msg
                FROM (
                    SELECT timestamp
                    FROM %s
                    WHERE entity_id = ?
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY topic, partition, "offset" ORDER BY recorded_at DESC) = 1
                ) deduped
                """.formatted(table);

        String topicSql = """
                SELECT topic, COUNT(*) AS cnt
                FROM (
                    SELECT topic
                    FROM %s
                    WHERE entity_id = ?
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY topic, partition, "offset" ORDER BY recorded_at DESC) = 1
                ) deduped
                GROUP BY topic
                ORDER BY topic
                """.formatted(table);

        String registrySql = """
                SELECT first_seen, last_seen
                FROM lake.known_entities
                WHERE entity_type = ? AND entity_id = ?
                """;

        long count = 0;
        Instant firstMsg = null;
        Instant lastMsg  = null;
        Map<String, Long> countByTopic = new LinkedHashMap<>();
        Instant firstSeen = null;
        Instant lastSeen  = null;

        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(aggSql)) {
                ps.setString(1, entityId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        count = rs.getLong("cnt");
                        Timestamp f = rs.getTimestamp("first_msg");
                        Timestamp l = rs.getTimestamp("last_msg");
                        if (f != null) firstMsg = f.toInstant();
                        if (l != null) lastMsg  = l.toInstant();
                    }
                }
            }
            try (PreparedStatement ps = duckDB.prepareStatement(topicSql)) {
                ps.setString(1, entityId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        countByTopic.put(rs.getString("topic"), rs.getLong("cnt"));
                    }
                }
            }
            try (PreparedStatement ps = duckDB.prepareStatement(registrySql)) {
                ps.setString(1, entityType);
                ps.setString(2, entityId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        firstSeen = rs.getTimestamp("first_seen").toInstant();
                        lastSeen  = rs.getTimestamp("last_seen").toInstant();
                    }
                }
            }
        }

        return new EntityStats(entityType, entityId, count, firstMsg, lastMsg,
                firstSeen, lastSeen, countByTopic);
    }

    // -------------------------------------------------------------------------
    // JDBC helpers
    // -------------------------------------------------------------------------

    private List<EntityRecord> executeEntityQuery(String sql, List<Object> params) throws SQLException {
        List<EntityRecord> records = new ArrayList<>();
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(sql)) {
                TopicReplayService.bindParams(ps, params);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        records.add(mapEntityRecord(rs));
                    }
                }
            }
        }
        return records;
    }

    private List<EntityInfo> executeInfoQuery(String sql, List<Object> params) throws SQLException {
        List<EntityInfo> entities = new ArrayList<>();
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(sql)) {
                TopicReplayService.bindParams(ps, params);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        entities.add(new EntityInfo(
                                rs.getString("entity_type"),
                                rs.getString("entity_id"),
                                rs.getInt("entity_bucket"),
                                rs.getTimestamp("first_seen").toInstant(),
                                rs.getTimestamp("last_seen").toInstant()
                        ));
                    }
                }
            }
        }
        return entities;
    }

    private static EntityRecord mapEntityRecord(ResultSet rs) throws SQLException {
        // Use positional index for "value" (column 9) to avoid a DuckDB JDBC bug
        // where rs.getBytes("value") incorrectly matches the struct sub-field named
        // "value" inside headers STRUCT(key VARCHAR, value BLOB)[] instead of the
        // top-level BLOB column. SELECT order:
        // entity_id(1), entity_bucket(2), topic(3), partition(4), "offset"(5),
        // timestamp(6), recorded_at(7), key(8), value(9), headers(10)
        return new EntityRecord(
                rs.getString("entity_id"),
                rs.getInt("entity_bucket"),
                rs.getString("topic"),
                rs.getInt("partition"),
                rs.getLong("offset"),
                rs.getTimestamp("timestamp").toInstant(),
                rs.getTimestamp("recorded_at").toInstant(),
                rs.getString("key"),
                TopicReplayService.encodeBlob(rs.getBytes(9)),
                TopicReplayService.mapHeaders(rs.getObject("headers"))
        );
    }

    private static String encodePlainCursor(String entityId) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(entityId.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodePlainCursor(String encoded) {
        return new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8);
    }

    private static String escapeLike(String q) {
        return q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    static void validateEntityType(String type) {
        if (type == null || !SAFE_IDENTIFIER.matcher(type).matches()) {
            throw new IllegalArgumentException(
                    "Invalid entity type '%s': must match [a-z][a-z0-9_]*".formatted(type));
        }
    }
}

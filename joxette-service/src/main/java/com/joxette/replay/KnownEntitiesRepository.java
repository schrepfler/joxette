package com.joxette.replay;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

/**
 * Maintains the {@code known_entities} registry (plain DuckDB, main schema).
 *
 * <p>Columns maintained per entity:
 * <ul>
 *   <li>{@code first_seen}        — immutable; set on first insert</li>
 *   <li>{@code last_seen}         — updated to the latest observed timestamp</li>
 *   <li>{@code message_count}     — incremented by the number of messages in each batch</li>
 *   <li>{@code source_topics}     — array of distinct Kafka topics that produced events;
 *       new topics are appended via {@code list_distinct(source_topics || [?])}</li>
 *   <li>{@code last_message_type} — overwritten with the most recent messageType</li>
 * </ul>
 */
@Repository
public class KnownEntitiesRepository {

    private final Connection duckDB;

    public KnownEntitiesRepository(DSLContext dsl) {
        // Extract the underlying JDBC connection — jOOQ's DSLContext.connectionResult()
        // is the cleanest way to get it without breaking the connection pool abstraction.
        this.duckDB = dsl.connectionResult(c -> c);
    }

    /**
     * Upserts all distinct (entityType, entityId) pairs found in {@code routes}.
     *
     * <p>Groups routes by (entityType, entityId) to compute the per-entity
     * message count, distinct topics, and last messageType for this batch, then
     * issues one PreparedStatement execution per distinct entity.
     */
    public void upsertBatch(List<EntityRoute> routes, Instant observedAt) throws SQLException {
        if (routes.isEmpty()) return;

        OffsetDateTime odt = observedAt.atOffset(ZoneOffset.UTC);

        // Aggregate per entity: count, topics, last messageType
        record EntitySummary(int count, String topic, String lastMessageType) {}

        Map<String, EntitySummary> byKey = new java.util.LinkedHashMap<>();
        for (EntityRoute r : routes) {
            String key = r.entityType() + "\0" + r.entityId();
            byKey.merge(key, new EntitySummary(1, r.topic(), r.messageType()),
                    (a, b) -> new EntitySummary(a.count() + 1, b.topic(), b.lastMessageType()));
        }

        // DuckDB array append: list_distinct(source_topics || CAST(? AS VARCHAR[]))
        // — appends the new topic and deduplicates in one expression.
        String sql = """
                INSERT INTO known_entities
                    (entity_type, entity_id, first_seen, last_seen,
                     message_count, source_topics, last_message_type)
                VALUES (?, ?, ?, ?, ?, CAST([?] AS VARCHAR[]), ?)
                ON CONFLICT (entity_type, entity_id) DO UPDATE SET
                    last_seen         = excluded.last_seen,
                    message_count     = known_entities.message_count + excluded.message_count,
                    source_topics     = list_distinct(
                                            list_concat(known_entities.source_topics,
                                                        excluded.source_topics)),
                    last_message_type = excluded.last_message_type
                """;

        try (PreparedStatement ps = duckDB.prepareStatement(sql)) {
            for (Map.Entry<String, EntitySummary> entry : byKey.entrySet()) {
                String[] parts = entry.getKey().split("\0", 2);
                EntitySummary s = entry.getValue();
                ps.setString(1, parts[0]);                   // entity_type
                ps.setString(2, parts[1]);                   // entity_id
                ps.setObject(3, odt);                        // first_seen
                ps.setObject(4, odt);                        // last_seen
                ps.setInt(5, s.count());                     // message_count
                ps.setString(6, s.topic());                  // source_topics element
                ps.setString(7, s.lastMessageType());        // last_message_type
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }
}

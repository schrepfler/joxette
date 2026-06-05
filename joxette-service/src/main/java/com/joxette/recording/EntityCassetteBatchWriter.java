package com.joxette.recording;

import com.joxette.replay.EntityRoute;
import com.joxette.replay.KafkaMessage;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

/**
 * Writes routed entity messages to their {@code lake.main.entity_{type}} cassette tables.
 *
 * <h2>Batch write</h2>
 * <p>{@link #writeBatch(List)} accepts the full list of {@code (routes, message)} pairs
 * for an entire {@link WriteBatch}, groups all rows by entity type across all messages,
 * and issues one multi-row INSERT per distinct entity type.  This replaces the previous
 * pattern of calling {@link #writeRoutes(List, KafkaMessage)} once per source Kafka
 * message, which issued N INSERTs for N messages even when all routes targeted the same
 * entity type.
 *
 * <h2>Per-message legacy API</h2>
 * <p>{@link #writeRoutes(List, KafkaMessage)} is retained for callers that process
 * messages individually, but {@link #writeBatch(List)} is preferred for bulk ingest.
 *
 * <h2>Headers</h2>
 * <p>Headers are stored as {@code STRUCT(key VARCHAR, value BLOB)[]} literals inlined
 * in the SQL — the same approach used by {@link CassetteBatchWriter} — because the
 * DuckDB JDBC driver cannot bind struct-array values via {@code setObject}.  Because
 * headers vary per message, the headers literal is computed per row.
 *
 * <p>Each instance owns a duplicated DuckDB connection so it does not contend
 * with the general-cassette writer or the compaction service on the shared connection.
 */
public class EntityCassetteBatchWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EntityCassetteBatchWriter.class);

    private final Connection conn;

    public EntityCassetteBatchWriter(Connection sharedDuckDbConnection) throws SQLException {
        DuckDBConnection duckConn = sharedDuckDbConnection.unwrap(DuckDBConnection.class);
        this.conn = duckConn.duplicate();
        log.info("EntityCassetteBatchWriter ready");
    }

    // -------------------------------------------------------------------------
    // Batch API (preferred — one INSERT per entity type per WriteBatch)
    // -------------------------------------------------------------------------

    /**
     * Writes all entity routes across all messages in a {@link WriteBatch} in bulk.
     *
     * <p>Rows are grouped by entity type; one multi-row INSERT is issued per distinct
     * entity type regardless of how many source messages contributed routes to it.
     * This reduces N×T INSERTs (N messages × T entity types) to T INSERTs per batch.
     */
    public void writeBatch(List<WriteBatch.EntityWriteItem> items) throws SQLException {
        if (items.isEmpty()) return;

        Timestamp recordedAt = Timestamp.from(Instant.now());

        // Group all (route, message) pairs by entity type across the whole batch
        // LinkedHashMap preserves insertion order for deterministic INSERT order
        Map<String, List<RouteWithMessage>> byType = new LinkedHashMap<>();
        for (WriteBatch.EntityWriteItem item : items) {
            for (EntityRoute route : item.routes()) {
                byType.computeIfAbsent(route.entityType(), k -> new ArrayList<>())
                      .add(new RouteWithMessage(route, item.message()));
            }
        }

        for (Map.Entry<String, List<RouteWithMessage>> entry : byType.entrySet()) {
            writeTypeRows(entry.getKey(), entry.getValue(), recordedAt);
        }
    }

    private void writeTypeRows(String entityType, List<RouteWithMessage> rows,
                                Timestamp recordedAt) throws SQLException {
        String tbl = "lake.main.entity_" + entityType;

        // Build multi-row INSERT — headers inlined per row since they vary per message
        StringBuilder sql = new StringBuilder(
                "INSERT INTO " + tbl +
                " (recorded_at, entity_id, bucket, message_type, topic," +
                "  kafka_offset, kafka_partition, kafka_timestamp," +
                "  kafka_key, kafka_value, kafka_value_str, metadata, headers) VALUES ");

        boolean first = true;
        for (RouteWithMessage rwm : rows) {
            if (!first) sql.append(',');
            first = false;
            String headersLiteral = CassetteBatchWriter.headersToStructLiteral(rwm.message().headers());
            sql.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ").append(headersLiteral).append(')');
        }

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            for (RouteWithMessage rwm : rows) {
                KafkaMessage msg = rwm.message();
                String valueStr = msg.value() != null ? new String(msg.value(), StandardCharsets.UTF_8) : null;
                ps.setTimestamp(idx++, recordedAt);
                ps.setString(idx++, rwm.route().entityId());
                ps.setInt(idx++, rwm.route().entityBucket());
                ps.setString(idx++, rwm.route().messageType());
                ps.setString(idx++, msg.topic());
                ps.setLong(idx++, msg.offset());
                ps.setInt(idx++, msg.partition());
                ps.setTimestamp(idx++, new Timestamp(msg.timestampMs()));
                ps.setString(idx++, msg.key());
                ps.setBytes(idx++, msg.value());
                ps.setString(idx++, valueStr);
            }
            ps.executeUpdate();
        }
        log.debug("Wrote {} entity row(s) to entity_{}", rows.size(), entityType);
    }

    // -------------------------------------------------------------------------
    // Per-message API (retained for compatibility)
    // -------------------------------------------------------------------------

    /**
     * Inserts all (route, message) pairs for a single Kafka message.
     *
     * <p>Prefer {@link #writeBatch(List)} for bulk ingest — it reduces the number
     * of DuckDB INSERTs from one-per-message to one-per-entity-type-per-batch.
     */
    public void writeRoutes(List<EntityRoute> routes, KafkaMessage message) throws SQLException {
        if (routes.isEmpty()) return;
        Timestamp recordedAt = Timestamp.from(Instant.now());
        List<RouteWithMessage> rows = new ArrayList<>(routes.size());
        for (EntityRoute r : routes) rows.add(new RouteWithMessage(r, message));
        // Group by entity type (usually just one type per message)
        Map<String, List<RouteWithMessage>> byType = new LinkedHashMap<>();
        for (RouteWithMessage rwm : rows) {
            byType.computeIfAbsent(rwm.route().entityType(), k -> new ArrayList<>()).add(rwm);
        }
        for (Map.Entry<String, List<RouteWithMessage>> entry : byType.entrySet()) {
            writeTypeRows(entry.getKey(), entry.getValue(), recordedAt);
        }
    }

    // -------------------------------------------------------------------------

    private record RouteWithMessage(EntityRoute route, KafkaMessage message) {}

    @Override
    public void close() throws SQLException {
        conn.close();
    }
}

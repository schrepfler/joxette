package com.joxette.recording;

import com.joxette.replay.EntityRoute;
import com.joxette.replay.KafkaMessage;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Writes routed entity messages to their {@code lake.main.entity_{type}} cassette tables.
 *
 * <p>Each call to {@link #writeRoutes(List, KafkaMessage)} groups the supplied routes by entity type
 * and issues one multi-row INSERT per distinct entity type.  Headers are stored
 * as {@code STRUCT(key VARCHAR, value BLOB)[]} literals inlined in the SQL —
 * the same approach used by {@link CassetteBatchWriter} — because the DuckDB
 * JDBC driver cannot bind struct-array values via {@code setObject}.
 *
 * <p>Because headers vary per message the SQL string is built fresh for each
 * batch step rather than being cached as a prepared statement.  The per-entity-type
 * grouping still amortises round-trip overhead when many routes target the same type.
 *
 * <p>Each instance owns a duplicated DuckDB connection so it does not contend
 * with the general-cassette writer or the compaction service on the shared
 * connection.
 */
public class EntityCassetteBatchWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EntityCassetteBatchWriter.class);

    private final Connection conn;

    public EntityCassetteBatchWriter(Connection sharedDuckDbConnection) throws SQLException {
        DuckDBConnection duckConn = sharedDuckDbConnection.unwrap(DuckDBConnection.class);
        this.conn = duckConn.duplicate();
        log.info("EntityCassetteBatchWriter ready");
    }

    /**
     * Inserts all (route, message) pairs into the appropriate entity cassette tables.
     *
     * <p>Routes are grouped by entity type; one multi-row INSERT is issued per
     * distinct entity type.  Headers are serialised as DuckDB struct-array
     * literals inlined in the SQL so the correct column type is preserved.
     *
     * @param routes  routing decisions produced by {@link com.joxette.replay.MessageRouter}
     * @param message the original Kafka message (shared across all routes in this call)
     */
    public void writeRoutes(List<EntityRoute> routes, KafkaMessage message) throws SQLException {
        if (routes.isEmpty()) return;

        Timestamp recordedAt = Timestamp.from(Instant.now());
        Timestamp kafkaTs    = new Timestamp(message.timestampMs());

        // Pre-compute the headers literal — same for all routes on this message
        String headersLiteral = CassetteBatchWriter.headersToStructLiteral(message.headers());

        // Group by entity type to produce one INSERT per table
        Map<String, List<EntityRoute>> byType = new HashMap<>();
        for (EntityRoute route : routes) {
            byType.computeIfAbsent(route.entityType(), k -> new ArrayList<>()).add(route);
        }

        for (Map.Entry<String, List<EntityRoute>> entry : byType.entrySet()) {
            String entityType = entry.getKey();
            List<EntityRoute> typeRoutes = entry.getValue();
            String tbl = "lake.main.entity_" + entityType;

            // Build a multi-row INSERT.  11 bound params per row; metadata and headers inlined.
            StringBuilder sql = new StringBuilder(
                    "INSERT INTO " + tbl +
                    " (recorded_at, entity_id, bucket, message_type, topic," +
                    "  kafka_offset, kafka_partition, kafka_timestamp," +
                    "  kafka_key, kafka_value, kafka_value_str, metadata, headers) VALUES ");

            boolean first = true;
            for (EntityRoute ignored : typeRoutes) {
                if (!first) sql.append(',');
                first = false;
                sql.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ").append(headersLiteral).append(')');
            }

            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                int idx = 1;
                String valueStr = message.value() != null ? new String(message.value()) : null;
                for (EntityRoute route : typeRoutes) {
                    ps.setTimestamp(idx++, recordedAt);
                    ps.setString(idx++, route.entityId());
                    ps.setInt(idx++, route.entityBucket());
                    ps.setString(idx++, route.messageType());
                    ps.setString(idx++, message.topic());
                    ps.setLong(idx++, message.offset());
                    ps.setInt(idx++, message.partition());
                    ps.setTimestamp(idx++, kafkaTs);
                    ps.setString(idx++, message.key());
                    ps.setBytes(idx++, message.value());
                    ps.setString(idx++, valueStr);
                }
                ps.executeUpdate();
            }
            log.debug("Wrote {} entity route(s) to entity_{}", typeRoutes.size(), entityType);
        }
    }

    @Override
    public void close() throws SQLException {
        conn.close();
    }
}

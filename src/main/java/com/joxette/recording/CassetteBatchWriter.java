package com.joxette.recording;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

/**
 * Writes batches of Kafka records to the DuckLake general cassette table for a
 * single topic: {@code lake.cassette_{topic}}.
 *
 * <p>Each instance owns a duplicated DuckDB connection so that concurrent
 * per-topic writers do not contend on the shared {@code Connection} bean.
 * DuckDB serialises concurrent writers internally.
 *
 * <p>Headers are stored as a JSON array of {@code {"key":"...","value":"<base64>"}}
 * objects in a VARCHAR column.  A migration to the native
 * {@code LIST(STRUCT(key VARCHAR, value BLOB))} type (compatible with
 * {@link com.joxette.replay.HeadersHelper} macros) can be applied once DuckDB
 * JDBC exposes a stable appender API for nested types.
 */
public class CassetteBatchWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CassetteBatchWriter.class);

    private final String topic;
    private final String qualifiedTable;
    private final Connection conn;

    public CassetteBatchWriter(String topic, Connection sharedDuckDbConnection) throws SQLException {
        this.topic = topic;
        this.qualifiedTable = "lake.cassette_" + sanitizeTopicName(topic);
        // Duplicate the connection — each writer runs on its own virtual thread.
        DuckDBConnection duckConn = sharedDuckDbConnection.unwrap(DuckDBConnection.class);
        this.conn = duckConn.duplicate();
        ensureSchema();
        ensureTable();
    }

    // -----------------------------------------------------------------------
    // Schema / table DDL
    // -----------------------------------------------------------------------

    private void ensureSchema() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS lake");
        }
    }

    private void ensureTable() throws SQLException {
        String ddl = """
                CREATE TABLE IF NOT EXISTS %s (
                    partition    INTEGER   NOT NULL,
                    "offset"     BIGINT    NOT NULL,
                    ts           BIGINT    NOT NULL,
                    key          VARCHAR,
                    value        BLOB,
                    headers      VARCHAR,
                    recorded_at  TIMESTAMP NOT NULL
                )
                """.formatted(qualifiedTable);
        try (Statement st = conn.createStatement()) {
            st.execute(ddl);
        }
        log.info("Ensured cassette table {} exists", qualifiedTable);
    }

    // -----------------------------------------------------------------------
    // Batch write
    // -----------------------------------------------------------------------

    /**
     * Bulk-inserts {@code batch} into the cassette table in a single JDBC
     * batch execution.  Callers should commit Kafka offsets only after this
     * method returns without throwing.
     */
    public void writeBatch(List<ConsumerRecord<String, byte[]>> batch) throws SQLException {
        if (batch.isEmpty()) return;

        String sql = "INSERT INTO " + qualifiedTable +
                " (partition, \"offset\", ts, key, value, headers, recorded_at)" +
                " VALUES (?, ?, ?, ?, ?, ?, ?)";

        Timestamp now = Timestamp.from(Instant.now());

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (ConsumerRecord<String, byte[]> record : batch) {
                ps.setInt(1, record.partition());
                ps.setLong(2, record.offset());
                ps.setLong(3, record.timestamp());
                ps.setString(4, record.key());
                ps.setBytes(5, record.value());
                ps.setString(6, headersToJson(record));
                ps.setTimestamp(7, now);
                ps.addBatch();
            }
            ps.executeBatch();
        }

        log.debug("Wrote batch of {} records to {}", batch.size(), qualifiedTable);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Replaces characters that are not valid in a DuckDB identifier with
     * underscores.  Kafka topic names allow {@code [a-zA-Z0-9._-]}.
     */
    static String sanitizeTopicName(String topic) {
        return topic.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    private static String headersToJson(ConsumerRecord<String, byte[]> record) {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (Header h : record.headers()) {
            if (!first) sb.append(',');
            sb.append("{\"key\":\"").append(escapeJson(h.key())).append("\",\"value\":\"");
            if (h.value() != null) {
                sb.append(Base64.getEncoder().encodeToString(h.value()));
            }
            sb.append("\"}");
            first = false;
        }
        return sb.append(']').toString();
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    @Override
    public void close() throws SQLException {
        conn.close();
    }
}

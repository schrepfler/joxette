package com.joxette.recording;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/**
 * Writes batches of Kafka records to the DuckLake general cassette table for a
 * single topic: {@code lake.main.general_{topic}}.
 *
 * <p>The table schema is managed by {@link com.joxette.db.SchemaManager}.
 * This class assumes the table already exists (created at startup) and only
 * performs INSERT operations.
 *
 * <p>Each instance owns a duplicated DuckDB connection so that concurrent
 * per-topic writers do not contend on the shared {@code Connection} bean.
 * DuckDB serialises concurrent writers internally.
 *
 * <p>Headers are stored as a {@code STRUCT(key VARCHAR, value VARCHAR)[]} array.
 * Header values are decoded as UTF-8 on write; non-UTF-8 binary values are
 * base64-encoded so the round-trip is lossless.
 */
public class CassetteBatchWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CassetteBatchWriter.class);

    private final String topic;
    private final String qualifiedTable;
    private final Connection conn;

    public CassetteBatchWriter(String topic, Connection sharedDuckDbConnection) throws SQLException {
        this.topic = topic;
        this.qualifiedTable = "lake.main.general_" + normalizeTopicName(topic);
        // Duplicate the connection — each writer runs on its own virtual thread.
        DuckDBConnection duckConn = sharedDuckDbConnection.unwrap(DuckDBConnection.class);
        this.conn = duckConn.duplicate();
        log.info("CassetteBatchWriter ready for topic '{}' → {}", topic, qualifiedTable);
    }

    // -----------------------------------------------------------------------
    // Batch write
    // -----------------------------------------------------------------------

    /**
     * Bulk-inserts {@code batch} into the general cassette table in a single JDBC
     * batch execution.  Callers should commit Kafka offsets only after this
     * method returns without throwing.
     *
     * @param batch        the Kafka consumer records to write
     * @param messageTypes parallel list of {@code message_type} labels (same size as
     *                     {@code batch}); individual entries may be {@code null} when
     *                     no {@code topic_message_type_matchers} row matched
     */
    public void writeBatch(List<ConsumerRecord<String, byte[]>> batch,
                           List<String> messageTypes) throws SQLException {
        if (batch.isEmpty()) return;

        // headers column is STRUCT(key VARCHAR, value BLOB)[] — pass a DuckDB struct-array
        // literal via a VALUES sub-expression so the driver doesn't attempt a VARCHAR cast.
        // We build one row of parameters per record (8 bound params) and append the headers
        // struct-array literal inline using DuckDB's list/struct syntax.
        StringBuilder sql = new StringBuilder(
                "INSERT INTO " + qualifiedTable +
                " (recorded_at, kafka_offset, kafka_partition, kafka_timestamp," +
                "  kafka_key, kafka_value, kafka_value_str, metadata, headers, message_type) VALUES ");

        Timestamp now = Timestamp.from(Instant.now());
        boolean first = true;
        List<Object[]> params = new java.util.ArrayList<>(batch.size());
        for (int i = 0; i < batch.size(); i++) {
            ConsumerRecord<String, byte[]> record = batch.get(i);
            if (!first) sql.append(',');
            first = false;
            // 8 bound params; headers and metadata are inlined as literals
            sql.append("(?, ?, ?, ?, ?, ?, ?, NULL, ")
               .append(headersToStructLiteral(record))
               .append(", ?)");
            params.add(new Object[]{
                now,
                record.offset(),
                record.partition(),
                new Timestamp(record.timestamp()),
                record.key(),
                record.value(),
                record.value() != null ? new String(record.value(), StandardCharsets.UTF_8) : null,
                messageTypes.get(i)
            });
        }

        try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            for (Object[] row : params) {
                ps.setTimestamp(idx++, (Timestamp) row[0]);
                ps.setLong(idx++, (Long) row[1]);
                ps.setInt(idx++, (Integer) row[2]);
                ps.setTimestamp(idx++, (Timestamp) row[3]);
                ps.setString(idx++, (String) row[4]);
                ps.setBytes(idx++, (byte[]) row[5]);
                ps.setString(idx++, (String) row[6]);
                ps.setString(idx++, (String) row[7]);
            }
            ps.executeUpdate();
        }

        log.debug("Wrote batch of {} records to {}", batch.size(), qualifiedTable);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Replaces characters that are not valid in a DuckDB identifier with
     * underscores, and lowercases the result.
     */
    static String normalizeTopicName(String topic) {
        return topic.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }

    /**
     * Serialises the headers of a {@link ConsumerRecord} as a DuckDB
     * {@code STRUCT(key VARCHAR, value VARCHAR)[]} literal inlined in SQL.
     *
     * <p>Header values are decoded as UTF-8. Values that are not valid UTF-8
     * (binary payloads) are base64-encoded so no data is lost.
     */
    static String headersToStructLiteral(ConsumerRecord<String, byte[]> record) {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (Header h : record.headers()) {
            if (!first) sb.append(", ");
            first = false;
            appendHeaderEntry(sb, h.key(), h.value());
        }
        return sb.append(']').toString();
    }

    /**
     * Serialises a {@link com.joxette.replay.KafkaMessage.Header} list as a DuckDB
     * {@code STRUCT(key VARCHAR, value VARCHAR)[]} literal inlined in SQL.
     *
     * <p>Shared with {@link EntityCassetteBatchWriter}.
     *
     * <p>Header values are decoded as UTF-8. Values that are not valid UTF-8
     * (binary payloads) are base64-encoded so no data is lost.
     */
    static String headersToStructLiteral(List<com.joxette.replay.KafkaMessage.Header> headers) {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (com.joxette.replay.KafkaMessage.Header h : headers) {
            if (!first) sb.append(", ");
            first = false;
            appendHeaderEntry(sb, h.key(), h.value());
        }
        return sb.append(']').toString();
    }

    /**
     * Appends one {@code {'key': '...', 'value': '...'}} VARCHAR struct entry.
     *
     * <p>The value bytes are decoded as UTF-8. If decoding fails (binary payload),
     * the bytes are base64-encoded instead. Single quotes in both key and value
     * are escaped as {@code ''} per SQL standard.
     */
    private static void appendHeaderEntry(StringBuilder sb, String key, byte[] value) {
        sb.append("{'key': '").append(escapeSingleQuote(key)).append("', 'value': '");
        if (value != null && value.length > 0) {
            sb.append(escapeSingleQuote(decodeHeaderValue(value)));
        }
        sb.append("'}");
    }

    /**
     * Decodes header value bytes as UTF-8.  Falls back to base64 for non-UTF-8 binary values.
     *
     * <p>Uses the decoder's CharBuffer output directly to avoid the double-allocation that
     * occurs when first decoding to validate and then constructing a separate String.
     */
    static String decodeHeaderValue(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return "";
        try {
            return java.nio.charset.StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(bytes))
                    .toString();
        } catch (java.nio.charset.CharacterCodingException e) {
            // Binary payload — base64-encode so it survives the VARCHAR round-trip
            return java.util.Base64.getEncoder().encodeToString(bytes);
        }
    }

    private static String escapeSingleQuote(String s) {
        return s.replace("'", "''");
    }

    @Override
    public void close() throws SQLException {
        conn.close();
    }
}

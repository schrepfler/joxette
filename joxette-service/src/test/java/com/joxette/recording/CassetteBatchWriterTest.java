package com.joxette.recording;

import com.joxette.support.DuckDBTestSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct unit coverage for {@link CassetteBatchWriter#writeBatch} against a real
 * in-memory DuckDB — locks down the write contract (column values, headers
 * encoding, batch semantics) before/across the JDBC-batch-INSERT → DuckDBAppender
 * conversion. {@code HeadersRoundTripIT} covers the same headers contract through
 * the full write→REST-read path; this class is the fast, no-Kafka-needed
 * complement covering every column, not just headers.
 */
class CassetteBatchWriterTest {

    private static final String TOPIC = "orders.events";
    private static final String TABLE = "lake.main.general_orders_events";

    private Connection duckDB;
    private CassetteBatchWriter writer;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC);
        writer = new CassetteBatchWriter(TOPIC, duckDB);
    }

    @AfterEach
    void tearDown() throws Exception {
        writer.close();
        duckDB.close();
    }

    private static ConsumerRecord<String, byte[]> record(
            int partition, long offset, long timestampMs, String key, byte[] value, RecordHeaders headers) {
        return new ConsumerRecord<>(TOPIC, partition, offset, timestampMs, TimestampType.CREATE_TIME,
                -1, -1, key, value, headers, Optional.empty());
    }

    @Test
    void writeBatch_emptyBatch_isNoOp() throws Exception {
        writer.writeBatch(List.of(), List.of());

        assertThat(DuckDBTestSupport.countRows(duckDB, TABLE)).isZero();
    }

    @Test
    void writeBatch_singleRecord_insertsAllColumnsCorrectly() throws Exception {
        ConsumerRecord<String, byte[]> r = record(
                3, 42L, 1_700_000_000_000L, "order-1", "payload".getBytes(StandardCharsets.UTF_8),
                new RecordHeaders());

        writer.writeBatch(List.of(r), List.of("OrderCreated"));

        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery("SELECT kafka_offset, kafka_partition, kafka_timestamp, " +
                     "kafka_key, kafka_value, message_type, metadata FROM " + TABLE)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("kafka_offset")).isEqualTo(42L);
            assertThat(rs.getInt("kafka_partition")).isEqualTo(3);
            assertThat(rs.getTimestamp("kafka_timestamp").getTime()).isEqualTo(1_700_000_000_000L);
            assertThat(rs.getString("kafka_key")).isEqualTo("order-1");
            assertThat(readBlob(rs, "kafka_value")).isEqualTo("payload".getBytes(StandardCharsets.UTF_8));
            assertThat(rs.getString("message_type")).isEqualTo("OrderCreated");
            assertThat(rs.getString("metadata")).as("metadata column is never populated by this writer").isNull();
            assertThat(rs.next()).isFalse();
        }
    }

    @Test
    void writeBatch_nullKeyAndValue_insertedAsNull() throws Exception {
        ConsumerRecord<String, byte[]> r = record(0, 0L, 0L, null, null, new RecordHeaders());

        writer.writeBatch(List.of(r), Collections.singletonList(null));

        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery("SELECT kafka_key, kafka_value, message_type FROM " + TABLE)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("kafka_key")).isNull();
            assertThat(rs.getObject("kafka_value")).isNull();
            assertThat(rs.getString("message_type")).isNull();
        }
    }

    @Test
    void writeBatch_multipleRecords_allInsertedInOrder() throws Exception {
        List<ConsumerRecord<String, byte[]>> batch = List.of(
                record(0, 0L, 100L, "k0", "v0".getBytes(StandardCharsets.UTF_8), new RecordHeaders()),
                record(0, 1L, 200L, "k1", "v1".getBytes(StandardCharsets.UTF_8), new RecordHeaders()),
                record(1, 0L, 300L, "k2", "v2".getBytes(StandardCharsets.UTF_8), new RecordHeaders()));
        List<String> types = java.util.Arrays.asList("A", "B", null);

        writer.writeBatch(batch, types);

        assertThat(DuckDBTestSupport.countRows(duckDB, TABLE)).isEqualTo(3);
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT kafka_partition, kafka_offset, kafka_key FROM " + TABLE +
                     " ORDER BY kafka_partition, kafka_offset")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("kafka_key")).isEqualTo("k0");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("kafka_key")).isEqualTo("k1");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("kafka_key")).isEqualTo("k2");
        }
    }

    @Test
    void writeBatch_headers_utf8ValuesStoredVerbatim() throws Exception {
        RecordHeaders headers = new RecordHeaders();
        headers.add("content-type", "application/json".getBytes(StandardCharsets.UTF_8));
        headers.add("x-trace-id", "abc-123".getBytes(StandardCharsets.UTF_8));

        writer.writeBatch(List.of(record(0, 0L, 0L, "k", "v".getBytes(StandardCharsets.UTF_8), headers)),
                Collections.singletonList(null));

        List<Object[]> rows = queryHeaders();
        assertThat(rows).containsExactly(
                new Object[]{"content-type", "application/json"},
                new Object[]{"x-trace-id", "abc-123"});
    }

    @Test
    void writeBatch_headers_nonUtf8BinaryValue_base64Encoded() throws Exception {
        byte[] binaryPayload = {(byte) 0xFF, (byte) 0xFE, (byte) 0x00};
        RecordHeaders headers = new RecordHeaders();
        headers.add("x-binary", binaryPayload);

        writer.writeBatch(List.of(record(0, 0L, 0L, "k", null, headers)), Collections.singletonList(null));

        List<Object[]> rows = queryHeaders();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0)[0]).isEqualTo("x-binary");
        assertThat(rows.get(0)[1]).isEqualTo(Base64.getEncoder().encodeToString(binaryPayload));
    }

    @Test
    void writeBatch_headers_duplicateKeysPreservedInOrder() throws Exception {
        RecordHeaders headers = new RecordHeaders();
        headers.add("content-type", "application/json".getBytes(StandardCharsets.UTF_8));
        headers.add("content-type", "text/plain".getBytes(StandardCharsets.UTF_8));

        writer.writeBatch(List.of(record(0, 0L, 0L, "k", null, headers)), Collections.singletonList(null));

        List<Object[]> rows = queryHeaders();
        assertThat(rows).containsExactly(
                new Object[]{"content-type", "application/json"},
                new Object[]{"content-type", "text/plain"});
    }

    @Test
    void writeBatch_headers_emptyList_storedAsEmptyArray() throws Exception {
        writer.writeBatch(List.of(record(0, 0L, 0L, "k", null, new RecordHeaders())), Collections.singletonList(null));

        assertThat(queryHeaders()).isEmpty();
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery("SELECT headers FROM " + TABLE)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getObject("headers")).as("empty headers must be [] not NULL").isNotNull();
        }
    }

    /** DuckDB's JDBC driver returns a {@link java.sql.Blob} wrapper from getObject for BLOB columns. */
    private static byte[] readBlob(ResultSet rs, String column) throws Exception {
        java.sql.Blob blob = rs.getBlob(column);
        return blob == null ? null : blob.getBytes(1, (int) blob.length());
    }

    /** Flattens the single row's headers column into (key, value) pairs, in array order. */
    private List<Object[]> queryHeaders() throws Exception {
        List<Object[]> result = new java.util.ArrayList<>();
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT unnest(headers).key AS k, unnest(headers).value AS v FROM " + TABLE)) {
            while (rs.next()) {
                result.add(new Object[]{rs.getString("k"), rs.getString("v")});
            }
        }
        return result;
    }
}

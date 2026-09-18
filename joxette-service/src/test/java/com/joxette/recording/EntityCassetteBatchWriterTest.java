package com.joxette.recording;

import com.joxette.replay.EntityRoute;
import com.joxette.replay.KafkaMessage;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct unit coverage for {@link EntityCassetteBatchWriter#writeBatch} against a
 * real in-memory DuckDB — locks down the write contract (column values, headers
 * encoding, per-entity-type grouping) before/across the JDBC-batch-INSERT →
 * DuckDBAppender conversion.
 */
class EntityCassetteBatchWriterTest {

    private static final String ENTITY_TYPE = "order";
    private static final String TABLE = "lake.main.entity_order";

    private Connection duckDB;
    private EntityCassetteBatchWriter writer;

    @BeforeEach
    void setUp() throws Exception {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createEntityTable(duckDB, ENTITY_TYPE);
        writer = new EntityCassetteBatchWriter(duckDB);
    }

    @AfterEach
    void tearDown() throws Exception {
        writer.close();
        duckDB.close();
    }

    private static KafkaMessage message(String topic, int partition, long offset, long timestampMs,
                                        String key, byte[] value, List<KafkaMessage.Header> headers) {
        return new KafkaMessage(topic, partition, offset, timestampMs, key, value, headers);
    }

    private static WriteBatch.EntityWriteItem item(EntityRoute route, KafkaMessage message) {
        return new WriteBatch.EntityWriteItem(List.of(route), message);
    }

    @Test
    void writeBatch_emptyItems_isNoOp() throws Exception {
        writer.writeBatch(List.of());

        assertThat(DuckDBTestSupport.countRows(duckDB, TABLE)).isZero();
    }

    @Test
    void writeBatch_singleRoute_insertsAllColumnsCorrectly() throws Exception {
        EntityRoute route = new EntityRoute(ENTITY_TYPE, "ORD-1", 7, "OrderCreated", "orders.events");
        KafkaMessage msg = message("orders.events", 2, 99L, 1_700_000_000_000L,
                "k1", "payload".getBytes(StandardCharsets.UTF_8), List.of());

        writer.writeBatch(List.of(item(route, msg)));

        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery("SELECT entity_id, bucket, message_type, topic, " +
                     "kafka_offset, kafka_partition, kafka_timestamp, kafka_key, kafka_value, metadata FROM " + TABLE)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("entity_id")).isEqualTo("ORD-1");
            assertThat(rs.getInt("bucket")).isEqualTo(7);
            assertThat(rs.getString("message_type")).isEqualTo("OrderCreated");
            assertThat(rs.getString("topic")).isEqualTo("orders.events");
            assertThat(rs.getLong("kafka_offset")).isEqualTo(99L);
            assertThat(rs.getInt("kafka_partition")).isEqualTo(2);
            assertThat(rs.getTimestamp("kafka_timestamp").getTime()).isEqualTo(1_700_000_000_000L);
            assertThat(rs.getString("kafka_key")).isEqualTo("k1");
            assertThat(readBlob(rs, "kafka_value")).isEqualTo("payload".getBytes(StandardCharsets.UTF_8));
            assertThat(rs.getString("metadata")).as("metadata column is never populated by this writer").isNull();
        }
    }

    @Test
    void writeBatch_multipleEntityTypes_oneInsertPerTypeAllRowsPresent() throws Exception {
        DuckDBTestSupport.createEntityTable(duckDB, "payment");

        EntityRoute orderRoute = new EntityRoute(ENTITY_TYPE, "ORD-1", 0, "OrderCreated", "orders.events");
        EntityRoute paymentRoute = new EntityRoute("payment", "PAY-1", 0, "PaymentReceived", "payments.events");
        KafkaMessage msg1 = message("orders.events", 0, 0L, 0L, "k1", null, List.of());
        KafkaMessage msg2 = message("payments.events", 0, 0L, 0L, "k2", null, List.of());

        writer.writeBatch(List.of(item(orderRoute, msg1), item(paymentRoute, msg2)));

        assertThat(DuckDBTestSupport.countRows(duckDB, TABLE)).isEqualTo(1);
        assertThat(DuckDBTestSupport.countRows(duckDB, "lake.main.entity_payment")).isEqualTo(1);
    }

    @Test
    void writeBatch_oneMessageRoutedToMultipleEntities_insertsOneRowPerRoute() throws Exception {
        // One message can match several entity source mappings — e.g. an order-line
        // event that is routed to both "order" and "sku" entity types.
        DuckDBTestSupport.createEntityTable(duckDB, "sku");
        KafkaMessage msg = message("orders.events", 0, 0L, 0L, "k", null, List.of());
        WriteBatch.EntityWriteItem multiRouteItem = new WriteBatch.EntityWriteItem(List.of(
                new EntityRoute(ENTITY_TYPE, "ORD-1", 0, "OrderCreated", "orders.events"),
                new EntityRoute("sku", "SKU-9", 0, "OrderCreated", "orders.events")), msg);

        writer.writeBatch(List.of(multiRouteItem));

        assertThat(DuckDBTestSupport.countRows(duckDB, TABLE)).isEqualTo(1);
        assertThat(DuckDBTestSupport.countRows(duckDB, "lake.main.entity_sku")).isEqualTo(1);
    }

    @Test
    void writeBatch_headers_utf8ValuesStoredVerbatim() throws Exception {
        EntityRoute route = new EntityRoute(ENTITY_TYPE, "ORD-1", 0, null, "orders.events");
        List<KafkaMessage.Header> headers = List.of(
                new KafkaMessage.Header("content-type", "application/json".getBytes(StandardCharsets.UTF_8)),
                new KafkaMessage.Header("x-trace-id", "abc-123".getBytes(StandardCharsets.UTF_8)));
        KafkaMessage msg = message("orders.events", 0, 0L, 0L, "k", null, headers);

        writer.writeBatch(List.of(item(route, msg)));

        assertThat(queryHeaders()).containsExactly(
                new Object[]{"content-type", "application/json"},
                new Object[]{"x-trace-id", "abc-123"});
    }

    @Test
    void writeBatch_headers_nonUtf8BinaryValue_base64Encoded() throws Exception {
        byte[] binaryPayload = {(byte) 0xFF, (byte) 0xFE, (byte) 0x00};
        EntityRoute route = new EntityRoute(ENTITY_TYPE, "ORD-1", 0, null, "orders.events");
        List<KafkaMessage.Header> headers = List.of(new KafkaMessage.Header("x-binary", binaryPayload));
        KafkaMessage msg = message("orders.events", 0, 0L, 0L, "k", null, headers);

        writer.writeBatch(List.of(item(route, msg)));

        List<Object[]> rows = queryHeaders();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0)[0]).isEqualTo("x-binary");
        assertThat(rows.get(0)[1]).isEqualTo(Base64.getEncoder().encodeToString(binaryPayload));
    }

    @Test
    void writeBatch_headers_emptyList_storedAsEmptyArray() throws Exception {
        EntityRoute route = new EntityRoute(ENTITY_TYPE, "ORD-1", 0, null, "orders.events");
        KafkaMessage msg = message("orders.events", 0, 0L, 0L, "k", null, List.of());

        writer.writeBatch(List.of(item(route, msg)));

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

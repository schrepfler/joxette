package com.joxette.it;

import com.joxette.recording.CassetteBatchWriter;
import com.joxette.replay.CassetteRecord;
import com.joxette.replay.PagedResponse;
import com.joxette.support.DuckDBTestSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying that Kafka message headers survive the full
 * write → store → read round-trip through {@link CassetteBatchWriter} and the
 * REST replay API.
 *
 * <h2>What is tested</h2>
 * <ol>
 *   <li>UTF-8 header values are returned verbatim (no encoding applied).</li>
 *   <li>Non-UTF-8 binary values are stored as standard Base64 by
 *       {@code CassetteBatchWriter.decodeHeaderValue()} and returned as that
 *       Base64 string — the caller can detect and decode them.</li>
 *   <li>Duplicate header keys are all preserved in insertion order.</li>
 * </ol>
 *
 * <h2>Storage contract</h2>
 * <p>Headers are stored as {@code STRUCT(key VARCHAR, value VARCHAR)[]} in the
 * DuckLake general-cassette table.  Non-UTF-8 byte arrays are converted to
 * standard (not URL-safe) Base64 by {@link CassetteBatchWriter} before storage.
 * {@link com.joxette.replay.TopicReplayService#mapHeaders} returns the VARCHAR
 * value verbatim — no additional encoding is applied — so the response
 * {@code CassetteRecord.Header.value} is either the original UTF-8 string or
 * the Base64-encoded fallback.
 *
 * <h2>Setup</h2>
 * <p>A Kafka container is required because the Spring Boot context (even with
 * {@code @ActiveProfiles("it")}) includes the Kafka consumer infrastructure.
 * The cassette table for the test topic is created explicitly in
 * {@code @BeforeEach} via {@link DuckDBTestSupport#createGeneralCassetteTable}
 * because the {@code it} profile has no bootstrap topics.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class HeadersRoundTripIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("joxette.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    /** Kafka topic name used across all test methods in this class. */
    private static final String TOPIC = "headers.roundtrip.test";

    /**
     * Normalised table suffix: {@code "headers_roundtrip_test"}.
     * The general-cassette table is {@code lake.main.general_headers_roundtrip_test}.
     */
    private static final String NORMALIZED_TOPIC = "headers_roundtrip_test";

    @LocalServerPort
    private int port;

    private final RestTemplate restTemplate = new RestTemplate();

    /** Shared DuckDB connection from the Spring context. */
    @Autowired
    private Connection duckDB;

    @BeforeEach
    void setUp() throws Exception {
        // Create the general cassette table for the test topic (IF NOT EXISTS — idempotent).
        // The it profile has no bootstrap topics so SchemaManager does not create this table.
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC);

        // Wipe any rows left by a previous test method so each test starts clean.
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.main.general_" + NORMALIZED_TOPIC);
        }
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    void headers_utf8ValuesReturnedVerbatim() throws Exception {
        RecordHeaders headers = new RecordHeaders();
        headers.add("content-type", "application/json".getBytes(StandardCharsets.UTF_8));
        headers.add("x-trace-id",   "abc-123".getBytes(StandardCharsets.UTF_8));

        writeRecord("key-utf8", "payload".getBytes(StandardCharsets.UTF_8),
                Instant.parse("2024-10-01T09:00:00Z"), 0L, headers);

        List<CassetteRecord.Header> returned = fetchFirstRecordHeaders();

        assertThat(returned).hasSize(2);
        assertThat(returned.get(0).key()).isEqualTo("content-type");
        assertThat(returned.get(0).value()).isEqualTo("application/json");
        assertThat(returned.get(1).key()).isEqualTo("x-trace-id");
        assertThat(returned.get(1).value()).isEqualTo("abc-123");
    }

    @Test
    void headers_binaryNonUtf8ValueStoredAsBase64AndRoundTrips() throws Exception {
        // [0xFF, 0xFE, 0x00] is not valid UTF-8 (0xFF is illegal in UTF-8).
        // CassetteBatchWriter.decodeHeaderValue() falls back to Base64.getEncoder()
        // (standard Base64, not URL-safe) for such payloads.
        byte[] binaryPayload = {(byte) 0xFF, (byte) 0xFE, (byte) 0x00};

        RecordHeaders headers = new RecordHeaders();
        headers.add("x-binary", binaryPayload);

        writeRecord("key-binary", null,
                Instant.parse("2024-10-01T10:00:00Z"), 1L, headers);

        List<CassetteRecord.Header> returned = fetchFirstRecordHeaders();

        assertThat(returned).hasSize(1);
        CassetteRecord.Header binaryHeader = returned.get(0);
        assertThat(binaryHeader.key()).isEqualTo("x-binary");

        // The stored value is the standard (non-URL-safe) Base64 encoding
        // of the raw bytes, because CassetteBatchWriter uses Base64.getEncoder().
        String expectedBase64 = Base64.getEncoder().encodeToString(binaryPayload);
        assertThat(binaryHeader.value())
                .as("non-UTF-8 header value must be stored and returned as standard Base64")
                .isEqualTo(expectedBase64);

        // Verify round-trip: decoding the returned string gives back the original bytes.
        assertThat(Base64.getDecoder().decode(binaryHeader.value()))
                .as("Base64-decoded header value must equal the original binary payload")
                .isEqualTo(binaryPayload);
    }

    @Test
    void headers_duplicateKeysAllPreservedInInsertionOrder() throws Exception {
        // Kafka allows duplicate header keys; both entries must survive in order.
        RecordHeaders headers = new RecordHeaders();
        headers.add("content-type", "application/json".getBytes(StandardCharsets.UTF_8));
        headers.add("content-type", "text/plain".getBytes(StandardCharsets.UTF_8));

        writeRecord("key-dup", "body".getBytes(StandardCharsets.UTF_8),
                Instant.parse("2024-10-01T11:00:00Z"), 2L, headers);

        List<CassetteRecord.Header> returned = fetchFirstRecordHeaders();

        assertThat(returned).hasSize(2);
        assertThat(returned.stream().map(CassetteRecord.Header::key).toList())
                .as("both duplicate-key header entries must be present in insertion order")
                .containsExactly("content-type", "content-type");
        assertThat(returned.stream().map(CassetteRecord.Header::value).toList())
                .containsExactly("application/json", "text/plain");
    }

    @Test
    void headers_emptyHeaderListReturnedForMessageWithNoHeaders() throws Exception {
        // ConsumerRecord with no headers — the stored headers array should be empty.
        writeRecord("key-no-headers", "v".getBytes(StandardCharsets.UTF_8),
                Instant.parse("2024-10-01T12:00:00Z"), 3L, new RecordHeaders());

        List<CassetteRecord.Header> returned = fetchFirstRecordHeaders();

        // TopicReplayService.mapHeaders returns an empty list (not null) for [].
        assertThat(returned).isNotNull().isEmpty();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Writes a single {@link ConsumerRecord} to the general cassette table via
     * {@link CassetteBatchWriter} — the real production write path.
     *
     * <p>{@code CassetteBatchWriter} duplicates the shared DuckDB connection
     * internally, so writes are visible to all other connections (and to the
     * REST API) as soon as the writer is closed.
     */
    private void writeRecord(String key, byte[] value, Instant timestamp,
                             long offset, RecordHeaders headers) throws Exception {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                TOPIC, 0, offset, timestamp.toEpochMilli(), TimestampType.CREATE_TIME,
                -1, -1, key, value, headers, Optional.empty());
        try (CassetteBatchWriter writer = new CassetteBatchWriter(TOPIC, duckDB)) {
            writer.writeBatch(List.of(record));
        }
    }

    /**
     * Calls {@code GET /cassettes/topics/{topic}} and returns the header list
     * of the first (and in each test the only) record in the response.
     */
    private List<CassetteRecord.Header> fetchFirstRecordHeaders() {
        ResponseEntity<PagedResponse<CassetteRecord>> response = restTemplate.exchange(
                "http://localhost:" + port + "/cassettes/topics/" + TOPIC,
                HttpMethod.GET, null, new ParameterizedTypeReference<>() {});
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        List<CassetteRecord> data = response.getBody().data();
        assertThat(data).as("cassette must contain at least one record").isNotEmpty();
        return data.get(0).headers();
    }
}

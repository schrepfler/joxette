package com.joxette.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: verifies that {@code timestamp} and {@code recordedAt} TIMESTAMPTZ
 * fields are serialised as ISO-8601 strings with a timezone designator across all three
 * response formats — {@code application/json}, {@code text/event-stream}, and
 * {@code application/x-ndjson} — for both the general (topic) and entity cassette paths.
 *
 * <h2>DuckDB 1.5.3 context</h2>
 * <p>DuckDB 1.5.3 fixed a bug where TIMESTAMPFORMAT was ignored for TIMESTAMPTZ columns
 * in JSON exports. Although Joxette does not use DuckDB's own JSON export (timestamps are
 * read via jOOQ as {@link java.time.OffsetDateTime} and then serialised by Jackson), this
 * regression suite guards the full pipeline against any future JDBC-layer timestamp
 * representation regressions — e.g. returning an epoch number instead of an ISO-8601
 * string, or returning a local-time string without a timezone designator.
 *
 * <h2>Strategy</h2>
 * <p>A single row with a precisely known {@link Instant} (millisecond precision, UTC) is
 * inserted directly into DuckDB for both the general cassette and the entity cassette.
 * Each test then exercises one response-format path, extracts the raw JSON text of the
 * {@code timestamp} and {@code recordedAt} fields, and asserts:
 * <ol>
 *   <li>The field value matches ISO-8601 combined date-time with a timezone designator
 *       ({@code Z} or {@code ±HH:MM}).</li>
 *   <li>The field value round-trips back to the exact {@link Instant} that was stored,
 *       confirming no epoch-seconds drift or timezone-offset error.</li>
 * </ol>
 *
 * <p>Kafka is not used by these tests; the container is required only for the Spring
 * context to start (the recording coordinator checks bootstrap-servers at boot).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class TimestampSerializationIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("joxette.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @LocalServerPort
    private int port;

    @Autowired
    private Connection duckDB;

    @Autowired
    private ObjectMapper objectMapper;

    private String baseUrl;

    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------

    private static final String TOPIC = "ts.serialization.test";

    private static final String ENTITY_TYPE = "tstest";
    private static final String ENTITY_ID   = "entity-ts-001";

    /**
     * Known timestamp with sub-second (millisecond) precision so we can confirm
     * that the fractional component survives the DuckDB → JDBC → Jackson round-trip.
     */
    private static final Instant KNOWN_TIMESTAMP   = Instant.parse("2025-01-15T10:30:00.123Z");

    /**
     * A distinct instant for {@code recorded_at} so we can tell the two fields apart.
     */
    private static final Instant KNOWN_RECORDED_AT = Instant.parse("2025-01-15T10:30:00.456Z");

    /**
     * ISO-8601 combined date-time with a mandatory timezone designator.
     * Accepts {@code "Z"} and {@code "±HH:MM"} suffixes; optional sub-second fraction.
     * Must NOT match a plain epoch number or a date-time without timezone.
     */
    private static final Pattern ISO8601_WITH_TZ = Pattern.compile(
            "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?(Z|[+-]\\d{2}:\\d{2})");

    // -----------------------------------------------------------------------
    // Test fixture setup
    // -----------------------------------------------------------------------

    @BeforeEach
    void setUp() throws Exception {
        baseUrl = "http://localhost:" + port;

        // ---- General cassette ----
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC);
        try (Statement st = duckDB.createStatement()) {
            // normalized: ts_serialization_test
            st.execute("DELETE FROM lake.main.general_ts_serialization_test");
        }
        DuckDBTestSupport.insertCassetteRow(
                duckDB, TOPIC, 0, 0L,
                KNOWN_TIMESTAMP, KNOWN_RECORDED_AT,
                "ts-key", "{\"event\":\"ts-probe\"}".getBytes(StandardCharsets.UTF_8));

        // ---- Entity cassette ----
        DuckDBTestSupport.createEntityTable(duckDB, ENTITY_TYPE);
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.main.entity_" + ENTITY_TYPE);
        }
        DuckDBTestSupport.insertEntityRow(
                duckDB, ENTITY_TYPE, ENTITY_ID, 0, null,
                TOPIC, 0, 0L,
                KNOWN_TIMESTAMP, KNOWN_RECORDED_AT,
                "ts-key", "{\"event\":\"ts-probe\"}".getBytes(StandardCharsets.UTF_8));
    }

    // =========================================================================
    // General cassette – topic replay
    // =========================================================================

    @Test
    @DisplayName("topic JSON: timestamp and recordedAt are ISO-8601 with timezone")
    void topicReplay_json_timestampFieldsAreIso8601WithTimezone() throws Exception {
        String raw = new RestTemplate()
                .getForObject(baseUrl + "/cassettes/topics/" + TOPIC, String.class);

        assertThat(raw).as("raw JSON body").isNotNull();
        JsonNode record = objectMapper.readTree(raw).path("data").get(0);
        assertTimestampFields(record, "topic JSON");
    }

    @Test
    @DisplayName("topic SSE: timestamp and recordedAt are ISO-8601 with timezone")
    void topicReplay_sse_timestampFieldsAreIso8601WithTimezone() throws Exception {
        String body = fetchBody(
                baseUrl + "/cassettes/topics/" + TOPIC, "text/event-stream");

        JsonNode record = extractFirstRecord(body, Format.SSE, "topic SSE");
        assertTimestampFields(record, "topic SSE");
    }

    @Test
    @DisplayName("topic NDJSON: timestamp and recordedAt are ISO-8601 with timezone")
    void topicReplay_ndjson_timestampFieldsAreIso8601WithTimezone() throws Exception {
        String body = fetchBody(
                baseUrl + "/cassettes/topics/" + TOPIC, "application/x-ndjson");

        JsonNode record = extractFirstRecord(body, Format.NDJSON, "topic NDJSON");
        assertTimestampFields(record, "topic NDJSON");
    }

    // =========================================================================
    // Entity cassette – entity replay
    // =========================================================================

    @Test
    @DisplayName("entity JSON: timestamp and recordedAt are ISO-8601 with timezone")
    void entityReplay_json_timestampFieldsAreIso8601WithTimezone() throws Exception {
        String url = baseUrl + "/cassettes/entities/" + ENTITY_TYPE + "/" + ENTITY_ID;
        String raw = new RestTemplate().getForObject(url, String.class);

        assertThat(raw).as("raw JSON body").isNotNull();
        JsonNode record = objectMapper.readTree(raw).path("data").get(0);
        assertTimestampFields(record, "entity JSON");
    }

    @Test
    @DisplayName("entity SSE: timestamp and recordedAt are ISO-8601 with timezone")
    void entityReplay_sse_timestampFieldsAreIso8601WithTimezone() throws Exception {
        String url = baseUrl + "/cassettes/entities/" + ENTITY_TYPE + "/" + ENTITY_ID;
        String body = fetchBody(url, "text/event-stream");

        JsonNode record = extractFirstRecord(body, Format.SSE, "entity SSE");
        assertTimestampFields(record, "entity SSE");
    }

    @Test
    @DisplayName("entity NDJSON: timestamp and recordedAt are ISO-8601 with timezone")
    void entityReplay_ndjson_timestampFieldsAreIso8601WithTimezone() throws Exception {
        String url = baseUrl + "/cassettes/entities/" + ENTITY_TYPE + "/" + ENTITY_ID;
        String body = fetchBody(url, "application/x-ndjson");

        JsonNode record = extractFirstRecord(body, Format.NDJSON, "entity NDJSON");
        assertTimestampFields(record, "entity NDJSON");
    }

    // =========================================================================
    // Shared assertions
    // =========================================================================

    /**
     * Verifies {@code timestamp} and {@code recordedAt} fields on a JSON record node:
     * <ol>
     *   <li>Present and non-empty</li>
     *   <li>Match ISO-8601 with a timezone designator (not a raw epoch number)</li>
     *   <li>Round-trip to the exact {@link Instant} that was inserted</li>
     * </ol>
     */
    private void assertTimestampFields(JsonNode record, String context) {
        assertThat(record)
                .as("[%s] first data record must not be missing/null", context)
                .isNotNull();
        assertThat(record.isMissingNode())
                .as("[%s] record must not be a missing node", context)
                .isFalse();

        String timestamp  = record.path("timestamp").asText(null);
        String recordedAt = record.path("recordedAt").asText(null);

        // 1. Fields must be present
        assertThat(timestamp)
                .as("[%s] 'timestamp' must be present and non-empty", context)
                .isNotNull().isNotEmpty();
        assertThat(recordedAt)
                .as("[%s] 'recordedAt' must be present and non-empty", context)
                .isNotNull().isNotEmpty();

        // 2. Must be ISO-8601 with timezone designator (not an epoch number)
        assertThat(timestamp)
                .as("[%s] 'timestamp' must match ISO-8601 with timezone, got: <%s>", context, timestamp)
                .matches(ISO8601_WITH_TZ.pattern());
        assertThat(recordedAt)
                .as("[%s] 'recordedAt' must match ISO-8601 with timezone, got: <%s>", context, recordedAt)
                .matches(ISO8601_WITH_TZ.pattern());

        // 3. Round-trip fidelity — exact Instant equality, including sub-second precision
        assertThat(Instant.parse(timestamp))
                .as("[%s] 'timestamp' must round-trip to KNOWN_TIMESTAMP (%s)", context, KNOWN_TIMESTAMP)
                .isEqualTo(KNOWN_TIMESTAMP);
        assertThat(Instant.parse(recordedAt))
                .as("[%s] 'recordedAt' must round-trip to KNOWN_RECORDED_AT (%s)", context, KNOWN_RECORDED_AT)
                .isEqualTo(KNOWN_RECORDED_AT);
    }

    // =========================================================================
    // Response-format parsers
    // =========================================================================

    private enum Format { SSE, NDJSON }

    /**
     * Scans the raw response body for the first line that parses as a JSON object
     * containing a {@code "timestamp"} field, which positively identifies a cassette
     * record (as opposed to preamble/control events or error frames).
     *
     * <ul>
     *   <li><b>SSE</b> — lines are prefixed with {@code "data:"} (the SSE specification
     *       allows both {@code "data:"} and {@code "data: "}; Spring omits the space).
     *       The prefix is stripped before parsing.</li>
     *   <li><b>NDJSON</b> — each line is a bare JSON object.</li>
     * </ul>
     */
    private JsonNode extractFirstRecord(String body, Format format, String context) {
        return Arrays.stream(body.split("\n"))
                .map(String::trim)
                .filter(l -> switch (format) {
                    // SSE spec: field name "data" followed by ":" and optional single space
                    case SSE    -> l.startsWith("data:");
                    case NDJSON -> l.startsWith("{");
                })
                .map(l -> switch (format) {
                    // Strip "data:" prefix (5 chars); also trim any leading space Spring may add
                    case SSE    -> l.substring("data:".length()).trim();
                    case NDJSON -> l;
                })
                .filter(json -> {
                    try {
                        return objectMapper.readTree(json).has("timestamp");
                    } catch (Exception e) {
                        return false;
                    }
                })
                .map(json -> {
                    try {
                        return objectMapper.readTree(json);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to parse JSON: " + json, e);
                    }
                })
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "[" + context + "] No cassette record found in response body:\n" + body));
    }

    // =========================================================================
    // HTTP helper
    // =========================================================================

    /**
     * Makes a GET request with the given {@code Accept} header and returns the full
     * response body as a UTF-8 string.
     *
     * <p>Uses {@link java.net.http.HttpClient} rather than {@link RestTemplate} so
     * that streaming response bodies ({@code text/event-stream},
     * {@code application/x-ndjson}) are read completely before returning, regardless
     * of Spring's content-negotiation for those media types.
     *
     * <p>The 30-second request timeout is sufficient for a single-record stream
     * and avoids hanging the test suite if the server fails to close the connection.
     */
    private static String fetchBody(String url, String acceptHeader) throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", acceptHeader)
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                .body();
    }
}

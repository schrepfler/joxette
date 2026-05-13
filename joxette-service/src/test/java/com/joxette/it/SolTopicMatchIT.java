package com.joxette.it;

import com.joxette.replay.CassetteController.SolMatchResponse;
import com.joxette.replay.SolMatchService.TagSpan;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@code POST /cassettes/topics/{topic}/sol-match}.
 *
 * <h2>Scenarios</h2>
 * <ul>
 *   <li>Topic records with {@code message_type} set — match by event name directly.</li>
 *   <li>Topic records with {@code message_type = NULL} and {@code typeField} specified —
 *       event name resolved from the JSON value payload.</li>
 *   <li>{@code typeField} not set + null message_type — events named after topic, no match.</li>
 * </ul>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
class SolTopicMatchIT {

    private static final String TOPIC = "sol-topic-it";

    private static final Instant T0 = Instant.parse("2025-06-01T10:00:00Z");
    private static final Instant T1 = T0.plusSeconds(60);
    private static final Instant T2 = T0.plusSeconds(120);
    private static final Instant T3 = T0.plusSeconds(180);

    @LocalServerPort int port;
    @Autowired Connection duckDB;

    private final RestTemplate rest = new RestTemplate();

    @BeforeEach
    void seed() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("""
                    INSERT INTO topic_configs
                        (topic, mode, created_at, updated_at)
                    VALUES ('%s', 'general', now(), now())
                    ON CONFLICT (topic) DO NOTHING
                    """.formatted(TOPIC));
        }
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC);
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.main.general_sol_topic_it");
        }

        // 4 events — message_type is NULL; event type lives in the JSON value as "type" field
        insertRow(0, T0, json("login",    "user-1"));
        insertRow(1, T1, json("browse",   "user-1"));
        insertRow(2, T2, json("purchase", "user-1"));
        insertRow(3, T3, json("logout",   "user-1"));
    }

    // -------------------------------------------------------------------------
    // typeField resolves event names from value JSON
    // -------------------------------------------------------------------------

    @Test
    void withTypeField_matchesByExtractedEventName() {
        // message_type is null; typeField="type" extracts "login","browse","purchase","logout"
        SolMatchResponse result = post(
                "match A(login) >> * >> B(purchase)",
                "type");

        assertThat(result.matched()).isTrue();
        assertThat(result.records()).hasSize(4);
    }

    @Test
    void withTypeField_noMatch_whenEventAbsent() {
        SolMatchResponse result = post(
                "match A(login) >> * >> B(checkout)",  // "checkout" not in sequence
                "type");

        assertThat(result.matched()).isFalse();
    }

    @Test
    void withTypeField_durationConstraint() {
        // login(T0) → purchase(T2) = 120 s < 5 min → matched
        SolMatchResponse result = post(
                "match A(login) >> * >> B(purchase)\nif duration(A, B) < 5min",
                "type");

        assertThat(result.matched()).isTrue();
    }

    // -------------------------------------------------------------------------
    // Without typeField — null message_type falls back to topic name
    // -------------------------------------------------------------------------

    @Test
    void withoutTypeField_nullMessageType_noEventNameMatch() {
        // Events are named after the topic string — "match login" won't match
        SolMatchResponse result = post(
                "match A(login) >> * >> B(purchase)",
                null);

        assertThat(result.matched()).isFalse();
    }

    // -------------------------------------------------------------------------
    // message_type set explicitly — no typeField needed
    // -------------------------------------------------------------------------

    @Test
    void withMessageTypeSet_matchesWithoutTypeField() throws Exception {
        // Insert additional rows with explicit message_type
        String topic2 = "sol-topic-it-typed";
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, topic2);
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.main.general_sol_topic_it_typed");
        }
        DuckDBTestSupport.insertCassetteRow(duckDB, topic2, 0, 0,
                T0, T0, null, json("ignored", "u"), "login");
        DuckDBTestSupport.insertCassetteRow(duckDB, topic2, 0, 1,
                T1, T1, null, json("ignored", "u"), "purchase");

        SolMatchResponse result = postTo(topic2,
                "match A(login) >> * >> B(purchase)", null);

        assertThat(result.matched()).isTrue();
    }

    // -------------------------------------------------------------------------
    // Record-level messageType after typeField extraction
    // -------------------------------------------------------------------------

    @Test
    void withTypeField_recordsCarryExtractedEventNames() {
        // typeField="type" should surface the extracted names as messageType on each record
        SolMatchResponse result = post(
                "match A(login) >> * >> B(purchase)",
                "type");

        assertThat(result.matched()).isTrue();
        assertThat(result.records()).hasSize(4);
        assertThat(result.records().stream().map(r -> r.messageType()).toList())
                .containsExactly("login", "browse", "purchase", "logout");
    }

    // -------------------------------------------------------------------------
    // Tag span assertions for typeField path
    // -------------------------------------------------------------------------

    @Test
    void withTypeField_tagSpansAreCorrect() {
        // Sequence (indices):  login(0)  browse(1)  purchase(2)  logout(3)
        // Query: match A(login) >> * >> B(purchase)
        // Expected tags:
        //   SEQ     [0, 4)  — full sequence
        //   MATCHED [0, 3)  — login through purchase inclusive
        //   A       [0, 1)  — login only
        //   B       [2, 3)  — purchase only
        //   SUFFIX  [3, 4)  — logout
        SolMatchResponse result = post(
                "match A(login) >> * >> B(purchase)",
                "type");

        assertThat(result.matched()).isTrue();

        Map<String, TagSpan> tags = result.tags();

        assertThat(tags).containsKey("SEQ");
        TagSpan seq = tags.get("SEQ");
        assertThat(seq.from()).isEqualTo(0);
        assertThat(seq.to()).isEqualTo(4);

        assertThat(tags).containsKey("MATCHED");
        TagSpan matched = tags.get("MATCHED");
        assertThat(matched.from()).isEqualTo(0);
        assertThat(matched.to()).isEqualTo(3);

        assertThat(tags).containsKey("A");
        assertThat(tags.get("A").from()).isEqualTo(0);
        assertThat(tags.get("A").to()).isEqualTo(1);

        assertThat(tags).containsKey("B");
        assertThat(tags.get("B").from()).isEqualTo(2);
        assertThat(tags.get("B").to()).isEqualTo(3);

        assertThat(tags).containsKey("SUFFIX");
        TagSpan suffix = tags.get("SUFFIX");
        assertThat(suffix.from()).isEqualTo(3);
        assertThat(suffix.to()).isEqualTo(4);

        assertThat(result.sequenceLength()).isEqualTo(4);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private SolMatchResponse post(String query, String typeField) {
        return postTo(TOPIC, query, typeField);
    }

    private SolMatchResponse postTo(String topic, String query, String typeField) {
        String tfPart = typeField != null
                ? ", \"typeField\": \"" + typeField + "\""
                : "";
        String body = "{\"query\": " + jsonString(query) + tfPart + "}";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<SolMatchResponse> resp = rest.postForEntity(
                "http://localhost:" + port + "/cassettes/topics/" + topic + "/sol-match",
                new HttpEntity<>(body, headers),
                SolMatchResponse.class);
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        return resp.getBody();
    }

    private void insertRow(long offset, Instant ts, byte[] value) throws Exception {
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, offset, ts, ts,
                null, value, null);  // message_type intentionally null
    }

    private static byte[] json(String type, String userId) {
        return ("{\"type\":\"" + type + "\",\"userId\":\"" + userId + "\"}")
                .getBytes(StandardCharsets.UTF_8);
    }

    private static String jsonString(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }
}

package com.joxette.it;

import com.joxette.replay.CassetteController.SolMatchResponse;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import java.util.Base64;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@code POST /cassettes/entities/{type}/{id}/sol-match}.
 *
 * <h2>Scenario</h2>
 * <ol>
 *   <li>Seed 4 entity events for {@code order / order-1} directly into
 *       {@code lake.main.entity_order}: {@code login → browse → purchase → logout}.</li>
 *   <li>POST various SOL queries and assert the returned event set and {@code matched} flag.</li>
 * </ol>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
class SolMatchIT {

    private static final String ENTITY_TYPE = "order";
    private static final String ENTITY_ID   = "order-sol-1";

    // Timestamps spaced 60 s apart so duration constraints can be tested
    private static final Instant T0 = Instant.parse("2025-01-01T10:00:00Z");
    private static final Instant T1 = T0.plusSeconds(60);
    private static final Instant T2 = T0.plusSeconds(120);
    private static final Instant T3 = T0.plusSeconds(180);

    @LocalServerPort
    private int port;

    @Autowired
    private Connection duckDB;

    private final RestTemplate rest = new RestTemplate();

    // -------------------------------------------------------------------------
    // Test data — seeded once per class via the shared Spring context
    // -------------------------------------------------------------------------

    @org.junit.jupiter.api.BeforeEach
    void seed() throws Exception {
        // Ensure the entity type is registered and the table exists.
        try (Statement st = duckDB.createStatement()) {
            st.execute("""
                    INSERT INTO entity_type_configs (entity_type, bucket_count, created_at)
                    VALUES ('order', 256, now())
                    ON CONFLICT (entity_type) DO NOTHING
                    """);
        }
        DuckDBTestSupport.createEntityTable(duckDB, ENTITY_TYPE);
        // Wipe any rows from a previous test run (table now guaranteed to exist).
        try (Statement st = duckDB.createStatement()) {
            st.execute("DELETE FROM lake.main.entity_order WHERE entity_id = '" + ENTITY_ID + "'");
        }

        // Insert 4 events: login, browse, purchase, logout
        insertEvent("login",    T0, 1);
        insertEvent("browse",   T1, 2);
        insertEvent("purchase", T2, 3);
        insertEvent("logout",   T3, 4);
    }

    // -------------------------------------------------------------------------
    // Parameterised test cases
    // -------------------------------------------------------------------------

    static Stream<Arguments> solMatchCases() {
        return Stream.of(

            Arguments.of(
                "simple event match",
                "match purchase",
                true,
                4  // full sequence preserved; MATCHED tag covers the purchase
            ),

            Arguments.of(
                "match with wildcard — login then purchase",
                "match Login(login) >> * >> Buy(purchase)",
                true,
                4
            ),

            Arguments.of(
                "filter MATCHED keeps matched sequence",
                "match Buy(purchase)\nfilter MATCHED",
                true,
                4
            ),

            Arguments.of(
                "filter not MATCHED — no purchase event → unmatched",
                "match NoSuchEvent(nonexistent)\nfilter not MATCHED",
                false,
                4  // unmatched sequence still returned (filter not MATCHED keeps it)
            ),

            Arguments.of(
                "duration constraint satisfied",
                // login→purchase span = 120 s < 5 min
                "match A(login) >> * >> B(purchase)\nif duration(A, B) < 5min",
                true,
                4
            ),

            Arguments.of(
                "duration constraint violated → no match",
                // login→purchase span = 120 s, require < 1 min
                "match A(login) >> * >> B(purchase)\nif duration(A, B) < 1min",
                false,
                4
            )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("solMatchCases")
    void solMatchScenarios(String label, String query, boolean expectMatched, int expectRecords)
            throws Exception {

        String body = """
                {"query": %s}
                """.formatted(jsonString(query));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<SolMatchResponse> response = rest.postForEntity(
                url("/cassettes/entities/" + ENTITY_TYPE + "/" + ENTITY_ID + "/sol-match"),
                new HttpEntity<>(body, headers),
                SolMatchResponse.class);

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        SolMatchResponse result = response.getBody();
        assertThat(result).isNotNull();
        assertThat(result.matched()).as(label + ": matched").isEqualTo(expectMatched);
        assertThat(result.records()).as(label + ": record count").hasSize(expectRecords);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void insertEvent(String messageType, Instant ts, long offset) throws Exception {
        String value = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(("{\"event\":\"" + messageType + "\"}").getBytes(StandardCharsets.UTF_8));
        DuckDBTestSupport.insertEntityRow(
                duckDB,
                ENTITY_TYPE, ENTITY_ID, 0, messageType,
                "orders.events", 0, offset,
                ts, ts,
                ENTITY_ID, ("{\"event\":\"" + messageType + "\"}").getBytes(StandardCharsets.UTF_8));
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private static String jsonString(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }
}

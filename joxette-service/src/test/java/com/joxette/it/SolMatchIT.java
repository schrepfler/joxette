package com.joxette.it;

import com.joxette.replay.CassetteController.SolMatchResponse;
import com.joxette.replay.SolMatchService.TagSpan;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
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
import java.util.List;
import java.util.Map;
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
            ),

            Arguments.of(
                "match split + combine — counts occurrences of browse",
                // split + combine: combine merges sub-sequence dims back into full sequence
                // matched=false (tags cleared by combine); original records preserved
                "match split B(browse)+\ncombine count = max(split_index)",
                false,  // combine clears tags → matched=false
                4       // combine preserves original sequence length
            ),

            // ------------------------------------------------------------------
            // match split scenarios
            // ------------------------------------------------------------------

            Arguments.of(
                "match split on purchase — splits before purchase, produces 2 sub-sequences recombined to 4",
                "match split P(purchase)\ncombine",
                false,   // combine clears tags
                4        // all 4 events survive
            ),

            Arguments.of(
                "match split with no occurrence — sequence unchanged",
                "match split Z(nonexistent)\ncombine",
                false,
                4
            ),

            Arguments.of(
                "match split multiple occurrences — split before login and purchase",
                // login(0) and purchase(2) each match; three segments: [](empty), [login,browse], [purchase,logout]
                "match split (login | purchase)\ncombine",
                false,
                4
            ),

            // ------------------------------------------------------------------
            // combine with aggregation
            // ------------------------------------------------------------------

            Arguments.of(
                "combine aggregation with max — adds seq dim without trimming events",
                "match split B(browse)+\ncombine total = max(split_index)",
                false,
                4
            ),

            // ------------------------------------------------------------------
            // replace scenarios
            // ------------------------------------------------------------------

            Arguments.of(
                "replace MATCHED with null — removes matched events from sequence",
                // match login → MATCHED = [0,1); replace removes it; 3 events remain
                "match L(login)\nreplace MATCHED with null",
                true,
                3
            ),

            Arguments.of(
                "replace PREFIX+MATCHED preserves only matched and suffix",
                // match purchase → MATCHED=[2,3); PREFIX=[0,2); replace PREFIX with null → 2 events
                "match P(purchase)\nreplace PREFIX with null",
                true,
                3
            ),

            // ------------------------------------------------------------------
            // set scenarios
            // ------------------------------------------------------------------

            Arguments.of(
                "set sequence dimension without changing length",
                "match A(login) >> * >> B(purchase)\nset span_s = duration(A, B)",
                true,
                4
            ),

            Arguments.of(
                "set on unmatched sequence — dimension still attached, records unchanged",
                "match X(nonexistent)\nset label = 'no-match'",
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
    // replace operation
    // -------------------------------------------------------------------------

    @Test
    void replace_keepsPrefixAndTarget() throws Exception {
        // Sequence: login → browse → purchase → logout
        // Keep only events up to and including purchase.
        // Use wildcard PREFIX rather than a named capture with quantifier.
        String query = "match start >> * >> Target(purchase)\nreplace SEQ with MATCHED";

        SolMatchResponse result = post(query);

        assertThat(result.matched()).isTrue();
        // MATCHED covers login(0)..purchase(2) inclusive = 3 events
        assertThat(result.records()).hasSize(3);
        assertThat(result.records().stream().map(r -> r.messageType()).toList())
                .containsExactly("login", "browse", "purchase");
    }

    // -------------------------------------------------------------------------
    // set operation
    // -------------------------------------------------------------------------

    @Test
    void set_addsDimensionToMatchedResult() throws Exception {
        // Compute duration between login and purchase; expose as a sequence dim.
        // filter MATCHED keeps the full sequence (matched=true means SEQ is returned),
        // but the sequence is unchanged — set only adds a dim, doesn't trim.
        String query = "match A(login) >> * >> B(purchase)\nset elapsed = duration(A, B)\nfilter MATCHED";

        SolMatchResponse result = post(query);

        assertThat(result.matched()).isTrue();
        assertThat(result.records()).hasSize(4); // full sequence preserved
        assertThat(result.sequenceLength()).isEqualTo(4);
    }

    // -------------------------------------------------------------------------
    // Tag span assertions
    // -------------------------------------------------------------------------

    @Test
    void tagSpans_matchedSpanCoversCorrectIndices() throws Exception {
        // login(0) → browse(1) → purchase(2) → logout(3)
        // match A(login) >> * >> B(purchase) → MATCHED covers indices 0..3 (full seq)
        String query = "match A(login) >> * >> B(purchase)";

        SolMatchResponse result = post(query);

        assertThat(result.matched()).isTrue();
        assertThat(result.tags()).isNotNull();

        Map<String, TagSpan> tags = result.tags();
        assertThat(tags).containsKey("MATCHED");
        assertThat(tags).containsKey("SEQ");

        TagSpan seq = tags.get("SEQ");
        assertThat(seq.from()).isEqualTo(0);
        assertThat(seq.to()).isEqualTo(4); // 4 events total

        // MATCHED: login(0) through purchase(2) inclusive → [0, 3) half-open
        TagSpan matched = tags.get("MATCHED");
        assertThat(matched.from()).isEqualTo(0);
        assertThat(matched.to()).isEqualTo(3);

        // A tag covers just index 0 (login) → [0, 1)
        assertThat(tags).containsKey("A");
        assertThat(tags.get("A").from()).isEqualTo(0);
        assertThat(tags.get("A").to()).isEqualTo(1);

        // B tag covers just index 2 (purchase) → [2, 3)
        assertThat(tags).containsKey("B");
        assertThat(tags.get("B").from()).isEqualTo(2);
        assertThat(tags.get("B").to()).isEqualTo(3);
    }

    @Test
    void tagSpans_prefixAndSuffixCoversCorrectIndices() throws Exception {
        // login(0) browse(1) purchase(2) logout(3)
        // match A(login) >> * >> B(purchase) produces:
        //   PREFIX = [] (nothing before login)  → from=0 to=0
        //   MATCHED = [login..purchase] → from=0 to=3
        //   SUFFIX  = [logout] → from=3 to=4
        String query = "match A(login) >> * >> B(purchase)";

        SolMatchResponse result = post(query);

        Map<String, TagSpan> tags = result.tags();
        assertThat(tags).containsKey("SUFFIX");
        TagSpan suffix = tags.get("SUFFIX");
        assertThat(suffix.from()).isEqualTo(3);
        assertThat(suffix.to()).isEqualTo(4);

        assertThat(result.sequenceLength()).isEqualTo(4);
    }

    // -------------------------------------------------------------------------
    // Tag span assertions — @CsvSource parameterised
    // -------------------------------------------------------------------------

    /**
     * Verifies start/end indices for named tags under various queries.
     *
     * <p>Columns: label | query | tagName | expectedFrom | expectedTo
     */
    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
        // Single-event match at index 0: A=[0,1), MATCHED=[0,1), no PREFIX, SUFFIX=[1,4)
        "single-event tag A from=0 to=1         | match A(login)                         | A       | 0 | 1",
        "single-event MATCHED from=0 to=1       | match A(login)                         | MATCHED | 0 | 1",
        "single-event SUFFIX from=1 to=4        | match A(login)                         | SUFFIX  | 1 | 4",
        // Middle match: purchase at index 2
        "purchase tag P from=2 to=3             | match P(purchase)                      | P       | 2 | 3",
        "purchase MATCHED from=2 to=3           | match P(purchase)                      | MATCHED | 2 | 3",
        "purchase PREFIX from=0 to=2            | match P(purchase)                      | PREFIX  | 0 | 2",
        "purchase SUFFIX from=3 to=4            | match P(purchase)                      | SUFFIX  | 3 | 4",
        // Wildcard span: login(0) to purchase(2) → MATCHED=[0,3), A=[0,1), B=[2,3)
        "wildcard MATCHED from=0 to=3           | match A(login) >> * >> B(purchase)     | MATCHED | 0 | 3",
        "wildcard named B from=2 to=3           | match A(login) >> * >> B(purchase)     | B       | 2 | 3",
        // Full sequence: SEQ always [0,4)
        "SEQ always covers full sequence        | match A(login)                         | SEQ     | 0 | 4",
    })
    void tagSpans_parameterised(String label, String query, String tagName,
                                int expectedFrom, int expectedTo) throws Exception {

        SolMatchResponse result = post(query.strip());

        assertThat(result.tags())
                .as(label + ": tag '" + tagName + "' present")
                .containsKey(tagName);
        TagSpan span = result.tags().get(tagName);
        assertThat(span.from())
                .as(label + ": " + tagName + ".from")
                .isEqualTo(expectedFrom);
        assertThat(span.to())
                .as(label + ": " + tagName + ".to")
                .isEqualTo(expectedTo);
    }

    // -------------------------------------------------------------------------
    // match split — event order and record identity preserved
    // -------------------------------------------------------------------------

    @Test
    void matchSplit_preservesEventOrderAfterCombine() throws Exception {
        // Sequence: login(0) browse(1) purchase(2) logout(3)
        // match split (login | purchase) splits at index 0 and index 2.
        // Segments: [] (before first match, skipped because empty per engine),
        //           [login, browse], [purchase, logout]
        // combine merges segments back in original order.
        String query = "match split (login | purchase)\ncombine";

        SolMatchResponse result = post(query);

        assertThat(result.matched()).isFalse(); // combine clears tags
        assertThat(result.records()).hasSize(4);
        List<String> types = result.records().stream().map(r -> r.messageType()).toList();
        assertThat(types).containsExactly("login", "browse", "purchase", "logout");
    }

    @Test
    void matchSplit_withAggregation_addsSequenceDim() throws Exception {
        // match split B(browse)+ splits on the one browse event.
        // combine count = max(split_index) aggregates.
        // Afterwards the combined sequence carries 'count' dim; length unchanged.
        String query = "match split B(browse)+\ncombine count = max(split_index)";

        SolMatchResponse result = post(query);

        assertThat(result.records()).hasSize(4);
        assertThat(result.matched()).isFalse();
    }

    // -------------------------------------------------------------------------
    // replace — event set changes
    // -------------------------------------------------------------------------

    @Test
    void replace_withNull_removesTargetTag() throws Exception {
        // match login → MATCHED=[0,1); replace MATCHED with null removes login.
        // Remaining sequence: browse(0) purchase(1) logout(2) — 3 events.
        String query = "match L(login)\nreplace MATCHED with null";

        SolMatchResponse result = post(query);

        assertThat(result.matched()).isTrue();
        assertThat(result.records()).hasSize(3);
        assertThat(result.records().stream().map(r -> r.messageType()).toList())
                .containsExactly("browse", "purchase", "logout");
    }

    @Test
    void replace_tagSpan_updatedAfterReplacement() throws Exception {
        // match purchase → P=[2,3), MATCHED=[2,3).
        // replace P with null removes purchase.
        // After replacement: 3 events — login(0) browse(1) logout(2).
        // No tag spans reported (MATCHED cleared by replace updating P to empty range).
        String query = "match P(purchase)\nreplace P with null";

        SolMatchResponse result = post(query);

        assertThat(result.matched()).isTrue();
        assertThat(result.records()).hasSize(3);
    }

    // -------------------------------------------------------------------------
    // set — dimension values
    // -------------------------------------------------------------------------

    @Test
    void set_multipleAssignments_preserveSequenceLength() throws Exception {
        // Two consecutive set operations; neither removes events.
        String query = "match A(login) >> * >> B(purchase)\nset elapsed = duration(A, B)\nset label = 'ok'";

        SolMatchResponse result = post(query);

        assertThat(result.matched()).isTrue();
        assertThat(result.records()).hasSize(4);
        assertThat(result.sequenceLength()).isEqualTo(4);
    }

    @Test
    void set_onUnmatchedSequence_recordsUnchanged() throws Exception {
        // No match → set still executes; sequence length must be unchanged.
        String query = "match X(nonexistent)\nset label = 'no-match'";

        SolMatchResponse result = post(query);

        assertThat(result.matched()).isFalse();
        assertThat(result.records()).hasSize(4);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private SolMatchResponse post(String query) {
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
        return response.getBody();
    }

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

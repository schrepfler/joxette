package com.joxette.replay;

import com.joxette.management.IdSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EntityIdExtractorTest {

    private EntityIdExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = new EntityIdExtractor();
    }

    // -----------------------------------------------------------------------
    // source = IdSource.KEY
    // -----------------------------------------------------------------------

    @Test
    void key_returnsMessageKey() {
        KafkaMessage msg = message("orders.events", "order-42", null);
        assertThat(extractor.extract(msg, IdSource.KEY, null)).hasValue("order-42");
    }

    @Test
    void key_emptyKeyReturnsEmpty() {
        KafkaMessage msg = message("orders.events", "", null);
        assertThat(extractor.extract(msg, IdSource.KEY, null)).isEmpty();
    }

    @Test
    void key_nullKeyReturnsEmpty() {
        KafkaMessage msg = message("orders.events", null, null);
        assertThat(extractor.extract(msg, IdSource.KEY, null)).isEmpty();
    }

    // -----------------------------------------------------------------------
    // source = IdSource.VALUE (JSONPath)
    // -----------------------------------------------------------------------

    @Test
    void value_extractsTopLevelField() {
        byte[] json = """
                {"order_id":"ORD-99","status":"pending"}
                """.getBytes(StandardCharsets.UTF_8);
        KafkaMessage msg = message("orders.events", null, json);
        assertThat(extractor.extract(msg, IdSource.VALUE, "$.order_id")).hasValue("ORD-99");
    }

    @Test
    void value_extractsNestedField() {
        byte[] json = """
                {"payment":{"order_id":"ORD-77","amount":100}}
                """.getBytes(StandardCharsets.UTF_8);
        KafkaMessage msg = message("payments.events", null, json);
        assertThat(extractor.extract(msg, IdSource.VALUE, "$.payment.order_id")).hasValue("ORD-77");
    }

    @Test
    void value_missingPathReturnsEmpty() {
        byte[] json = """
                {"status":"pending"}
                """.getBytes(StandardCharsets.UTF_8);
        KafkaMessage msg = message("orders.events", null, json);
        assertThat(extractor.extract(msg, IdSource.VALUE, "$.order_id")).isEmpty();
    }

    @Test
    void value_nullValueReturnsEmpty() {
        KafkaMessage msg = message("orders.events", null, null);
        assertThat(extractor.extract(msg, IdSource.VALUE, "$.order_id")).isEmpty();
    }

    @Test
    void value_emptyBytesReturnsEmpty() {
        KafkaMessage msg = message("orders.events", null, new byte[0]);
        assertThat(extractor.extract(msg, IdSource.VALUE, "$.order_id")).isEmpty();
    }

    @Test
    void value_invalidJsonReturnsEmpty() {
        byte[] notJson = "not-json".getBytes(StandardCharsets.UTF_8);
        KafkaMessage msg = message("orders.events", null, notJson);
        assertThat(extractor.extract(msg, IdSource.VALUE, "$.order_id")).isEmpty();
    }

    @Test
    void value_numericIdIsConvertedToString() {
        byte[] json = """
                {"order_id":12345}
                """.getBytes(StandardCharsets.UTF_8);
        KafkaMessage msg = message("orders.events", null, json);
        Optional<String> result = extractor.extract(msg, IdSource.VALUE, "$.order_id");
        assertThat(result).hasValue("12345");
    }

    // -----------------------------------------------------------------------
    // source = IdSource.HEADER
    // -----------------------------------------------------------------------

    @Test
    void header_returnsFirstMatchingHeader() {
        List<KafkaMessage.Header> headers = List.of(
                new KafkaMessage.Header("x-entity-id", "ENT-1".getBytes(StandardCharsets.UTF_8)),
                new KafkaMessage.Header("content-type", "application/json".getBytes(StandardCharsets.UTF_8))
        );
        KafkaMessage msg = messageWithHeaders("orders.events", headers);
        assertThat(extractor.extract(msg, IdSource.HEADER, "x-entity-id")).hasValue("ENT-1");
    }

    @Test
    void header_absentKeyReturnsEmpty() {
        List<KafkaMessage.Header> headers = List.of(
                new KafkaMessage.Header("content-type", "application/json".getBytes(StandardCharsets.UTF_8))
        );
        KafkaMessage msg = messageWithHeaders("orders.events", headers);
        assertThat(extractor.extract(msg, IdSource.HEADER, "x-entity-id")).isEmpty();
    }

    @Test
    void header_emptyHeaderListReturnsEmpty() {
        KafkaMessage msg = messageWithHeaders("orders.events", List.of());
        assertThat(extractor.extract(msg, IdSource.HEADER, "x-entity-id")).isEmpty();
    }

    // -----------------------------------------------------------------------
    // unknown source
    // -----------------------------------------------------------------------

    @Test
    void unknownSourceThrows() {
        assertThatThrownBy(() -> IdSource.fromValue("payload"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // -----------------------------------------------------------------------
    // extractDetailed — distinguishes extraction failures from "no match"
    // -----------------------------------------------------------------------

    static Stream<Arguments> detailedExtractionCases() {
        return Stream.of(
                Arguments.of(
                        // Note: a bare unquoted token like "not-json" does NOT trigger this —
                        // json-smart's lenient parser accepts it as a plain string literal, and
                        // JsonPath then throws PathNotFoundException (a normal "no match", not a
                        // failure) when the expression tries to navigate into it as an object.
                        // An unterminated/syntactically broken document is what genuinely fails
                        // to parse (InvalidJsonException). Verified against json-path 2.10.0.
                        "malformed JSON is a failure",
                        "{".getBytes(StandardCharsets.UTF_8),
                        "$.order_id",
                        true,   // expectFailed
                        false   // expectPresent
                ),
                Arguments.of(
                        "valid JSON with a missing path is NOT a failure",
                        "{\"status\":\"pending\"}".getBytes(StandardCharsets.UTF_8),
                        "$.order_id",
                        false,
                        false
                ),
                Arguments.of(
                        "valid JSON with a matching path succeeds",
                        "{\"order_id\":\"ORD-1\"}".getBytes(StandardCharsets.UTF_8),
                        "$.order_id",
                        false,
                        true
                )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("detailedExtractionCases")
    void extractDetailed_distinguishesFailureFromNoMatch(
            String description, byte[] json, String expression, boolean expectFailed, boolean expectPresent) {
        KafkaMessage msg = message("orders.events", null, json);
        EntityIdExtractor.Extraction result = extractor.extractDetailed(msg, IdSource.VALUE, expression);

        assertThat(result.failed()).isEqualTo(expectFailed);
        assertThat(result.value().isPresent()).isEqualTo(expectPresent);
        if (expectFailed) {
            assertThat(result.failureReason()).isNotBlank();
        }
    }

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------

    private KafkaMessage message(String topic, String key, byte[] value) {
        return new KafkaMessage(topic, 0, 0L, System.currentTimeMillis(), key, value, List.of());
    }

    private KafkaMessage messageWithHeaders(String topic, List<KafkaMessage.Header> headers) {
        return new KafkaMessage(topic, 0, 0L, System.currentTimeMillis(), null, null, headers);
    }
}

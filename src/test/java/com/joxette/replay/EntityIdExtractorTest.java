package com.joxette.replay;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class EntityIdExtractorTest {

    private EntityIdExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = new EntityIdExtractor();
    }

    // -----------------------------------------------------------------------
    // source = "key"
    // -----------------------------------------------------------------------

    @Test
    void key_returnsMessageKey() {
        KafkaMessage msg = message("orders.events", "order-42", null);
        assertThat(extractor.extract(msg, "key", null)).hasValue("order-42");
    }

    @Test
    void key_emptyKeyReturnsEmpty() {
        KafkaMessage msg = message("orders.events", "", null);
        assertThat(extractor.extract(msg, "key", null)).isEmpty();
    }

    @Test
    void key_nullKeyReturnsEmpty() {
        KafkaMessage msg = message("orders.events", null, null);
        assertThat(extractor.extract(msg, "key", null)).isEmpty();
    }

    // -----------------------------------------------------------------------
    // source = "value" (JSONPath)
    // -----------------------------------------------------------------------

    @Test
    void value_extractsTopLevelField() {
        byte[] json = """
                {"order_id":"ORD-99","status":"pending"}
                """.getBytes(StandardCharsets.UTF_8);
        KafkaMessage msg = message("orders.events", null, json);
        assertThat(extractor.extract(msg, "value", "$.order_id")).hasValue("ORD-99");
    }

    @Test
    void value_extractsNestedField() {
        byte[] json = """
                {"payment":{"order_id":"ORD-77","amount":100}}
                """.getBytes(StandardCharsets.UTF_8);
        KafkaMessage msg = message("payments.events", null, json);
        assertThat(extractor.extract(msg, "value", "$.payment.order_id")).hasValue("ORD-77");
    }

    @Test
    void value_missingPathReturnsEmpty() {
        byte[] json = """
                {"status":"pending"}
                """.getBytes(StandardCharsets.UTF_8);
        KafkaMessage msg = message("orders.events", null, json);
        assertThat(extractor.extract(msg, "value", "$.order_id")).isEmpty();
    }

    @Test
    void value_nullValueReturnsEmpty() {
        KafkaMessage msg = message("orders.events", null, null);
        assertThat(extractor.extract(msg, "value", "$.order_id")).isEmpty();
    }

    @Test
    void value_emptyBytesReturnsEmpty() {
        KafkaMessage msg = message("orders.events", null, new byte[0]);
        assertThat(extractor.extract(msg, "value", "$.order_id")).isEmpty();
    }

    @Test
    void value_invalidJsonReturnsEmpty() {
        byte[] notJson = "not-json".getBytes(StandardCharsets.UTF_8);
        KafkaMessage msg = message("orders.events", null, notJson);
        assertThat(extractor.extract(msg, "value", "$.order_id")).isEmpty();
    }

    @Test
    void value_numericIdIsConvertedToString() {
        byte[] json = """
                {"order_id":12345}
                """.getBytes(StandardCharsets.UTF_8);
        KafkaMessage msg = message("orders.events", null, json);
        Optional<String> result = extractor.extract(msg, "value", "$.order_id");
        assertThat(result).hasValue("12345");
    }

    // -----------------------------------------------------------------------
    // source = "header"
    // -----------------------------------------------------------------------

    @Test
    void header_returnsFirstMatchingHeader() {
        List<KafkaMessage.Header> headers = List.of(
                new KafkaMessage.Header("x-entity-id", "ENT-1".getBytes(StandardCharsets.UTF_8)),
                new KafkaMessage.Header("content-type", "application/json".getBytes(StandardCharsets.UTF_8))
        );
        KafkaMessage msg = messageWithHeaders("orders.events", headers);
        assertThat(extractor.extract(msg, "header", "x-entity-id")).hasValue("ENT-1");
    }

    @Test
    void header_absentKeyReturnsEmpty() {
        List<KafkaMessage.Header> headers = List.of(
                new KafkaMessage.Header("content-type", "application/json".getBytes(StandardCharsets.UTF_8))
        );
        KafkaMessage msg = messageWithHeaders("orders.events", headers);
        assertThat(extractor.extract(msg, "header", "x-entity-id")).isEmpty();
    }

    @Test
    void header_emptyHeaderListReturnsEmpty() {
        KafkaMessage msg = messageWithHeaders("orders.events", List.of());
        assertThat(extractor.extract(msg, "header", "x-entity-id")).isEmpty();
    }

    // -----------------------------------------------------------------------
    // unknown source
    // -----------------------------------------------------------------------

    @Test
    void unknownSourceReturnsEmpty() {
        KafkaMessage msg = message("orders.events", "k", null);
        assertThat(extractor.extract(msg, "payload", "$.id")).isEmpty();
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

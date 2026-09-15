package com.joxette.replay;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NdjsonLineTest {

    private final ObjectMapper mapper = new ObjectMapper();

    record Sample(String name, int count) {}

    @Test
    void recordLineSerializesAsBareValue() {
        NdjsonLine line = new NdjsonLine.RecordLine<>(new Sample("orders", 3));
        String json = mapper.writerFor(NdjsonLine.class).writeValueAsString(line);
        assertThat(json).isEqualTo("{\"name\":\"orders\",\"count\":3}");
    }

    @Test
    void controlEventSerializesAsBareMap() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("event", "heartbeat");
        payload.put("ts", "2026-09-15T10:00:00Z");
        NdjsonLine line = new NdjsonLine.ControlEvent(payload);
        String json = mapper.writerFor(NdjsonLine.class).writeValueAsString(line);
        assertThat(json).isEqualTo("{\"event\":\"heartbeat\",\"ts\":\"2026-09-15T10:00:00Z\"}");
    }

    @Test
    void errorFrameSerializesAsBareMap() {
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("status", 500);
        inner.put("title", "Internal Server Error");
        NdjsonLine line = new NdjsonLine.ErrorFrame(Map.of("_error", inner));
        String json = mapper.writerFor(NdjsonLine.class).writeValueAsString(line);
        assertThat(json).isEqualTo("{\"_error\":{\"status\":500,\"title\":\"Internal Server Error\"}}");
    }
}

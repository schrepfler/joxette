package com.joxette.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.GlobalExceptionHandler;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.management.KafkaTopicAdmin;
import com.joxette.recording.CassetteRecordingBus;
import com.joxette.replay.sink.kafka.KafkaRecordSinkFactory;
import com.joxette.replay.transform.ReplayMetadataInjector;
import com.joxette.replay.transform.TransformPresetRepository;
import com.joxette.support.DuckDBTestSupport;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;

import java.sql.Connection;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Tests for the {@code POST /cassettes/entities/{type}/batch} endpoint.
 *
 * <p>Covers validation (empty ids, too many ids) and the NDJSON event-envelope
 * format for a real in-memory DuckDB with pre-seeded entity rows.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BatchReplayTest {

    private static final String ENTITY_TYPE = "order";

    @Mock TopicReplayService        topicService;
    @Mock SseReplayHandler          sseHandler;
    @Mock CassetteLifecycleService  lifecycle;
    @Mock KafkaRecordSinkFactory    sinkFactory;
    @Mock ScheduledReplayService    scheduledReplayService;
    @Mock ReplayMetadataInjector    metadataInjector;
    @Mock TransformPresetRepository presetRepository;
    @Mock JoxetteProperties         properties;
    @Mock SequenceMatchService      sequenceMatchService;
    @Mock FieldSuggestionsService   fieldSuggestionsService;
    @Mock CassetteRecordingBus      recordingBus;
    @Mock KafkaTopicAdmin           kafkaTopicAdmin;
    @Mock ConfigRepository          configRepository;
    @Mock ActiveReplayTracker       replayTracker;

    private Connection conn;
    private MockMvc    mvc;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() throws Exception {
        conn = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createEntityTable(conn, ENTITY_TYPE);

        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        objectMapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        EntityReplayService entityService =
                new EntityReplayService(DSL.using(conn, SQLDialect.DUCKDB));

        JoxetteProperties.Replay replay = mock(JoxetteProperties.Replay.class);
        JoxetteProperties.Replay.Follow follow = mock(JoxetteProperties.Replay.Follow.class);
        org.mockito.Mockito.when(follow.getMaxSubscriptions()).thenReturn(100);
        org.mockito.Mockito.when(follow.getHeartbeatSeconds()).thenReturn(30);
        org.mockito.Mockito.when(replay.getFollow()).thenReturn(follow);
        org.mockito.Mockito.when(properties.getReplay()).thenReturn(replay);

        SseReplayHandler realSseHandler = new SseReplayHandler(objectMapper);

        CassetteController controller = new CassetteController(
                topicService, entityService, realSseHandler, lifecycle, sinkFactory,
                scheduledReplayService, metadataInjector, presetRepository, properties,
                objectMapper, sequenceMatchService, null, null,
                fieldSuggestionsService, recordingBus, kafkaTopicAdmin, configRepository, replayTracker,
                new StateFoldService(objectMapper),
                new DiffService(objectMapper),
                new TimelineService(),
                new PortraitService());

        FormattingConversionService conversion = new FormattingConversionService();
        conversion.addConverter(String.class, Order.class, Order::parse);

        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .setConversionService(conversion)
                .build();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) conn.close();
    }

    // -------------------------------------------------------------------------
    // Validation
    // -------------------------------------------------------------------------

    @Test
    void batch_emptyIds_returns400() throws Exception {
        mvc.perform(post("/cassettes/entities/order/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"ids\":[]}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void batch_tooManyIds_returns400() throws Exception {
        List<String> ids = IntStream.range(0, 101)
                .mapToObj(i -> "order-" + i)
                .toList();
        String body = objectMapper.writeValueAsString(java.util.Map.of("ids", ids));
        mvc.perform(post("/cassettes/entities/order/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    // -------------------------------------------------------------------------
    // NDJSON envelope — events output
    // -------------------------------------------------------------------------

    @Test
    void batch_twoEntities_streamsEnvelopedEvents() throws Exception {
        Instant ts = Instant.parse("2024-06-01T10:00:00Z");
        DuckDBTestSupport.insertEntityRow(conn, ENTITY_TYPE, "ORD-1", 1, "OrderCreated",
                "orders.events", 0, 0L, ts, ts, "ORD-1", null);
        DuckDBTestSupport.insertEntityRow(conn, ENTITY_TYPE, "ORD-2", 2, "OrderPaid",
                "orders.events", 0, 1L, ts.plusSeconds(1), ts.plusSeconds(1), "ORD-2", null);

        MvcResult asyncResult = mvc.perform(post("/cassettes/entities/order/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"ids\":[\"ORD-1\",\"ORD-2\"]}"))
                .andReturn();

        String response = mvc.perform(asyncDispatch(asyncResult))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        // Each line must contain the entityId envelope
        String[] lines = response.strip().split("\n");
        org.assertj.core.api.Assertions.assertThat(lines).hasSize(2);
        org.assertj.core.api.Assertions.assertThat(lines[0]).contains("\"entityId\"").contains("ORD-1");
        org.assertj.core.api.Assertions.assertThat(lines[1]).contains("\"entityId\"").contains("ORD-2");
    }

    @Test
    void batch_emptyEntity_streamsNoLinesForThatEntity() throws Exception {
        Instant ts = Instant.parse("2024-06-01T10:00:00Z");
        DuckDBTestSupport.insertEntityRow(conn, ENTITY_TYPE, "ORD-1", 1, "OrderCreated",
                "orders.events", 0, 0L, ts, ts, "ORD-1", null);

        MvcResult asyncResult = mvc.perform(post("/cassettes/entities/order/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"ids\":[\"ORD-1\",\"ORD-GHOST\"]}"))
                .andReturn();

        String response = mvc.perform(asyncDispatch(asyncResult))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        // Only ORD-1 has events; ORD-GHOST contributes no lines
        String[] lines = response.strip().split("\n");
        org.assertj.core.api.Assertions.assertThat(lines).hasSize(1);
        org.assertj.core.api.Assertions.assertThat(lines[0]).contains("ORD-1");
    }

    // -------------------------------------------------------------------------
    // output=state per entity
    // -------------------------------------------------------------------------

    @Test
    void batch_outputState_oneStateLinePerEntity() throws Exception {
        Instant ts = Instant.parse("2024-06-01T10:00:00Z");
        byte[] val1 = Base64.encodeValue("{\"status\":\"pending\"}");
        byte[] val2 = Base64.encodeValue("{\"status\":\"paid\"}");
        DuckDBTestSupport.insertEntityRow(conn, ENTITY_TYPE, "ORD-A", 1, "OrderCreated",
                "orders.events", 0, 0L, ts, ts, "ORD-A", val1);
        DuckDBTestSupport.insertEntityRow(conn, ENTITY_TYPE, "ORD-B", 2, "OrderPaid",
                "orders.events", 0, 1L, ts, ts, "ORD-B", val2);

        MvcResult asyncResult = mvc.perform(post("/cassettes/entities/order/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"ids\":[\"ORD-A\",\"ORD-B\"],\"output\":\"state\"}"))
                .andReturn();

        String response = mvc.perform(asyncDispatch(asyncResult))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        String[] lines = response.strip().split("\n");
        org.assertj.core.api.Assertions.assertThat(lines).hasSize(2);
        org.assertj.core.api.Assertions.assertThat(lines[0]).contains("\"state\"").contains("ORD-A");
        org.assertj.core.api.Assertions.assertThat(lines[1]).contains("\"state\"").contains("ORD-B");
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private static final class Base64 {
        static byte[] encodeValue(String json) {
            return json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}

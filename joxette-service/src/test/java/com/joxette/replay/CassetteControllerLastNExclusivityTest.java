package com.joxette.replay;

import tools.jackson.databind.ObjectMapper;
import com.joxette.api.error.GlobalExceptionHandler;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.management.KafkaTopicAdmin;
import com.joxette.recording.CassetteRecordingBus;
import com.joxette.replay.sink.kafka.KafkaRecordSinkFactory;
import com.joxette.replay.transform.ReplayMetadataInjector;
import com.joxette.replay.transform.TransformPresetRepository;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CassetteControllerLastNExclusivityTest {

    @Mock TopicReplayService        topicService;
    @Mock EntityReplayService       entityService;
    @Mock SseReplayHandler          sseHandler;
    @Mock CassetteLifecycleService  lifecycle;
    @Mock KafkaRecordSinkFactory    sinkFactory;
    @Mock ScheduledReplayService    scheduledReplayService;
    @Mock ReplayMetadataInjector    metadataInjector;
    @Mock TransformPresetRepository presetRepository;
    @Mock SequenceMatchService      sequenceMatchService;
    @Mock FieldSuggestionsService   fieldSuggestionsService;
    @Mock CassetteRecordingBus      recordingBus;
    @Mock KafkaTopicAdmin           kafkaTopicAdmin;
    @Mock ConfigRepository          configRepository;
    @SuppressWarnings("unchecked")
    @Mock ActorRef<ReplayCoordinatorActor.Cmd> replayCoordinator;
    @SuppressWarnings("unchecked")
    @Mock ActorSystem<Void>         actorSystem;

    private MockMvc mvc;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        JoxetteProperties properties = new JoxetteProperties();

        CassetteController controller = new CassetteController(
                topicService, entityService, sseHandler, lifecycle, sinkFactory,
                scheduledReplayService, metadataInjector, presetRepository, properties,
                mapper, sequenceMatchService, null, null,
                fieldSuggestionsService, recordingBus, kafkaTopicAdmin, configRepository,
                replayCoordinator, actorSystem,
                Executors.newVirtualThreadPerTaskExecutor(),
                new StateFoldService(mapper),
                new DiffService(mapper),
                new TimelineService(),
                new PortraitService());

        DefaultFormattingConversionService conversion = new DefaultFormattingConversionService();
        conversion.addConverter(String.class, Order.class, Order::parse);
        conversion.addConverter(String.class, SolOutput.class, SolOutput::parse);
        conversion.addConverter(String.class, ReplayOutputMode.class, ReplayOutputMode::parse);
        conversion.addConverter(String.class, ResponseFormat.class, ResponseFormat::parse);
        conversion.addConverter(String.class, StateFoldStrategy.class, StateFoldStrategy::parse);
        conversion.addConverter(String.class, DedupPolicy.class, DedupPolicy::parse);

        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .setConversionService(conversion)
                .build();
    }

    // -------------------------------------------------------------------------
    // JSON
    // -------------------------------------------------------------------------

    @Test
    void json_lastNWithFrom_returns400() throws Exception {
        mvc.perform(get("/cassettes/entities/order/cust-1")
                .param("last_n", "5")
                .param("from", "2025-01-01T00:00:00Z"))
           .andExpect(status().isBadRequest())
           .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));
    }

    @Test
    void json_lastNWithCursor_returns400() throws Exception {
        mvc.perform(get("/cassettes/entities/order/cust-1")
                .param("last_n", "5")
                .param("cursor", "some-cursor"))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));
    }

    @Test
    void json_lastNAlone_succeeds() throws Exception {
        when(entityService.queryEntityEvents(anyString(), anyString(), any(), any(), anyInt(), any(),
                any(), anyString(), any(), any(), any(), any()))
                .thenReturn(new PagedResponse<>(List.of(), null, false, null, null));

        mvc.perform(get("/cassettes/entities/order/cust-1").param("last_n", "5"))
           .andExpect(status().isOk());
    }

    // -------------------------------------------------------------------------
    // SSE
    // -------------------------------------------------------------------------

    @Test
    void sse_lastNWithTo_returns400() throws Exception {
        mvc.perform(get("/cassettes/entities/order/cust-1")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .param("last_n", "5")
                .param("to", "2025-01-01T00:00:00Z"))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));
    }

    @Test
    void sse_lastNAlone_doesNotThrow() throws Exception {
        when(sseHandler.<EntityRecord>streamSse(any(), any(), any())).thenReturn(new SseEmitter());
        when(entityService.queryEntityEvents(anyString(), anyString(), any(), any(), anyInt(), any(),
                any(), anyString(), any(), any(), any(), any()))
                .thenReturn(new PagedResponse<>(List.of(), null, false, null, null));

        mvc.perform(get("/cassettes/entities/order/cust-1")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .param("last_n", "5"))
           .andExpect(request().asyncStarted());
    }

    // -------------------------------------------------------------------------
    // NDJSON
    // -------------------------------------------------------------------------

    @Test
    void ndjson_lastNWithFromAndTo_returns400() throws Exception {
        mvc.perform(get("/cassettes/entities/order/cust-1")
                .accept(MediaType.parseMediaType("application/x-ndjson"))
                .param("last_n", "5")
                .param("from", "2025-01-01T00:00:00Z")
                .param("to", "2025-01-02T00:00:00Z"))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));
    }

    @Test
    void ndjson_lastNAlone_doesNotThrow() throws Exception {
        when(sseHandler.<EntityRecord>streamNdjson(any(), any()))
                .thenReturn(outputStream -> {});
        when(entityService.queryEntityEvents(anyString(), anyString(), any(), any(), anyInt(), any(),
                any(), anyString(), any(), any(), any(), any()))
                .thenReturn(new PagedResponse<>(List.of(), null, false, null, null));

        mvc.perform(get("/cassettes/entities/order/cust-1")
                .accept(MediaType.parseMediaType("application/x-ndjson"))
                .param("last_n", "5"))
           .andExpect(request().asyncStarted());
    }

    // =========================================================================
    // Finding I6: the same last_n exclusivity check must also apply to the
    // POST/batch replay handlers — previously they accepted from/to/lastN (and,
    // for the JSON variant, cursor) unchecked and silently ignored the filters
    // instead of 400ing like the GET handlers.
    // =========================================================================

    @Test
    void postJson_lastNWithFrom_returns400() throws Exception {
        mvc.perform(post("/cassettes/entities/order/cust-1/replay")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"lastN\":5,\"from\":\"2025-01-01T00:00:00Z\"}"))
           .andExpect(status().isBadRequest())
           .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));

        verifyNoInteractions(entityService);
    }

    @Test
    void postJson_lastNWithCursor_returns400() throws Exception {
        mvc.perform(post("/cassettes/entities/order/cust-1/replay")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"lastN\":5,\"cursor\":\"some-cursor\"}"))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));

        verifyNoInteractions(entityService);
    }

    @Test
    void postJson_lastNAlone_succeeds() throws Exception {
        when(entityService.queryEntityEvents(anyString(), anyString(), any(), any(), anyInt(), any(),
                any(), anyString(), any(), any(), any(), any()))
                .thenReturn(new PagedResponse<>(List.of(), null, false, null, null));

        mvc.perform(post("/cassettes/entities/order/cust-1/replay")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"lastN\":5}"))
           .andExpect(status().isOk());
    }

    @Test
    void postSse_lastNWithTo_returns400() throws Exception {
        mvc.perform(post("/cassettes/entities/order/cust-1/replay")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .content("{\"lastN\":5,\"to\":\"2025-01-01T00:00:00Z\"}"))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));

        verifyNoInteractions(entityService);
        verifyNoInteractions(sseHandler);
    }

    @Test
    void postNdjson_lastNWithFromAndTo_returns400() throws Exception {
        mvc.perform(post("/cassettes/entities/order/cust-1/replay")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType("application/x-ndjson"))
                .content("{\"lastN\":5,\"from\":\"2025-01-01T00:00:00Z\",\"to\":\"2025-01-02T00:00:00Z\"}"))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));

        verifyNoInteractions(entityService);
        verifyNoInteractions(sseHandler);
    }

    @Test
    void batch_lastNWithFrom_returns400() throws Exception {
        mvc.perform(post("/cassettes/entities/order/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"ids\":[\"cust-1\"],\"lastN\":5,\"from\":\"2025-01-01T00:00:00Z\"}"))
           .andExpect(status().isBadRequest())
           .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));

        verifyNoInteractions(entityService);
    }

    @Test
    void batch_lastNWithTo_returns400() throws Exception {
        mvc.perform(post("/cassettes/entities/order/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"ids\":[\"cust-1\"],\"lastN\":5,\"to\":\"2025-01-01T00:00:00Z\"}"))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.errorCode").value("ERR_VALIDATION"));

        verifyNoInteractions(entityService);
    }

    @Test
    void batch_lastNAlone_doesNotThrow() throws Exception {
        // sseHandler is an unstubbed mock here (streamNdjson() -> null body), so unlike
        // the real-sseHandler tests in BatchReplayTest this completes synchronously with
        // an empty 200 rather than entering async dispatch — the point of this test is
        // only that a valid last_n-alone request passes validation, not the stream content.
        mvc.perform(post("/cassettes/entities/order/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"ids\":[\"cust-1\"],\"lastN\":5}"))
           .andExpect(status().isOk());
    }
}

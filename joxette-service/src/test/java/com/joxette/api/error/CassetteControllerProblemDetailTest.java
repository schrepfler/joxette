package com.joxette.api.error;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.config.JoxetteProperties;
import com.joxette.recording.CassetteRecordingBus;
import com.joxette.replay.CassetteController;
import com.joxette.replay.CassetteLifecycleService;
import com.joxette.replay.EntityReplayService;
import com.joxette.replay.FieldSuggestionsService;
import com.joxette.replay.ScheduledReplayService;
import com.joxette.replay.SequenceMatchService;
import com.joxette.replay.Order;
import com.joxette.replay.SseReplayHandler;
import com.joxette.replay.TopicReplayService;
import com.joxette.replay.sink.kafka.KafkaRecordSinkFactory;
import com.joxette.replay.transform.ReplayMetadataInjector;
import com.joxette.replay.transform.TransformPresetRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

/**
 * MockMvc coverage of the RFC 7807 ProblemDetail contract for
 * {@link CassetteController}: malformed cursor (400,
 * https://joxette.dev/problems/invalid-cursor), SQLException → 503 upstream
 * unavailable, and uncaught RuntimeException → 500 internal.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CassetteControllerProblemDetailTest {

    @Mock TopicReplayService        topicService;
    @Mock EntityReplayService       entityService;
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

    private MockMvc mvc;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        CassetteController controller = new CassetteController(
                topicService, entityService, sseHandler, lifecycle, sinkFactory,
                scheduledReplayService, metadataInjector, presetRepository, properties,
                mapper, sequenceMatchService, null /* solMatchService */, fieldSuggestionsService, recordingBus);

        // Register the Order converter so `?order=asc|desc` binding matches production.
        FormattingConversionService conversion = new FormattingConversionService();
        conversion.addConverter(String.class, Order.class, Order::parse);

        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .setConversionService(conversion)
                .build();
    }

    // =========================================================================
    // 400 — malformed cursor surfaces as InvalidCursor problem
    // =========================================================================

    @Test
    void replayTopic_malformedCursor_returnsInvalidCursorProblem() throws Exception {
        when(topicService.query(
                anyString(), any(), any(), any(), any(), any(), anyInt(), anyString(), any(), anyString(), any()))
                .thenThrow(InvalidCursorException.malformed(new RuntimeException("bad base64")));

        mvc.perform(get("/cassettes/topics/orders").param("cursor", "this-is-not-a-cursor"))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   400,
                   ErrorTypes.INVALID_CURSOR.toString(),
                   ErrorCodes.INVALID_CURSOR,
                   "/cassettes/topics/orders"))
           .andExpect(jsonPath("$.title").value("Invalid Cursor"));
    }

    // =========================================================================
    // 503 — SQLException from underlying service surfaces as upstream-unavailable
    // =========================================================================

    @Test
    void replayTopic_sqlException_returnsUpstreamUnavailableProblem() throws Exception {
        when(topicService.query(
                anyString(), any(), any(), any(), any(), any(), anyInt(), any(), any(), anyString(), any()))
                .thenThrow(new java.sql.SQLException("duckdb gone"));

        mvc.perform(get("/cassettes/topics/orders"))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   503,
                   ErrorTypes.UPSTREAM_UNAVAILABLE.toString(),
                   ErrorCodes.UPSTREAM_UNAVAILABLE,
                   "/cassettes/topics/orders"));
    }

    // =========================================================================
    // 500 — uncaught RuntimeException: generic detail, no stack trace leak
    // =========================================================================

    @Test
    void entityStats_runtime_returnsInternalProblemWithoutDetails() throws Exception {
        when(entityService.getEntityStats(eq("order"), eq("o-1")))
                .thenThrow(new RuntimeException("db cursor already closed: SECRET"));

        mvc.perform(get("/cassettes/entities/order/o-1/stats"))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   500,
                   ErrorTypes.INTERNAL.toString(),
                   ErrorCodes.INTERNAL,
                   "/cassettes/entities/order/o-1/stats"))
           .andExpect(ProblemDetailAssertions.noStackTraceLeak());
    }

    // =========================================================================
    // 400 — type mismatch on a query param surfaces as validation problem
    // =========================================================================

    @Test
    void replayTopic_badLimit_returnsTypeMismatchProblem() throws Exception {
        // 'limit' must be int — a non-numeric value should produce a type-mismatch problem.
        mvc.perform(get("/cassettes/topics/orders").param("limit", "not-a-number"))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   400,
                   ErrorTypes.VALIDATION.toString(),
                   ErrorCodes.TYPE_MISMATCH,
                   "/cassettes/topics/orders"));
    }
}

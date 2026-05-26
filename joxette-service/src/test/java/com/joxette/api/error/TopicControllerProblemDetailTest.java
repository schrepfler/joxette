package com.joxette.api.error;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.management.ConfigRepository;
import com.joxette.management.KafkaTopicAdmin;
import com.joxette.management.TopicConfig;
import com.joxette.management.TopicController;
import com.joxette.recording.RecorderStatus;
import com.joxette.recording.RecordingCoordinator;
import com.joxette.replay.MessageRouter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;
import org.mockito.junit.jupiter.MockitoSettings;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

/**
 * MockMvc coverage of the RFC 7807 ProblemDetail contract for
 * {@link TopicController}: unknown topic (404), duplicate (409), invalid body
 * (400 with field-level errors), and uncaught RuntimeException (500 fallback
 * with no stack trace or internal details).
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TopicControllerProblemDetailTest {

    @Mock ConfigRepository config;
    @Mock RecordingCoordinator coordinator;
    @Mock MessageRouter router;
    @Mock KafkaTopicAdmin kafkaTopicAdmin;

    private MockMvc mvc;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        TopicController controller = new TopicController(config, coordinator, router, kafkaTopicAdmin);
        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    // =========================================================================
    // 404 — unknown topic maps to https://joxette.dev/problems/not-found
    // =========================================================================

    @Test
    void getUnknownTopic_returnsNotFoundProblem() throws Exception {
        when(config.findTopic("missing")).thenReturn(Optional.empty());

        mvc.perform(get("/topics/missing"))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   404,
                   ErrorTypes.NOT_FOUND.toString(),
                   ErrorCodes.NOT_FOUND,
                   "/topics/missing"))
           .andExpect(jsonPath("$.detail").value("Topic not found: missing"));
    }

    // =========================================================================
    // 409 — duplicate topic maps to https://joxette.dev/problems/conflict
    // =========================================================================

    @Test
    void createTopic_duplicate_returnsConflictProblem() throws Exception {
        TopicConfig existing = new TopicConfig("orders", "general", false, false, null, "latest", null);
        when(config.findTopic("orders")).thenReturn(Optional.of(existing));

        String body = mapper.writeValueAsString(Map.of(
                "topic", "orders",
                "mode",  "general"));

        mvc.perform(post("/topics").contentType(MediaType.APPLICATION_JSON).content(body))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   409,
                   ErrorTypes.CONFLICT.toString(),
                   ErrorCodes.CONFLICT,
                   "/topics"))
           .andExpect(jsonPath("$.detail").value("Topic already registered: orders"));
    }

    // =========================================================================
    // 400 — validation errors with field-level extension
    // =========================================================================

    @Test
    void createTopic_blankTopicField_returnsValidationProblemWithFieldErrors() throws Exception {
        String body = mapper.writeValueAsString(Map.of(
                "topic", "",
                "mode",  "general"));

        mvc.perform(post("/topics").contentType(MediaType.APPLICATION_JSON).content(body))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   400,
                   ErrorTypes.VALIDATION.toString(),
                   ErrorCodes.VALIDATION,
                   "/topics"))
           .andExpect(jsonPath("$.errors").isArray())
           .andExpect(jsonPath("$.errors[0].field").value("topic"))
           .andExpect(jsonPath("$.errors[0].message").exists());
    }

    @Test
    void createTopic_malformedJson_returnsMalformedRequestProblem() throws Exception {
        mvc.perform(post("/topics").contentType(MediaType.APPLICATION_JSON).content("{not json"))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   400,
                   ErrorTypes.VALIDATION.toString(),
                   ErrorCodes.MALFORMED_REQUEST,
                   "/topics"));
    }

    // =========================================================================
    // 500 — uncaught RuntimeException falls back to internal problem,
    // with NO stack trace or internal details leaked in the response body.
    // =========================================================================

    @Test
    void listTopics_runtimeException_returnsInternalProblemWithoutStackTrace() throws Exception {
        when(config.listTopics()).thenThrow(new RuntimeException("boom: secret-internal-detail"));

        mvc.perform(get("/topics"))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   500,
                   ErrorTypes.INTERNAL.toString(),
                   ErrorCodes.INTERNAL,
                   "/topics"))
           .andExpect(ProblemDetailAssertions.noStackTraceLeak())
           .andExpect(jsonPath("$.detail").value("An unexpected error occurred. See server logs for details."));
    }

    // =========================================================================
    // Parameterized mapping — every 404 endpoint surfaces the same problem type.
    // =========================================================================

    static Stream<Arguments> notFoundPaths() {
        return Stream.of(
                Arguments.of("GET", "/topics/does-not-exist"),
                Arguments.of("POST", "/topics/does-not-exist/pause"),
                Arguments.of("POST", "/topics/does-not-exist/resume")
        );
    }

    @ParameterizedTest(name = "{0} {1} → 404 not-found problem")
    @MethodSource("notFoundPaths")
    void unknownTopicAcrossEndpoints_returnsConsistentNotFound(String method, String path) throws Exception {
        when(config.findTopic(any())).thenReturn(Optional.empty());
        when(coordinator.listRunning()).thenReturn(Map.<String, RecorderStatus>of());

        var req = switch (method) {
            case "GET"  -> get(path);
            case "POST" -> post(path);
            default -> throw new IllegalArgumentException(method);
        };

        mvc.perform(req)
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   404,
                   ErrorTypes.NOT_FOUND.toString(),
                   ErrorCodes.NOT_FOUND,
                   path));
    }

    @Test
    @SuppressWarnings("unused")
    void routerReloadFailureDoesNotSurfaceAsError() throws Exception {
        // Sanity: SQLException from reload is swallowed; the route still succeeds.
        TopicConfig saved = new TopicConfig("payments", "both", false, false, null, "latest", null);
        when(config.findTopic("payments")).thenReturn(Optional.empty());
        when(config.upsertTopic("payments", "both", false, "latest", null)).thenReturn(saved);
        doThrow(new java.sql.SQLException("db error")).when(router).reload();
        when(coordinator.activeTopics()).thenReturn(java.util.Set.of("payments"));

        String body = mapper.writeValueAsString(Map.of(
                "topic", "payments",
                "mode",  "both"));

        mvc.perform(post("/topics").contentType(MediaType.APPLICATION_JSON).content(body))
           .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isCreated());
    }
}

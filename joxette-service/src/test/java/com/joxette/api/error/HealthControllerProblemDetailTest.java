package com.joxette.api.error;

import com.joxette.config.JoxetteProperties;
import com.joxette.management.HealthController;
import com.joxette.recording.RecordingCoordinator;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import com.joxette.config.BrokerConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.sql.Connection;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

/**
 * MockMvc coverage of the RFC 7807 ProblemDetail contract for
 * {@link HealthController}. Only the /metrics endpoint exposes a failure path
 * (catalog/lag errors on /health are caught and rendered as -1 values), so the
 * uncaught-RuntimeException contract is the main concern here.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HealthControllerProblemDetailTest {

    @Mock RecordingCoordinator coordinator;
    @Mock JoxetteProperties properties;
    @Mock BrokerConnectionFactory adminClient;
    @Mock Connection duckDB;
    @Mock PrometheusMeterRegistry metricsRegistry;

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        HealthController controller = new HealthController(
                coordinator, properties, adminClient, duckDB, metricsRegistry);
        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    // =========================================================================
    // 500 — uncaught RuntimeException: generic detail, no stack trace leak
    // =========================================================================

    @Test
    void metrics_runtime_returnsInternalProblemWithoutDetails() throws Exception {
        when(metricsRegistry.scrape()).thenThrow(new RuntimeException("prometheus registry closed: SENSITIVE-STATE"));

        mvc.perform(get("/metrics"))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   500,
                   ErrorTypes.INTERNAL.toString(),
                   ErrorCodes.INTERNAL,
                   "/metrics"))
           .andExpect(ProblemDetailAssertions.noStackTraceLeak());
    }

    @Test
    void health_runtime_returnsInternalProblemWithoutDetails() throws Exception {
        when(coordinator.activeTopics()).thenThrow(new RuntimeException("coordinator gone"));

        mvc.perform(get("/health"))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   500,
                   ErrorTypes.INTERNAL.toString(),
                   ErrorCodes.INTERNAL,
                   "/health"))
           .andExpect(ProblemDetailAssertions.noStackTraceLeak());
    }
}

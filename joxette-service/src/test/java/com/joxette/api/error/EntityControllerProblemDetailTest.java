package com.joxette.api.error;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.db.SchemaManager;
import com.joxette.management.ConfigRepository;
import com.joxette.management.EntityController;
import com.joxette.management.EntityTypeConfig;
import com.joxette.config.events.ConfigEventBus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

/**
 * MockMvc coverage of the RFC 7807 ProblemDetail contract for
 * {@link EntityController}: duplicate entity type (409), unknown type (404),
 * field validation (400 with per-field extension), and uncaught exception (500
 * fallback with no stack trace or internal details leaked).
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EntityControllerProblemDetailTest {

    @Mock ConfigRepository config;
    @Mock SchemaManager schemaManager;
    @Mock ConfigEventBus eventBus;

    private MockMvc mvc;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        EntityController controller = new EntityController(config, schemaManager, eventBus);
        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    // =========================================================================
    // 409 — POST /entities with duplicate type
    // =========================================================================

    @Test
    void createEntityType_duplicate_returnsConflictProblem() throws Exception {
        when(config.findEntityType("order")).thenReturn(Optional.of(new EntityTypeConfig("order", 256, null)));

        String body = mapper.writeValueAsString(Map.of("type", "order", "buckets", 256));

        mvc.perform(post("/entities").contentType(MediaType.APPLICATION_JSON).content(body))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   409,
                   ErrorTypes.CONFLICT.toString(),
                   ErrorCodes.CONFLICT,
                   "/entities"))
           .andExpect(jsonPath("$.detail").value("Entity type already registered: order"));
    }

    // =========================================================================
    // 400 — invalid entity-type name surfaces via ValidationException
    // =========================================================================

    @Test
    void createEntityType_invalidName_returnsValidationProblem() throws Exception {
        String body = mapper.writeValueAsString(Map.of("type", "BadType", "buckets", 256));

        mvc.perform(post("/entities").contentType(MediaType.APPLICATION_JSON).content(body))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   400,
                   ErrorTypes.VALIDATION.toString(),
                   ErrorCodes.VALIDATION,
                   "/entities"));
    }

    @Test
    void createEntityType_blankField_returnsValidationProblemWithFieldErrors() throws Exception {
        String body = mapper.writeValueAsString(Map.of("type", "", "buckets", 256));

        mvc.perform(post("/entities").contentType(MediaType.APPLICATION_JSON).content(body))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   400,
                   ErrorTypes.VALIDATION.toString(),
                   ErrorCodes.VALIDATION,
                   "/entities"))
           .andExpect(jsonPath("$.errors").isArray())
           .andExpect(jsonPath("$.errors[0].field").value("type"));
    }

    // =========================================================================
    // 404 — unknown entity type across several endpoints
    // =========================================================================

    static Stream<Arguments> notFoundPaths() {
        return Stream.of(
                Arguments.of("GET", "/entities/ghost"),
                Arguments.of("GET", "/entities/ghost/sources")
        );
    }

    @ParameterizedTest(name = "{0} {1} → 404 not-found")
    @MethodSource("notFoundPaths")
    void unknownEntityType_returnsNotFound(String method, String path) throws Exception {
        when(config.findEntityType(any())).thenReturn(Optional.empty());

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

    // =========================================================================
    // 500 — uncaught RuntimeException, generic detail, no stack trace
    // =========================================================================

    @Test
    void listEntityTypes_runtime_returnsInternalProblemWithoutDetailsLeak() throws Exception {
        when(config.listEntityTypes()).thenThrow(new RuntimeException("kaboom: sensitive-detail-zzz"));

        mvc.perform(get("/entities"))
           .andExpectAll(ProblemDetailAssertions.problemDetail(
                   500,
                   ErrorTypes.INTERNAL.toString(),
                   ErrorCodes.INTERNAL,
                   "/entities"))
           .andExpect(ProblemDetailAssertions.noStackTraceLeak());
    }
}

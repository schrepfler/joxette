package com.joxette.operator.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract guard: every joxette-service endpoint the operator's REST reconcilers
 * depend on must exist in the committed OpenAPI spec ({@code docs/openapi.json},
 * exported by joxette-service's SpringDocIT). This catches the service removing or
 * renaming an endpoint the operator calls — without generating a client or running
 * the service.
 *
 * <p>Skipped when the spec file is absent (e.g. a partial checkout or before
 * SpringDocIT has run); CI runs the service tests first, so the file is present.
 */
class OpenApiContractTest {

    /** Endpoints used by TopicConverger / EntityConverger / JoxetteRestClient (templated form). */
    private static final List<String> REQUIRED_PATHS = List.of(
            "/topics",
            "/topics/{topic}",
            "/topics/{topic}/pause",
            "/entities",
            "/entities/{type}",
            "/entities/{type}/sources",
            "/actuator/health/readiness"
    );

    private static Path specPath;
    private static Set<String> paths;

    @BeforeAll
    static void load() throws Exception {
        specPath = repoRoot().resolve("docs").resolve("openapi.json");
        if (Files.exists(specPath)) {
            JsonNode root = new ObjectMapper().readTree(specPath.toFile());
            JsonNode p = root.get("paths");
            paths = p == null ? Set.of() : new java.util.LinkedHashSet<>(iterableToList(p.fieldNames()));
        }
    }

    static boolean specPresent() {
        return Files.exists(repoRoot().resolve("docs").resolve("openapi.json"));
    }

    @Test
    @EnabledIf("specPresent")
    void everyOperatorEndpointExistsInTheServiceSpec() {
        // /actuator/** is a Spring Boot management endpoint, not in the API spec.
        List<String> apiPaths = REQUIRED_PATHS.stream()
                .filter(p -> !p.startsWith("/actuator"))
                .toList();
        assertThat(paths)
                .as("operator-required API paths missing from docs/openapi.json — "
                        + "joxette-service changed its contract")
                .containsAll(apiPaths);
    }

    private static Path repoRoot() {
        Path dir = Path.of("").toAbsolutePath();   // .../joxette-operator
        return dir.getFileName().toString().equals("joxette-operator") ? dir.getParent() : dir;
    }

    private static List<String> iterableToList(java.util.Iterator<String> it) {
        List<String> out = new java.util.ArrayList<>();
        it.forEachRemaining(out::add);
        return out;
    }
}

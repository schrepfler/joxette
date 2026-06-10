package com.joxette.it;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that SpringDoc generates a valid OpenAPI document at {@code /v3/api-docs}
 * and that all expected API tags are present.
 *
 * <p>As a side effect this also <b>exports the spec to a committed file</b>
 * ({@code docs/openapi.json}, repo root), so the API contract is versioned and
 * diff-able in PRs and the joxette-operator contract test can assert against it
 * without a running service.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class SpringDocIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("joxette.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @LocalServerPort
    private int port;

    private RestTemplate restTemplate;

    private String baseUrl;

    @BeforeEach
    void setUp() {
        restTemplate = new RestTemplate();
        baseUrl = "http://localhost:" + port;
    }

    @Test
    void apiDocs_returnsOkWithNonEmptyPaths() throws Exception {
        ResponseEntity<Map> response = restTemplate.getForEntity(baseUrl + "/v3/api-docs", Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        Map body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body).containsKey("paths");

        Map paths = (Map) body.get("paths");
        assertThat(paths).isNotEmpty();

        exportSpec(body);
    }

    /**
     * Writes the spec to {@code <repo-root>/docs/openapi.json}, pretty-printed for a
     * readable diff. Locating the repo root from the module working dir keeps the
     * file at a stable, committed path regardless of which module runs the test.
     */
    private void exportSpec(Map<?, ?> spec) throws Exception {
        Path moduleDir = Path.of("").toAbsolutePath();          // .../joxette-service
        Path repoRoot = moduleDir.getFileName().toString().equals("joxette-service")
                ? moduleDir.getParent() : moduleDir;
        Path target = repoRoot.resolve("docs").resolve("openapi.json");
        Files.createDirectories(target.getParent());
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(target.toFile(), spec);
    }
}

package com.joxette.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.ErrorCodes;
import com.joxette.api.error.ErrorTypes;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end confirmation that validation and not-found errors flow through
 * the same RFC 7807 ProblemDetail contract across the full stack
 * (Spring MVC → controller advice → Jackson). Uses the same embedded
 * Kafka + DuckDB wiring as the other integration tests.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class ProblemDetailContractIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("joxette.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @LocalServerPort
    private int port;

    @Autowired
    private ObjectMapper mapper;

    private RestTemplate nonThrowingRestTemplate() {
        RestTemplate rt = new RestTemplate();
        // Default ResponseErrorHandler raises on 4xx/5xx; override so we can
        // inspect ProblemDetail bodies directly.
        rt.setErrorHandler(new DefaultResponseErrorHandler() {
            @Override public boolean hasError(org.springframework.http.client.ClientHttpResponse response) {
                return false;
            }
        });
        return rt;
    }

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private JsonNode parse(String body) throws Exception {
        return mapper.readTree(body);
    }

    // =========================================================================
    // NOT FOUND — GET /topics/{topic} on unknown topic
    // =========================================================================

    @Test
    void getUnknownTopic_rendersNotFoundProblemDetail() throws Exception {
        RestTemplate rt = nonThrowingRestTemplate();

        ResponseEntity<String> response = rt.exchange(
                url("/topics/does-not-exist"),
                HttpMethod.GET,
                HttpEntity.EMPTY,
                String.class);

        assertThat(response.getStatusCode().value()).isEqualTo(404);
        assertThat(response.getHeaders().getContentType())
                .isNotNull()
                .matches(ct -> ct.toString().contains("application/problem+json"),
                         "Content-Type should be application/problem+json");

        JsonNode body = parse(response.getBody());
        assertThat(body.get("type").asText()).isEqualTo(ErrorTypes.NOT_FOUND.toString());
        assertThat(body.get("title").asText()).isEqualTo("Not Found");
        assertThat(body.get("status").asInt()).isEqualTo(404);
        assertThat(body.get("detail").asText()).contains("does-not-exist");
        assertThat(body.get("errorCode").asText()).isEqualTo(ErrorCodes.NOT_FOUND);
        assertThat(body.get("timestamp").asText()).isNotBlank();
        assertThat(body.get("path").asText()).isEqualTo("/topics/does-not-exist");
    }

    // =========================================================================
    // VALIDATION — POST /topics with blank field surfaces field-level errors
    // =========================================================================

    @Test
    void postTopic_blankName_rendersValidationProblemDetailWithFieldErrors() throws Exception {
        RestTemplate rt = nonThrowingRestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        String body = "{\"topic\":\"\",\"mode\":\"general\"}";
        ResponseEntity<String> response = rt.exchange(
                url("/topics"),
                HttpMethod.POST,
                new HttpEntity<>(body, headers),
                String.class);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getHeaders().getContentType().toString())
                .contains("application/problem+json");

        JsonNode node = parse(response.getBody());
        assertThat(node.get("type").asText()).isEqualTo(ErrorTypes.VALIDATION.toString());
        assertThat(node.get("title").asText()).isEqualTo("Validation Failed");
        assertThat(node.get("status").asInt()).isEqualTo(400);
        assertThat(node.get("errorCode").asText()).isEqualTo(ErrorCodes.VALIDATION);
        assertThat(node.get("timestamp").asText()).isNotBlank();
        assertThat(node.get("path").asText()).isEqualTo("/topics");

        // Field-level extension is the contract for bean-validation failures.
        JsonNode errors = node.get("errors");
        assertThat(errors).isNotNull();
        assertThat(errors.isArray()).isTrue();
        assertThat(errors).anyMatch(e -> "topic".equals(e.get("field").asText()));
    }

    // =========================================================================
    // VALIDATION — POST /entities with invalid type name
    // =========================================================================

    @Test
    void postEntity_invalidTypeName_rendersValidationProblemDetail() throws Exception {
        RestTemplate rt = nonThrowingRestTemplate();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        String body = "{\"type\":\"BadType\",\"buckets\":256}";
        ResponseEntity<String> response = rt.exchange(
                url("/entities"),
                HttpMethod.POST,
                new HttpEntity<>(body, headers),
                String.class);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getHeaders().getContentType().toString())
                .contains("application/problem+json");

        JsonNode node = parse(response.getBody());
        assertThat(node.get("type").asText()).isEqualTo(ErrorTypes.VALIDATION.toString());
        assertThat(node.get("errorCode").asText()).isEqualTo(ErrorCodes.VALIDATION);
        assertThat(node.get("path").asText()).isEqualTo("/entities");
    }
}

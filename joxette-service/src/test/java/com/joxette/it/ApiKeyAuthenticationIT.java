package com.joxette.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.ErrorCodes;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end confirmation that mutating endpoints require {@code X-API-Key} when
 * {@code joxette.security.api-key} is set, and that GET requests are never gated.
 * Uses the same embedded Kafka + DuckDB wiring as the other IT tests
 * (see {@code com.joxette.it.ProblemDetailContractIT}).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class ApiKeyAuthenticationIT {

    private static final String API_KEY = "test-only-secret-key";

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:4.0.2"));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("joxette.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("joxette.security.api-key", () -> API_KEY);
    }

    @LocalServerPort
    private int port;

    private final ObjectMapper mapper = new ObjectMapper();

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    // Uses the java.net.http.HttpClient-backed request factory rather than RestTemplate's
    // default SimpleClientHttpRequestFactory (java.net.HttpURLConnection). With a short POST
    // body and setFixedLengthStreamingMode (which SimpleClientHttpRequestFactory always uses),
    // HttpURLConnection can lose the response body when the server (correctly) sends the 401
    // before consuming the request body — a well-known JDK HttpURLConnection limitation, not a
    // server-side defect: raw java.net.http.HttpClient and curl both read the body fine.
    private RestTemplate nonThrowingRestTemplate() {
        RestTemplate rt = new RestTemplate(new JdkClientHttpRequestFactory());
        rt.setErrorHandler(new DefaultResponseErrorHandler() {
            @Override public boolean hasError(ClientHttpResponse response) { return false; }
        });
        return rt;
    }

    @Test
    void postWithoutApiKey_returns401ProblemJson() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String body = "{\"type\":\"customer\",\"buckets\":256}";

        ResponseEntity<String> response = nonThrowingRestTemplate().exchange(
                url("/entities"), HttpMethod.POST, new HttpEntity<>(body, headers), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(response.getHeaders().getContentType().toString()).contains("application/problem+json");
        JsonNode node = mapper.readTree(response.getBody());
        assertThat(node.get("errorCode").asText()).isEqualTo(ErrorCodes.UNAUTHORIZED);
        assertThat(node.get("status").asInt()).isEqualTo(401);
        assertThat(node.get("path").asText()).isEqualTo("/entities");
    }

    @Test
    void postWithCorrectApiKey_reachesController() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-API-Key", API_KEY);
        String body = "{\"type\":\"widget\",\"buckets\":256}";

        ResponseEntity<String> response = nonThrowingRestTemplate().exchange(
                url("/entities"), HttpMethod.POST, new HttpEntity<>(body, headers), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
    }

    @Test
    void getWithoutApiKey_succeedsUnauthenticated() {
        ResponseEntity<String> response = nonThrowingRestTemplate().getForEntity(url("/entities"), String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}

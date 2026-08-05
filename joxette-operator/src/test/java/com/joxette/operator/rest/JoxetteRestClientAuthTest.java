package com.joxette.operator.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link JoxetteRestClient}'s {@code X-API-Key} header support (finding C3): the
 * operator has no way to authenticate against a joxette-service instance with
 * {@code joxette.security.api-key} set unless it sends this header on every
 * mutating call.
 */
class JoxetteRestClientAuthTest {

    private HttpServer server;

    /** path+method -> observed X-API-Key header value (or "" if absent). */
    private final Map<String, String> observedHeaders = new ConcurrentHashMap<>();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private JoxetteRestClient clientFor(int port, String apiKey) {
        return new JoxetteRestClient("http://127.0.0.1:" + port,
                java.net.http.HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build(),
                new ObjectMapper(), Duration.ofSeconds(2), apiKey);
    }

    private void startEchoServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/topics", ex -> {
            String key = ex.getRequestHeaders().getFirst("X-API-Key");
            observedHeaders.put(ex.getRequestMethod() + " " + ex.getRequestURI().getPath(),
                    key != null ? key : "");
            byte[] body = "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
    }

    @Test
    void postSendsApiKeyHeaderWhenConfigured() throws IOException {
        startEchoServer();
        JoxetteRestClient client = clientFor(server.getAddress().getPort(), "secret-123");

        client.postJson("/topics", Map.of("topic", "orders"));

        assertThat(observedHeaders).containsEntry("POST /v1/topics", "secret-123");
    }

    @Test
    void putSendsApiKeyHeaderWhenConfigured() throws IOException {
        startEchoServer();
        JoxetteRestClient client = clientFor(server.getAddress().getPort(), "secret-123");

        client.putJson("/topics", Map.of("topic", "orders"));

        assertThat(observedHeaders).containsEntry("PUT /v1/topics", "secret-123");
    }

    @Test
    void deleteSendsApiKeyHeaderWhenConfigured() throws IOException {
        startEchoServer();
        JoxetteRestClient client = clientFor(server.getAddress().getPort(), "secret-123");

        client.delete("/topics");

        assertThat(observedHeaders).containsEntry("DELETE /v1/topics", "secret-123");
    }

    @Test
    void postWithNoBodySendsApiKeyHeaderWhenConfigured() throws IOException {
        startEchoServer();
        JoxetteRestClient client = clientFor(server.getAddress().getPort(), "secret-123");

        client.post("/topics");

        assertThat(observedHeaders).containsEntry("POST /v1/topics", "secret-123");
    }

    @Test
    void noHeaderSentWhenApiKeyNotConfigured() throws IOException {
        startEchoServer();
        JoxetteRestClient client = clientFor(server.getAddress().getPort(), null);

        int status = client.postJson("/topics", Map.of("topic", "orders"));

        assertThat(status).isEqualTo(200);
        assertThat(observedHeaders).containsEntry("POST /v1/topics", "");
    }

    @Test
    void noHeaderSentWhenApiKeyBlank() throws IOException {
        startEchoServer();
        JoxetteRestClient client = clientFor(server.getAddress().getPort(), "   ");

        client.postJson("/topics", Map.of("topic", "orders"));

        assertThat(observedHeaders).containsEntry("POST /v1/topics", "");
    }

    @Test
    void getNeverSendsApiKeyHeaderEvenWhenConfigured() throws IOException {
        startEchoServer();
        JoxetteRestClient client = clientFor(server.getAddress().getPort(), "secret-123");

        client.getJson("/topics");

        assertThat(observedHeaders).containsEntry("GET /v1/topics", "");
    }
}

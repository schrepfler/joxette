package com.joxette.operator.topic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.operator.rest.JoxetteRestClient;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link TopicConverger} together with {@link JoxetteRestClient} against
 * a real in-process HTTP server that emulates the {@code /topics} API — the full
 * GET/POST/PUT/DELETE path, not a mock of the client.
 */
class TopicConvergerTest {

    private HttpServer server;
    private JoxetteRestClient client;
    private final ObjectMapper json = new ObjectMapper();

    /** topic name -> stored config JSON; null entry means "not present". */
    private final Map<String, String> store = new ConcurrentHashMap<>();
    private final List<String> calls = new ArrayList<>();

    @BeforeEach
    void start() throws IOException {
        store.clear();
        calls.clear();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/topics", this::handle);
        server.start();
        client = new JoxetteRestClient("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private void handle(HttpExchange ex) throws IOException {
        String method = ex.getRequestMethod();
        String path = ex.getRequestURI().getPath();   // /topics or /topics/{t} or /topics/{t}/pause
        calls.add(method + " " + path);
        String[] parts = path.split("/");              // ["", "topics", "{t}", "pause"?]
        try {
            if (method.equals("GET") && parts.length == 3) {
                String t = parts[2];
                if (store.containsKey(t)) {
                    respond(ex, 200, store.get(t));
                } else {
                    respond(ex, 404, "{}");
                }
            } else if (method.equals("POST") && parts.length == 2) {
                var body = json.readTree(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                String t = body.get("topic").asText();
                store.put(t, body.toString());
                respond(ex, 201, body.toString());
            } else if (method.equals("PUT") && parts.length == 3) {
                String t = parts[2];
                var body = json.readTree(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                // merge mode/brokerId onto stored, keep topic field
                var merged = json.createObjectNode();
                merged.put("topic", t);
                merged.set("mode", body.get("mode"));
                if (body.has("brokerId")) merged.set("brokerId", body.get("brokerId"));
                store.put(t, merged.toString());
                respond(ex, 200, merged.toString());
            } else if (method.equals("POST") && parts.length == 4 && parts[3].equals("pause")) {
                String t = parts[2];
                respond(ex, store.containsKey(t) ? 200 : 404, "{}");
            } else if (method.equals("DELETE") && parts.length == 3) {
                String t = parts[2];
                respond(ex, store.remove(t) != null ? 204 : 404, "");
            } else {
                respond(ex, 400, "");
            }
        } catch (Exception e) {
            respond(ex, 500, e.getMessage());
        }
    }

    private void respond(HttpExchange ex, int code, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(code, b.length == 0 ? -1 : b.length);
        if (b.length > 0) {
            try (OutputStream os = ex.getResponseBody()) {
                os.write(b);
            }
        }
        ex.close();
    }

    private RecordedTopicSpec spec(String topic, String mode, String broker) {
        RecordedTopicSpec s = new RecordedTopicSpec();
        s.setTopic(topic);
        s.setMode(mode);
        s.setBrokerId(broker);
        return s;
    }

    @Test
    void createsAbsentTopic() {
        var result = new TopicConverger(client).converge(spec("orders.events", "both", null));
        assertThat(result).isEqualTo(JoxetteRestClient.ChangeResult.CREATED);
        assertThat(store).containsKey("orders.events");
        assertThat(calls).contains("GET /topics/orders.events", "POST /topics");
    }

    @Test
    void unchangedWhenAlreadyMatching() {
        store.put("orders.events", "{\"topic\":\"orders.events\",\"mode\":\"both\"}");
        var result = new TopicConverger(client).converge(spec("orders.events", "both", null));
        assertThat(result).isEqualTo(JoxetteRestClient.ChangeResult.UNCHANGED);
        assertThat(calls).noneMatch(c -> c.startsWith("PUT") || c.startsWith("POST"));
    }

    @Test
    void updatesOnModeDrift() {
        store.put("orders.events", "{\"topic\":\"orders.events\",\"mode\":\"general\"}");
        var result = new TopicConverger(client).converge(spec("orders.events", "both", null));
        assertThat(result).isEqualTo(JoxetteRestClient.ChangeResult.UPDATED);
        assertThat(json(store.get("orders.events"))).contains("both");
    }

    @Test
    void deletePolicyDeletesTopic() {
        store.put("orders.events", "{\"topic\":\"orders.events\",\"mode\":\"both\"}");
        RecordedTopicSpec s = spec("orders.events", "both", null);
        s.setDeletionPolicy(RecordedTopicSpec.DeletionPolicy.Delete);
        var result = new TopicConverger(client).onDelete(s);
        assertThat(result).isEqualTo(JoxetteRestClient.ChangeResult.DELETED);
        assertThat(store).doesNotContainKey("orders.events");
    }

    @Test
    void pausePolicyPausesTopic() {
        store.put("orders.events", "{\"topic\":\"orders.events\",\"mode\":\"both\"}");
        RecordedTopicSpec s = spec("orders.events", "both", null);
        s.setDeletionPolicy(RecordedTopicSpec.DeletionPolicy.Pause);
        var result = new TopicConverger(client).onDelete(s);
        assertThat(result).isEqualTo(JoxetteRestClient.ChangeResult.UPDATED);
        assertThat(calls).contains("POST /topics/orders.events/pause");
        assertThat(store).containsKey("orders.events"); // pause leaves data
    }

    @Test
    void orphanPolicyTouchesNothing() {
        store.put("orders.events", "{\"topic\":\"orders.events\",\"mode\":\"both\"}");
        RecordedTopicSpec s = spec("orders.events", "both", null);
        s.setDeletionPolicy(RecordedTopicSpec.DeletionPolicy.Orphan);
        var result = new TopicConverger(client).onDelete(s);
        assertThat(result).isEqualTo(JoxetteRestClient.ChangeResult.UNCHANGED);
        assertThat(calls).isEmpty();
    }

    private static String json(String s) {
        return s;
    }
}

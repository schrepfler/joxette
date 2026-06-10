package com.joxette.operator.entity;

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
 * Exercises {@link EntityConverger} + {@link JoxetteRestClient} against a real
 * in-process HTTP server emulating the {@code /entities} API.
 */
class EntityConvergerTest {

    private HttpServer server;
    private JoxetteRestClient client;
    private final ObjectMapper json = new ObjectMapper();

    private final Map<String, String> types = new ConcurrentHashMap<>();
    private final List<String> sourcePosts = new ArrayList<>();
    private final List<String> calls = new ArrayList<>();

    @BeforeEach
    void start() throws IOException {
        types.clear();
        sourcePosts.clear();
        calls.clear();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/entities", this::handle);
        server.start();
        client = new JoxetteRestClient("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private void handle(HttpExchange ex) throws IOException {
        String method = ex.getRequestMethod();
        String path = ex.getRequestURI().getPath();
        calls.add(method + " " + path);
        String[] parts = path.split("/");   // ["","entities","{type}","sources"?]
        try {
            if (method.equals("GET") && parts.length == 3) {
                String t = parts[2];
                if (types.containsKey(t)) respond(ex, 200, types.get(t)); else respond(ex, 404, "{}");
            } else if (method.equals("POST") && parts.length == 2) {
                var body = json.readTree(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                types.put(body.get("type").asText(), body.toString());
                respond(ex, 201, body.toString());
            } else if (method.equals("PUT") && parts.length == 3) {
                var body = json.readTree(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                var merged = json.createObjectNode();
                merged.put("type", parts[2]);
                merged.set("buckets", body.get("buckets"));
                types.put(parts[2], merged.toString());
                respond(ex, 200, merged.toString());
            } else if (method.equals("POST") && parts.length == 4 && parts[3].equals("sources")) {
                sourcePosts.add(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                respond(ex, 201, "{}");
            } else if (method.equals("DELETE") && parts.length == 3) {
                respond(ex, types.remove(parts[2]) != null ? 204 : 404, "");
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

    private EntityTypeSpec orderSpec() {
        EntityTypeSpec s = new EntityTypeSpec();
        s.setType("order");
        s.setBuckets(256);
        EntityTypeSpec.Source src = new EntityTypeSpec.Source();
        src.setTopic("orders.events");
        src.setMode("both");
        EntityTypeSpec.Matcher m = new EntityTypeSpec.Matcher();
        m.setMessageType("OrderCreated");
        m.setIdSource("value");
        m.setIdExpression("$.order_id");
        src.setMatchers(List.of(m));
        s.setSources(List.of(src));
        return s;
    }

    @Test
    void createsTypeAndUpsertsSources() {
        int sources = new EntityConverger(client).converge(orderSpec());
        assertThat(sources).isEqualTo(1);
        assertThat(types).containsKey("order");
        assertThat(calls).contains("GET /entities/order", "POST /entities", "POST /entities/order/sources");
        assertThat(sourcePosts).hasSize(1);
        assertThat(sourcePosts.get(0)).contains("orders.events", "OrderCreated", "$.order_id");
    }

    @Test
    void updatesBucketsOnDriftAndStillUpsertsSources() {
        types.put("order", "{\"type\":\"order\",\"buckets\":128}");
        new EntityConverger(client).converge(orderSpec()); // spec wants 256
        assertThat(calls).contains("PUT /entities/order");
        assertThat(types.get("order")).contains("256");
        assertThat(sourcePosts).hasSize(1);
    }

    @Test
    void noTypeWriteWhenBucketsMatchButSourcesStillUpserted() {
        types.put("order", "{\"type\":\"order\",\"buckets\":256}");
        new EntityConverger(client).converge(orderSpec());
        assertThat(calls).noneMatch(c -> c.equals("POST /entities") || c.equals("PUT /entities/order"));
        assertThat(sourcePosts).hasSize(1); // sources are always upserted (idempotent)
    }

    @Test
    void deletePolicyDeletesType() {
        types.put("order", "{\"type\":\"order\",\"buckets\":256}");
        EntityTypeSpec spec = orderSpec();
        spec.setDeletionPolicy(EntityTypeSpec.DeletionPolicy.Delete);
        var result = new EntityConverger(client).onDelete(spec);
        assertThat(result).isEqualTo(JoxetteRestClient.ChangeResult.DELETED);
        assertThat(types).doesNotContainKey("order");
    }

    @Test
    void orphanPolicyTouchesNothing() {
        types.put("order", "{\"type\":\"order\",\"buckets\":256}");
        EntityTypeSpec spec = orderSpec();
        spec.setDeletionPolicy(EntityTypeSpec.DeletionPolicy.Orphan);
        var result = new EntityConverger(client).onDelete(spec);
        assertThat(result).isEqualTo(JoxetteRestClient.ChangeResult.UNCHANGED);
        assertThat(calls).isEmpty();
        assertThat(types).containsKey("order");
    }
}

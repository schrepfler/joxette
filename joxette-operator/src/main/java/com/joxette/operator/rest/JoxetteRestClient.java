package com.joxette.operator.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;

/**
 * Thin REST client over the dozen joxette-service endpoints the operator's API
 * reconcilers use ({@code /topics}, {@code /entities} and their sub-resources).
 *
 * <p>Built on the JDK {@link HttpClient} — no extra dependency. One instance per
 * target cluster base URL (resolved from a {@code clusterRef} → Service DNS).
 * Methods map HTTP status to small typed outcomes so reconcilers can branch
 * without parsing bodies for control flow.
 *
 * <h2>API-key auth</h2>
 * <p>When constructed with a non-blank {@code apiKey}, every mutating request
 * (POST/PUT/DELETE) carries it as the {@code X-API-Key} header, matching
 * {@code SecurityConfig}'s {@code ApiKeyAuthenticationFilter} in joxette-service.
 * GET requests never send it — {@code SecurityConfig} permits GET/HEAD
 * unauthenticated regardless. When {@code apiKey} is null/blank (the default —
 * see {@link RestClientFactory}), no header is sent and requests behave exactly
 * as before this was added, which only matters if the target cluster has
 * {@code joxette.security.api-key} configured.
 */
public class JoxetteRestClient {

    private static final String API_KEY_HEADER = "X-API-Key";

    private final HttpClient http;
    private final ObjectMapper json;
    private final URI baseUri;
    private final Duration timeout;
    private final String apiKey;

    public JoxetteRestClient(String baseUrl) {
        this(baseUrl, null);
    }

    /** @param apiKey sent as {@code X-API-Key} on mutating requests; null/blank to send none. */
    public JoxetteRestClient(String baseUrl, String apiKey) {
        this(baseUrl,
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build(),
                new ObjectMapper(), Duration.ofSeconds(10), apiKey);
    }

    JoxetteRestClient(String baseUrl, HttpClient http, ObjectMapper json, Duration timeout) {
        this(baseUrl, http, json, timeout, null);
    }

    JoxetteRestClient(String baseUrl, HttpClient http, ObjectMapper json, Duration timeout, String apiKey) {
        this.baseUri = URI.create(baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl);
        this.http = http;
        this.json = json;
        this.timeout = timeout;
        this.apiKey = apiKey;
    }

    /** Outcome of a converge call, so reconcilers can report what happened. */
    public enum ChangeResult { CREATED, UPDATED, UNCHANGED, DELETED, NOT_FOUND }

    // ---- readiness ----------------------------------------------------------

    /**
     * Best-effort readiness probe used by the API reconcilers to decide whether to
     * converge now or reschedule. Returns {@code false} (never throws) when the
     * cluster's REST endpoint is unreachable or not yet ready — i.e. before the
     * JoxetteCluster pods are serving. Hits Spring Boot's readiness group, which
     * gates on catalog ATTACH.
     */
    public boolean ready() {
        try {
            HttpResponse<String> res = send(request("/actuator/health/readiness").GET().build());
            return res.statusCode() / 100 == 2;
        } catch (JoxetteRestException e) {
            return false;
        }
    }

    // ---- generic verbs ------------------------------------------------------

    /** GET; returns the parsed body, or empty on 404. */
    public Optional<ObjectNode> getJson(String path) {
        HttpResponse<String> res = send(request(path).GET().build());
        if (res.statusCode() == 404) {
            return Optional.empty();
        }
        requireSuccess(res, "GET " + path);
        return Optional.of(parse(res.body()));
    }

    /** POST a JSON body; returns the HTTP status code. */
    public int postJson(String path, Object body) {
        HttpResponse<String> res = send(authenticatedRequest(path)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(toJson(body))).build());
        return res.statusCode();
    }

    /** PUT a JSON body; returns the HTTP status code. */
    public int putJson(String path, Object body) {
        HttpResponse<String> res = send(authenticatedRequest(path)
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(toJson(body))).build());
        return res.statusCode();
    }

    /** POST with no body (e.g. /pause). Returns the status code. */
    public int post(String path) {
        HttpResponse<String> res = send(authenticatedRequest(path)
                .POST(HttpRequest.BodyPublishers.noBody()).build());
        return res.statusCode();
    }

    /** DELETE; returns true if deleted (2xx) or already gone (404). */
    public boolean delete(String path) {
        HttpResponse<String> res = send(authenticatedRequest(path).DELETE().build());
        int sc = res.statusCode();
        if (sc == 404) {
            return true;
        }
        requireSuccess(res, "DELETE " + path);
        return true;
    }

    // ---- helpers ------------------------------------------------------------

    /** Prefix applied to every joxette-service REST endpoint under {@code com.joxette}. */
    private static final String API_PREFIX = "/v1";

    private HttpRequest.Builder request(String path) {
        // path always starts with '/'; baseUri has no trailing slash.
        return HttpRequest.newBuilder(URI.create(baseUri + apiPath(path))).timeout(timeout);
    }

    /** Like {@link #request(String)}, plus the {@code X-API-Key} header for mutating verbs. */
    private HttpRequest.Builder authenticatedRequest(String path) {
        HttpRequest.Builder builder = request(path);
        if (apiKey != null && !apiKey.isBlank()) {
            builder.header(API_KEY_HEADER, apiKey);
        }
        return builder;
    }

    /**
     * Prepends the {@code /v1} API prefix to every {@code com.joxette} controller
     * path. Spring Boot Actuator endpoints (e.g. {@code /actuator/health/readiness})
     * are unprefixed and exempt.
     */
    private static String apiPath(String path) {
        return path.startsWith("/actuator") ? path : API_PREFIX + path;
    }

    private HttpResponse<String> send(HttpRequest req) {
        try {
            return http.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new JoxetteRestException("Joxette REST call failed: " + req.method() + " "
                    + req.uri() + " — " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JoxetteRestException("Interrupted calling " + req.uri(), e);
        }
    }

    private void requireSuccess(HttpResponse<String> res, String what) {
        if (res.statusCode() / 100 != 2) {
            throw new JoxetteRestException(what + " -> HTTP " + res.statusCode() + ": " + res.body());
        }
    }

    private ObjectNode parse(String body) {
        try {
            return (ObjectNode) json.readTree(body);
        } catch (Exception e) {
            throw new JoxetteRestException("Unparseable JSON response: " + e.getMessage(), e);
        }
    }

    private String toJson(Object body) {
        try {
            return json.writeValueAsString(body);
        } catch (Exception e) {
            throw new JoxetteRestException("Failed to serialise request body: " + e.getMessage(), e);
        }
    }

    public String baseUrl() {
        return baseUri.toString();
    }
}

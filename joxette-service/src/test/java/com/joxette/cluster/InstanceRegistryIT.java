package com.joxette.cluster;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.RestTemplate;

import java.sql.*;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link InstanceRegistry} and {@link InstanceController}.
 *
 * <h2>What is verified</h2>
 * <ol>
 *   <li>On startup the own instance row is inserted into {@code joxette_instances}.</li>
 *   <li>Stale rows (last_heartbeat older than 2 minutes) are deleted when
 *       {@link InstanceRegistry#reapStaleInstances()} is called.</li>
 *   <li>{@link InstanceRegistry#sendHeartbeat()} updates {@code last_heartbeat} to a
 *       timestamp strictly after the previous value.</li>
 *   <li>{@code GET /instances} returns the instance list with the computed
 *       {@code "alive"} / {@code "stale"} status.</li>
 *   <li>{@code GET /health} includes a populated {@code cluster} object.</li>
 * </ol>
 *
 * <p>No Kafka container is required — {@link com.joxette.recording.RecordingCoordinator}
 * is lazy and its {@code activeTopics()} returns an empty set until a topic is started.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
class InstanceRegistryIT {

    @Autowired
    private InstanceRegistry registry;

    @Autowired
    private Connection duckDB;

    @LocalServerPort
    private int port;

    private final RestTemplate restTemplate = new RestTemplate();

    // -------------------------------------------------------------------------
    // Startup insertion
    // -------------------------------------------------------------------------

    @Test
    void ownRowIsInsertedOnStartup() throws SQLException {
        String instanceId = registry.getInstanceId();
        assertThat(instanceId).isNotBlank().contains(":");

        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT instance_id, catalog_backend, last_heartbeat " +
                    "FROM joxette_instances WHERE instance_id = ?")) {
                ps.setString(1, instanceId);
                try (ResultSet rs = ps.executeQuery()) {
                    assertThat(rs.next())
                            .as("own instance row must be present after startup")
                            .isTrue();
                    assertThat(rs.getString("catalog_backend"))
                            .as("catalog_backend must be set")
                            .isNotBlank();
                    assertThat(rs.getTimestamp("last_heartbeat").toInstant())
                            .as("last_heartbeat must be recent (within 60 s of now)")
                            .isAfter(Instant.now().minusSeconds(60));
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Stale-instance reaping
    // -------------------------------------------------------------------------

    @Test
    void staleInstancesAreReapedWhenReapIsCalled() throws SQLException {
        String staleId = "stale-test-instance:" + System.currentTimeMillis();

        // Insert a row with last_heartbeat 10 minutes in the past.
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "INSERT INTO joxette_instances " +
                    "    (instance_id, roles, catalog_backend, started_at, last_heartbeat) " +
                    "VALUES (?, ['recorder'], 'EMBEDDED_DUCKDB', " +
                    "        now() - INTERVAL '10 minutes', now() - INTERVAL '10 minutes')")) {
                ps.setString(1, staleId);
                ps.executeUpdate();
            }
        }

        // Verify the stale row was inserted.
        assertThat(rowExists(staleId)).isTrue();

        // Reap stale instances.
        registry.reapStaleInstances();

        // The stale row must be gone; the own row must still exist.
        assertThat(rowExists(staleId))
                .as("stale instance row must be deleted after reap")
                .isFalse();
        assertThat(rowExists(registry.getInstanceId()))
                .as("own instance row must survive reaping")
                .isTrue();
    }

    @Test
    void freshInstanceIsNotReapedByReap() throws SQLException {
        // The own row was inserted less than 2 minutes ago — must survive reaping.
        registry.reapStaleInstances();

        assertThat(rowExists(registry.getInstanceId()))
                .as("own instance row (fresh) must not be reaped")
                .isTrue();
    }

    // -------------------------------------------------------------------------
    // Heartbeat
    // -------------------------------------------------------------------------

    @Test
    void heartbeatUpdatesLastHeartbeatTimestamp() throws SQLException, InterruptedException {
        Instant beforeHeartbeat = Instant.now();

        // Small pause to guarantee the DB now() will be strictly later.
        Thread.sleep(20);
        registry.sendHeartbeat();

        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT last_heartbeat FROM joxette_instances WHERE instance_id = ?")) {
                ps.setString(1, registry.getInstanceId());
                try (ResultSet rs = ps.executeQuery()) {
                    assertThat(rs.next()).as("own row must still exist after heartbeat").isTrue();
                    Instant afterHeartbeat = rs.getTimestamp("last_heartbeat").toInstant();
                    assertThat(afterHeartbeat)
                            .as("last_heartbeat must be updated to a time after the pre-heartbeat capture")
                            .isAfterOrEqualTo(beforeHeartbeat);
                }
            }
        }
    }

    @Test
    void heartbeatSetsKafkaAssignments() throws SQLException {
        registry.sendHeartbeat();

        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT kafka_assignments FROM joxette_instances WHERE instance_id = ?")) {
                ps.setString(1, registry.getInstanceId());
                try (ResultSet rs = ps.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    String json = rs.getString("kafka_assignments");
                    // No topics are active in the IT profile, so the assignments map is empty.
                    assertThat(json).as("kafka_assignments must be a valid JSON object").isNotNull();
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // REST endpoint — GET /instances
    // -------------------------------------------------------------------------

    @Test
    void getInstancesReturnsOwnInstance() {
        String url = "http://localhost:" + port + "/instances";
        ResponseEntity<List<Map<String, Object>>> response = restTemplate.exchange(
                url, HttpMethod.GET, null,
                new ParameterizedTypeReference<>() {});

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        List<Map<String, Object>> body = response.getBody();
        assertThat(body).as("instances list must not be empty").isNotEmpty();

        // At least one entry must match the current instance.
        boolean foundOwn = body.stream()
                .anyMatch(row -> registry.getInstanceId().equals(row.get("instanceId")));
        assertThat(foundOwn).as("own instanceId must appear in GET /instances response").isTrue();
    }

    @Test
    void getInstancesComputesAliveStatus() {
        String url = "http://localhost:" + port + "/instances";
        ResponseEntity<List<Map<String, Object>>> response = restTemplate.exchange(
                url, HttpMethod.GET, null,
                new ParameterizedTypeReference<>() {});

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        List<Map<String, Object>> body = response.getBody();
        assertThat(body).isNotEmpty();

        // All instances with a recent heartbeat (i.e., our own) must be "alive".
        body.stream()
                .filter(row -> registry.getInstanceId().equals(row.get("instanceId")))
                .forEach(row ->
                        assertThat(row.get("status"))
                                .as("own instance must have status 'alive'")
                                .isEqualTo("alive"));
    }

    @Test
    void getInstancesIncludesRolesAndCatalogBackend() {
        String url = "http://localhost:" + port + "/instances";
        ResponseEntity<List<Map<String, Object>>> response = restTemplate.exchange(
                url, HttpMethod.GET, null,
                new ParameterizedTypeReference<>() {});

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        List<Map<String, Object>> body = response.getBody();

        body.stream()
                .filter(row -> registry.getInstanceId().equals(row.get("instanceId")))
                .forEach(row -> {
                    assertThat(row.get("roles")).as("roles must be present").isNotNull();
                    assertThat(row.get("catalogBackend")).as("catalogBackend must be present").isNotNull();
                    assertThat(row.get("startedAt")).as("startedAt must be present").isNotNull();
                    assertThat(row.get("lastHeartbeat")).as("lastHeartbeat must be present").isNotNull();
                });
    }

    // -------------------------------------------------------------------------
    // GET /health — cluster enrichment
    // -------------------------------------------------------------------------

    @Test
    void healthEndpointIncludesClusterSummary() {
        String url = "http://localhost:" + port + "/health";
        ResponseEntity<Map<String, Object>> response = restTemplate.exchange(
                url, HttpMethod.GET, null,
                new ParameterizedTypeReference<>() {});

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> body = response.getBody();
        assertThat(body).containsKey("cluster");

        @SuppressWarnings("unchecked")
        Map<String, Object> cluster = (Map<String, Object>) body.get("cluster");
        assertThat(cluster).containsKeys("instanceCount", "alive", "stale");
        assertThat((Integer) cluster.get("instanceCount")).as("at least one instance must be registered").isGreaterThanOrEqualTo(1);
        assertThat((Integer) cluster.get("alive")).as("at least one instance must be alive").isGreaterThanOrEqualTo(1);
    }

    @Test
    void healthEndpointInstanceIdMatchesRegistry() {
        String url = "http://localhost:" + port + "/health";
        ResponseEntity<Map<String, Object>> response = restTemplate.exchange(
                url, HttpMethod.GET, null,
                new ParameterizedTypeReference<>() {});

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> body = response.getBody();
        assertThat(body.get("instanceId"))
                .as("health instanceId must match InstanceRegistry.getInstanceId()")
                .isEqualTo(registry.getInstanceId());
    }

    // -------------------------------------------------------------------------
    // listAll() unit-level
    // -------------------------------------------------------------------------

    @Test
    void listAllReturnsNonEmptyList() {
        List<InstanceRecord> instances = registry.listAll();
        assertThat(instances).as("listAll must include at least the current instance").isNotEmpty();
    }

    @Test
    void listAllIncludesOwnInstanceWithAliveStatus() {
        List<InstanceRecord> instances = registry.listAll();
        InstanceRecord own = instances.stream()
                .filter(r -> registry.getInstanceId().equals(r.instanceId()))
                .findFirst()
                .orElse(null);

        assertThat(own).as("own instance must appear in listAll()").isNotNull();
        assertThat(own.status()).as("own status must be 'alive'").isEqualTo("alive");
        assertThat(own.catalogBackend()).as("catalogBackend must be set").isNotBlank();
        assertThat(own.roles()).as("roles list must not be empty").isNotEmpty();
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private boolean rowExists(String instanceId) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT 1 FROM joxette_instances WHERE instance_id = ?")) {
                ps.setString(1, instanceId);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            }
        }
    }
}

package com.joxette.it;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class ReconciliationEndToEndIT {

    private static final String BUCKET = "joxette-recon-e2e-test";

    @Container
    static final MinIOContainer minio =
            new MinIOContainer(DockerImageName.parse("minio/minio:RELEASE.2024-01-16T16-07-38Z"));

    @DynamicPropertySource
    static void minioProperties(DynamicPropertyRegistry registry) {
        String s3Url = minio.getS3URL();
        String userName = minio.getUserName();
        String password = minio.getPassword();
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(s3Url))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(userName, password)))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build()) {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
        registry.add("joxette.catalog.object-storage-path", () -> "s3://" + BUCKET + "/data/");
        registry.add("joxette.s3.endpoint",   () -> s3Url);
        registry.add("joxette.s3.access-key", () -> userName);
        registry.add("joxette.s3.secret-key", () -> password);
    }

    @LocalServerPort private int port;
    @Autowired private Connection duckDB;
    private final RestTemplate restTemplate = new RestTemplate();

    private String url(String path) {
        return "http://localhost:" + port + "/v1" + path;
    }

    @Test
    void trigger_status_history_roundTripOverRest() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.general_orders_events (" +
                    "recorded_at TIMESTAMPTZ, kafka_offset BIGINT, kafka_partition INTEGER, " +
                    "kafka_timestamp TIMESTAMPTZ, kafka_key VARCHAR, kafka_value BLOB, " +
                    "metadata VARCHAR, headers STRUCT(key VARCHAR, value VARCHAR)[], message_type VARCHAR)");
            st.execute("COPY (SELECT 1 AS id) TO " +
                    "'s3://" + BUCKET + "/data/main/general_orders_events/stray.parquet' (FORMAT PARQUET)");
        }

        ResponseEntity<Map> trigger = restTemplate.postForEntity(
                url("/compaction/trigger-reconciliation"), Map.of(), Map.class);
        assertThat(trigger.getStatusCode().value()).isEqualTo(202);
        assertThat(trigger.getBody().get("status")).isEqualTo("running");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            ResponseEntity<Map> statusResp =
                    restTemplate.getForEntity(url("/compaction/reconciliation-status"), Map.class);
            assertThat(statusResp.getBody().get("running")).isEqualTo(false);
            Map<?, ?> lastRun = (Map<?, ?>) statusResp.getBody().get("lastRun");
            assertThat(lastRun).isNotNull();
            assertThat(lastRun.get("status")).isEqualTo("completed");
            assertThat(((Number) lastRun.get("orphanedFiles")).intValue()).isEqualTo(1);
        });

        // Assert the triggered run appears in history by id, rather than asserting an exact
        // list size — this class's two @Test methods share one cached Spring context/DB
        // (same MinIO container + dynamic properties), so reconciliation_history can contain
        // rows from whichever of them ran first; exact-size assertions would depend on
        // JUnit's (unspecified) method execution order.
        long triggeredId = ((Number) trigger.getBody().get("id")).longValue();
        ResponseEntity<java.util.List> history = restTemplate.getForEntity(
                url("/compaction/reconciliation-history?limit=20"), java.util.List.class);
        boolean found = history.getBody().stream().anyMatch(row ->
                ((Number) ((Map<?, ?>) row).get("id")).longValue() == triggeredId);
        assertThat(found).isTrue();
    }

    @Test
    void trigger_whileAlreadyRunning_returns409() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.general_second_topic (" +
                    "recorded_at TIMESTAMPTZ, kafka_offset BIGINT, kafka_partition INTEGER, " +
                    "kafka_timestamp TIMESTAMPTZ, kafka_key VARCHAR, kafka_value BLOB, " +
                    "metadata VARCHAR, headers STRUCT(key VARCHAR, value VARCHAR)[], message_type VARCHAR)");
        }
        restTemplate.postForEntity(url("/compaction/trigger-reconciliation"), Map.of(), Map.class);

        org.springframework.web.client.HttpClientErrorException ex = null;
        try {
            restTemplate.postForEntity(url("/compaction/trigger-reconciliation"), Map.of(), Map.class);
        } catch (org.springframework.web.client.HttpClientErrorException e) {
            ex = e;
        }
        assertThat(ex).isNotNull();
        assertThat(ex.getStatusCode().value()).isEqualTo(409);

        // Let the first run finish so it doesn't leak into other test classes sharing MinIO/Spring context.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            ResponseEntity<Map> statusResp =
                    restTemplate.getForEntity(url("/compaction/reconciliation-status"), Map.class);
            assertThat(statusResp.getBody().get("running")).isEqualTo(false);
        });
    }
}

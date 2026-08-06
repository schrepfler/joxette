package com.joxette.it;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class HealthFlushedDataSizeIT {

    private static final String BUCKET = "joxette-flushed-bytes-test";

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

    @Test
    void health_reportsFlushedDataSizeBytes_summedAcrossFlushedCassetteTables() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.general_orders_events (" +
                    "recorded_at TIMESTAMPTZ, kafka_offset BIGINT, kafka_partition INTEGER, " +
                    "kafka_timestamp TIMESTAMPTZ, kafka_key VARCHAR, kafka_value BLOB, " +
                    "metadata VARCHAR, headers STRUCT(key VARCHAR, value VARCHAR)[], message_type VARCHAR)");
            st.execute("INSERT INTO lake.main.general_orders_events (recorded_at, kafka_offset, " +
                    "kafka_partition, kafka_timestamp, kafka_key, kafka_value, headers) " +
                    "VALUES (now(), 0, 0, now(), 'k', 'v', [])");
            st.execute("CALL ducklake_flush_inlined_data('lake')");
            st.execute("CHECKPOINT");
        }

        Map<String, Object> health = restTemplate.getForObject(
                "http://localhost:" + port + "/v1/health", Map.class);

        assertThat(((Number) health.get("flushedDataSizeBytes")).longValue()).isGreaterThan(0);
    }
}

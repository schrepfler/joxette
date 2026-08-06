package com.joxette.it;

import com.joxette.reconciliation.ReconciliationService;
import com.joxette.compaction.RunStatus;
import com.joxette.compaction.TriggerSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
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
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class ReconciliationRecoveryIT {

    private static final String BUCKET = "joxette-recon-recovery-test";

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

    @Autowired private Connection duckDB;
    @Autowired private ReconciliationService reconciliationService;

    @Test
    void reconciliation_recoverOrphanedFiles_registersFileAndMakesRowsQueryable() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.general_orders_events (" +
                    "recorded_at TIMESTAMPTZ, kafka_offset BIGINT, kafka_partition INTEGER, " +
                    "kafka_timestamp TIMESTAMPTZ, kafka_key VARCHAR, kafka_value BLOB, " +
                    "metadata VARCHAR, headers STRUCT(key VARCHAR, value VARCHAR)[], message_type VARCHAR)");
            st.execute("COPY (SELECT now()::TIMESTAMPTZ AS recorded_at, 0::BIGINT AS kafka_offset, " +
                    "0::INTEGER AS kafka_partition, now()::TIMESTAMPTZ AS kafka_timestamp, " +
                    "'k'::VARCHAR AS kafka_key, 'v'::BLOB AS kafka_value, NULL::VARCHAR AS metadata, " +
                    "[]::STRUCT(key VARCHAR, value VARCHAR)[] AS headers, NULL::VARCHAR AS message_type) " +
                    "TO 's3://" + BUCKET + "/data/main/general_orders_events/stray.parquet' (FORMAT PARQUET)");
        }

        var run = reconciliationService.beginRun(TriggerSource.MANUAL, null, true);
        reconciliationService.executeRun(run.id(), null, true);

        var status = reconciliationService.getStatus();
        assertThat(status.lastRun().status()).isEqualTo(RunStatus.COMPLETED);
        assertThat(status.lastRun().recoveredFiles()).isEqualTo(1);

        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM lake.main.general_orders_events")) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(1L);
        }

        // A second run should find nothing left to recover.
        var second = reconciliationService.beginRun(TriggerSource.MANUAL, null, true);
        reconciliationService.executeRun(second.id(), null, true);
        assertThat(reconciliationService.getStatus().lastRun().orphanedFiles()).isZero();
    }
}

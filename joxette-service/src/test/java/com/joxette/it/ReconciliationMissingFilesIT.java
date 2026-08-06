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
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class ReconciliationMissingFilesIT {

    private static final String BUCKET = "joxette-recon-missing-test";

    @Container
    static final MinIOContainer minio =
            new MinIOContainer(DockerImageName.parse("minio/minio:RELEASE.2024-01-16T16-07-38Z"));

    private static String s3Url;
    private static String userName;
    private static String password;

    @DynamicPropertySource
    static void minioProperties(DynamicPropertyRegistry registry) {
        s3Url = minio.getS3URL();
        userName = minio.getUserName();
        password = minio.getPassword();
        try (S3Client s3 = s3Client()) {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
        registry.add("joxette.catalog.object-storage-path", () -> "s3://" + BUCKET + "/data/");
        registry.add("joxette.s3.endpoint",   () -> s3Url);
        registry.add("joxette.s3.access-key", () -> userName);
        registry.add("joxette.s3.secret-key", () -> password);
    }

    private static S3Client s3Client() {
        return S3Client.builder()
                .endpointOverride(URI.create(s3Url))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(userName, password)))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
    }

    @Autowired private Connection duckDB;
    @Autowired private ReconciliationService reconciliationService;

    @Test
    void reconciliation_detectsAFileDeletedOutOfBand() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.general_audit_log (" +
                    "recorded_at TIMESTAMPTZ, kafka_offset BIGINT, kafka_partition INTEGER, " +
                    "kafka_timestamp TIMESTAMPTZ, kafka_key VARCHAR, kafka_value BLOB, " +
                    "metadata VARCHAR, headers STRUCT(key VARCHAR, value VARCHAR)[], message_type VARCHAR)");
            st.execute("INSERT INTO lake.main.general_audit_log (recorded_at, kafka_offset, " +
                    "kafka_partition, kafka_timestamp, kafka_key, kafka_value, headers) " +
                    "VALUES (now(), 0, 0, now(), 'k', 'v', [])");
            st.execute("CALL ducklake_flush_inlined_data('lake')");
            st.execute("CHECKPOINT");
        }

        String trackedKey;
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT data_file FROM ducklake_list_files('lake', 'general_audit_log')")) {
            rs.next();
            String dataFile = rs.getString(1); // s3://bucket/data/main/general_audit_log/ducklake-...parquet
            trackedKey = dataFile.substring(("s3://" + BUCKET + "/").length());
        }

        // Delete the tracked file directly from the bucket — out-of-band, not via DuckLake.
        try (S3Client s3 = s3Client()) {
            s3.deleteObject(DeleteObjectRequest.builder().bucket(BUCKET).key(trackedKey).build());
        }

        var run = reconciliationService.beginRun(TriggerSource.MANUAL, null, false);
        reconciliationService.executeRun(run.id(), null, false);

        var status = reconciliationService.getStatus();
        assertThat(status.lastRun().status()).isEqualTo(RunStatus.COMPLETED);
        assertThat(status.lastRun().missingFiles()).isEqualTo(1);
        assertThat(status.lastRun().missingBytes()).isGreaterThan(0);
    }
}

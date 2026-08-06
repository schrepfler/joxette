package com.joxette.it;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.joxette.reconciliation.ReconciliationService;
import com.joxette.compaction.RunStatus;
import com.joxette.compaction.TriggerSource;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
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
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("it")
@Testcontainers
class ReconciliationOrphanedFilesIT {

    private static final String BUCKET = "joxette-recon-test";

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
    void reconciliation_detectsAndSizesAStrayOrphanedFile() throws Exception {
        try (Statement st = duckDB.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS lake.main.general_orders_events (" +
                    "recorded_at TIMESTAMPTZ, kafka_offset BIGINT, kafka_partition INTEGER, " +
                    "kafka_timestamp TIMESTAMPTZ, kafka_key VARCHAR, kafka_value BLOB, " +
                    "metadata VARCHAR, headers STRUCT(key VARCHAR, value VARCHAR)[], message_type VARCHAR)");
            // A stray file dropped directly into the table's default DuckLake path,
            // never registered via any catalog write — simulates external drift.
            st.execute("COPY (SELECT 1 AS id) TO " +
                    "'s3://" + BUCKET + "/data/main/general_orders_events/stray.parquet' (FORMAT PARQUET)");
        }

        // Diagnostic logging exists so a hung run's last log line pinpoints which phase/call
        // was in flight (motivated by a real production incident: a stuck object-storage call
        // froze the whole app and didn't even respond to shutdown interruption). This is the
        // only test exercising a real, successful ducklake_delete_orphaned_files call, so it's
        // the only place that can verify the "scan complete" line actually fires with real counts.
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ReconciliationService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level savedLevel = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);

        var run = reconciliationService.beginRun(TriggerSource.MANUAL, null, false);
        reconciliationService.executeRun(run.id(), null, false);

        List<String> messages;
        try {
            messages = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(savedLevel);
        }

        var status = reconciliationService.getStatus();
        assertThat(status.lastRun().status()).isEqualTo(RunStatus.COMPLETED);
        assertThat(status.lastRun().orphanedFiles()).isEqualTo(1);
        assertThat(status.lastRun().orphanedBytes()).isGreaterThan(0);

        assertThat(messages).anyMatch(m -> m.contains("calling ducklake_delete_orphaned_files"));
        assertThat(messages).anyMatch(m ->
                m.contains("ducklake_delete_orphaned_files returned 1 candidate path(s)"));
        assertThat(messages).anyMatch(m -> m.contains("orphaned-file scan complete") && m.contains("1 found"));
    }
}

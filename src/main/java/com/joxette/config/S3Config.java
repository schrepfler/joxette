package com.joxette.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;

/**
 * Creates an {@link S3Client} bean when {@code joxette.object-store.bucket} is configured.
 *
 * <p>When {@code endpoint-url} is set the client is redirected to that URL (MinIO, LocalStack,
 * or any S3-compatible service). Credentials fall back to the SDK default provider chain
 * (IAM role, env vars, {@code ~/.aws/credentials}) when {@code access-key}/{@code secret-key}
 * are not set.
 */
@Configuration
@ConditionalOnProperty(prefix = "joxette.object-store", name = "bucket")
public class S3Config {

    @Bean
    public S3Client s3Client(JoxetteProperties properties) {
        var os = properties.getObjectStore();

        var builder = S3Client.builder()
                .region(Region.of(os.getRegion()))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(os.isForcePathStyle())
                        .build());

        if (os.getAccessKey() != null && os.getSecretKey() != null) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(os.getAccessKey(), os.getSecretKey())));
        }

        if (os.getEndpointUrl() != null) {
            builder.endpointOverride(URI.create(os.getEndpointUrl()));
        }

        return builder.build();
    }
}

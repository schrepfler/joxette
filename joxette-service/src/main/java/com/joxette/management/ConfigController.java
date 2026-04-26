package com.joxette.management;

import com.joxette.config.JoxetteProperties;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;

/**
 * Exposes the effective runtime configuration of the Joxette service.
 *
 * <pre>
 * GET /config/runtime  – immutable deployment config + live domain summary
 * </pre>
 */
@Tag(name = "Configuration",
     description = "Effective runtime configuration and live domain summary for operator diagnostics.")
@RestController
@RequestMapping("/config")
public class ConfigController {

    private final JoxetteProperties properties;
    private final ConfigRepository config;

    public ConfigController(JoxetteProperties properties, ConfigRepository config) {
        this.properties = properties;
        this.config     = config;
    }

    @Operation(
        operationId = "getRuntimeConfig",
        summary = "Effective runtime configuration",
        description = "Returns the immutable deployment configuration (read from YAML at startup) " +
                      "and a live domain summary (topic/entity counts from config tables). " +
                      "Designed to help operators answer 'why isn't X being recorded' without " +
                      "needing access to YAML files or the server filesystem."
    )
    @ApiResponse(responseCode = "200", description = "Effective runtime configuration",
        content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
            schema = @Schema(implementation = RuntimeConfigResponse.class)))
    @GetMapping(value = "/runtime", produces = MediaType.APPLICATION_JSON_VALUE)
    public RuntimeConfigResponse getRuntime() throws SQLException {
        JoxetteProperties.Compaction comp = properties.getCompaction();
        JoxetteProperties.S3 s3 = properties.getS3();

        var deployment = new RuntimeConfigResponse.DeploymentConfig(
                properties.getKafka().getBootstrapServers(),
                properties.getCatalog().getPath(),
                properties.getCatalog().getObjectStoragePath(),
                s3.getEndpoint(),
                s3.getRegion(),
                properties.getInline().getThresholdMb(),
                properties.getInline().getThresholdRecords(),
                comp.getSchedule(),
                properties.getRetention().getSchedule(),
                properties.getRecording().getBatchSize(),
                properties.getRecording().getBatchTimeoutMs(),
                comp.getEntity().getMinFilesPerBucket(),
                comp.getEntity().getTargetFileSizeMb(),
                comp.getEntity().getLookbackDays(),
                comp.getGeneral().isEnabled(),
                comp.getGeneral().getMinFilesPerPartition(),
                comp.getGeneral().getTargetFileSizeMb());

        var domain = new RuntimeConfigResponse.DomainSummary(
                config.countTopics(),
                config.countEntityTypes(),
                config.countSourceMappings());

        return new RuntimeConfigResponse(deployment, domain);
    }
}

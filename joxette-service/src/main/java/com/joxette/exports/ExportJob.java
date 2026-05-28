package com.joxette.exports;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.List;

/**
 * Represents one async export job — immutable snapshot returned by the REST API.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ExportJob(
        String            id,
        String            entityType,
        List<String>      entityIds,
        Instant           from,
        Instant           to,
        List<String>      messageTypes,
        ExportOutputFormat outputFormat,
        ExportStatus      status,
        String            outputPath,
        Long              rowCount,
        String            errorMessage,
        Instant           createdAt,
        Instant           startedAt,
        Instant           completedAt
) {}

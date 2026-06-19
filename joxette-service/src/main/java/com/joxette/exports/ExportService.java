package com.joxette.exports;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.ResourceNotFoundException;
import com.joxette.api.error.ValidationException;
import com.joxette.config.JoxetteProperties;
import com.joxette.lifecycle.BackgroundTaskRegistry;
import com.joxette.replay.EntityRecord;
import com.joxette.replay.EntityReplayService;
import com.joxette.replay.Order;
import com.joxette.replay.transform.TransformPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Manages async export jobs.
 *
 * <p>Each job is persisted in the {@code export_jobs} DuckDB table and executed
 * on a dedicated virtual thread.  For Parquet exports the records are first
 * collected into a temporary DuckDB table and then written via
 * {@code COPY … TO … (FORMAT PARQUET)}.  For NDJSON exports the records are
 * streamed directly to the configured output path via DuckDB's
 * {@code COPY … TO … (FORMAT JSON)}.
 */
@Service
public class ExportService {

    private static final Logger log = LoggerFactory.getLogger(ExportService.class);

    static final int MAX_ENTITY_IDS = 10_000;

    private final ExportJobRepository    repository;
    private final EntityReplayService    entityReplayService;
    private final Connection             duckDB;
    private final JoxetteProperties      properties;
    private final ObjectMapper           objectMapper;
    private final BackgroundTaskRegistry taskRegistry;

    public ExportService(ExportJobRepository repository,
                         EntityReplayService entityReplayService,
                         Connection duckDB,
                         JoxetteProperties properties,
                         ObjectMapper objectMapper,
                         BackgroundTaskRegistry taskRegistry) {
        this.repository          = repository;
        this.entityReplayService = entityReplayService;
        this.duckDB              = duckDB;
        this.properties          = properties;
        this.objectMapper        = objectMapper;
        this.taskRegistry        = taskRegistry;
    }

    /**
     * Creates a new export job and submits it for async execution.
     *
     * @throws ValidationException if the request is invalid
     */
    public ExportJob submit(String entityType, List<String> entityIds,
                            Instant from, Instant to, List<String> messageTypes,
                            ExportOutputFormat outputFormat) {
        if (entityIds == null || entityIds.isEmpty()) {
            throw new ValidationException("entityIds must be a non-empty list");
        }
        if (entityIds.size() > MAX_ENTITY_IDS) {
            throw new ValidationException(
                    "entityIds list exceeds the maximum of " + MAX_ENTITY_IDS + " entries");
        }

        ExportJob job = repository.create(entityType, entityIds, from, to, messageTypes, outputFormat);
        taskRegistry.submit("export-" + job.id(),
                () -> execute(job.id(), entityType, entityIds, from, to, messageTypes, outputFormat));
        return job;
    }

    public List<ExportJob> list(String entityType) {
        if (entityType != null) {
            return repository.listByEntityType(entityType);
        }
        return repository.listAll();
    }

    public ExportJob get(String id) {
        return repository.findById(id)
                .orElseThrow(() -> ResourceNotFoundException.exportJob(id));
    }

    /** Deletes a job record. Only allowed for completed or failed jobs. */
    public void delete(String id) {
        ExportJob job = get(id);
        if (job.status() == ExportStatus.RUNNING || job.status() == ExportStatus.PENDING) {
            throw new ValidationException(
                    "Cannot delete an export job in status: " + job.status().getValue());
        }
        repository.delete(id);
    }

    // -------------------------------------------------------------------------

    private static final long EXPORT_RETRY_INITIAL_MS  = 2_000;
    private static final double EXPORT_RETRY_MULTIPLIER = 2.0;
    private static final long EXPORT_RETRY_MAX_MS      = 60_000;
    private static final int  EXPORT_RETRY_MAX_ATTEMPTS = 5;

    private void execute(String jobId, String entityType, List<String> entityIds,
                         Instant from, Instant to, List<String> messageTypes,
                         ExportOutputFormat outputFormat) {
        repository.markRunning(jobId);
        String outputPath = resolveOutputPath(jobId, outputFormat);
        long delayMs = EXPORT_RETRY_INITIAL_MS;
        int attempt  = 0;
        while (true) {
            try {
                long rowCount = switch (outputFormat) {
                    case PARQUET -> exportParquet(jobId, entityType, entityIds, from, to, messageTypes, outputPath);
                    case NDJSON  -> exportNdjson(jobId, entityType, entityIds, from, to, messageTypes, outputPath);
                };
                repository.markCompleted(jobId, outputPath, rowCount);
                if (attempt > 0) {
                    log.info("Export job {} completed after {} retries: {} rows → {}", jobId, attempt, rowCount, outputPath);
                } else {
                    log.info("Export job {} completed: {} rows → {}", jobId, rowCount, outputPath);
                }
                return;
            } catch (Exception e) {
                if (!isTransientStorageError(e) || ++attempt >= EXPORT_RETRY_MAX_ATTEMPTS) {
                    String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
                    repository.markFailed(jobId, msg);
                    log.error("Export job {} failed after {} attempt(s)", jobId, attempt, e);
                    return;
                }
                log.warn("Export job {} transient failure (attempt {}/{}), retrying in {} ms: {}",
                        jobId, attempt, EXPORT_RETRY_MAX_ATTEMPTS, delayMs, e.getMessage());
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    repository.markFailed(jobId, "Interrupted during retry");
                    return;
                }
                delayMs = Math.min((long) (delayMs * EXPORT_RETRY_MULTIPLIER), EXPORT_RETRY_MAX_MS);
            }
        }
    }

    private static boolean isTransientStorageError(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof java.sql.SQLException) {
                String msg = cur.getMessage();
                if (msg != null && (msg.contains("IO Error") || msg.contains("HTTP PUT")
                        || msg.contains("HTTP GET") || msg.contains("Could not connect")
                        || msg.contains("Connection refused") || msg.contains("Connection timed out"))) {
                    return true;
                }
            }
            cur = cur.getCause();
        }
        return false;
    }

    private long exportParquet(String jobId, String entityType, List<String> entityIds,
                               Instant from, Instant to, List<String> messageTypes,
                               String outputPath) throws Exception {
        List<EntityRecord> records = loadAll(entityType, entityIds, from, to, messageTypes);
        if (records.isEmpty()) {
            return 0L;
        }

        String tmpTable = "export_tmp_" + jobId.replace("-", "_");
        try (Statement st = duckDB.createStatement()) {
            st.execute("""
                    CREATE TEMP TABLE %s (
                        entity_id       VARCHAR,
                        message_type    VARCHAR,
                        topic           VARCHAR,
                        kafka_partition INTEGER,
                        kafka_offset    BIGINT,
                        kafka_timestamp TIMESTAMPTZ,
                        recorded_at     TIMESTAMPTZ,
                        kafka_key       VARCHAR,
                        kafka_value_b64 VARCHAR
                    )""".formatted(tmpTable));

            try (PreparedStatement ps = duckDB.prepareStatement(
                    "INSERT INTO " + tmpTable + " VALUES (?,?,?,?,?,?,?,?,?)")) {
                for (EntityRecord r : records) {
                    ps.setString(1, r.entityId());
                    ps.setString(2, r.messageType());
                    ps.setString(3, r.topic());
                    ps.setInt   (4, r.partition());
                    ps.setLong  (5, r.offset());
                    ps.setObject(6, r.timestamp() != null
                            ? java.sql.Timestamp.from(r.timestamp()) : null);
                    ps.setObject(7, r.recordedAt() != null
                            ? java.sql.Timestamp.from(r.recordedAt()) : null);
                    ps.setString(8, r.key());
                    ps.setString(9, r.value());
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            st.execute("COPY " + tmpTable + " TO '" + outputPath + "' (FORMAT PARQUET)");
            st.execute("DROP TABLE IF EXISTS " + tmpTable);
        }
        return records.size();
    }

    private long exportNdjson(String jobId, String entityType, List<String> entityIds,
                              Instant from, Instant to, List<String> messageTypes,
                              String outputPath) throws Exception {
        List<EntityRecord> records = loadAll(entityType, entityIds, from, to, messageTypes);
        if (records.isEmpty()) {
            return 0L;
        }
        // Write NDJSON to a local/S3 path by building the content and writing via DuckDB.
        // For local paths write through a DuckDB COPY; for remote paths use the same COPY
        // path since DuckDB httpfs handles s3:// writes transparently.
        String tmpTable = "export_tmp_ndjson_" + jobId.replace("-", "_");
        try (Statement st = duckDB.createStatement()) {
            st.execute("""
                    CREATE TEMP TABLE %s (line VARCHAR)
                    """.formatted(tmpTable));

            try (PreparedStatement ps = duckDB.prepareStatement(
                    "INSERT INTO " + tmpTable + " VALUES (?)")) {
                for (EntityRecord r : records) {
                    ps.setString(1, objectMapper.writeValueAsString(r));
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            // DuckDB COPY … TO with FORMAT CSV and no separator/header gives one line per row.
            st.execute("COPY (SELECT line FROM " + tmpTable + ") TO '" + outputPath
                       + "' (FORMAT CSV, HEADER false, QUOTE '', DELIMITER '')");
            st.execute("DROP TABLE IF EXISTS " + tmpTable);
        }
        return records.size();
    }

    private List<EntityRecord> loadAll(String entityType, List<String> entityIds,
                                       Instant from, Instant to,
                                       List<String> messageTypes) throws Exception {
        List<EntityRecord> all = new ArrayList<>();
        for (String entityId : entityIds) {
            entityReplayService.streamEntityEvents(
                    entityType, entityId, from, to,
                    all::add,
                    TransformPipeline.IDENTITY, "",
                    Order.ASC, null, null, messageTypes);
        }
        return all;
    }

    private String resolveOutputPath(String jobId, ExportOutputFormat format) {
        String base = properties.getCatalog().getObjectStoragePath();
        if (base == null || base.isBlank()) {
            base = "exports";
        }
        if (!base.endsWith("/")) {
            base = base + "/";
        }
        String ext = format == ExportOutputFormat.PARQUET ? ".parquet" : ".ndjson";
        return base + "exports/" + jobId + ext;
    }
}

package com.joxette.exports;

import com.joxette.config.JoxetteProperties;
import com.joxette.replay.EntityReplayService;
import com.joxette.replay.MessageRouter;
import com.joxette.support.DuckDBTestSupport;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link ExportService#exportNdjson} directly (package-private for
 * exactly this reason). No prior test called it directly -- this is new
 * coverage, not a pre-existing regression net.
 */
class ExportServiceNdjsonTest {

    private static final String ENTITY_TYPE = "ndjsontest";

    private Connection conn;
    private ExportService service;
    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        conn = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createEntityTable(conn, ENTITY_TYPE);
        EntityReplayService entityReplayService =
                new EntityReplayService(DSL.using(conn, SQLDialect.DUCKDB), conn, new JoxetteProperties());
        ObjectMapper objectMapper = new ObjectMapper();
        // repository/properties/taskRegistry are never touched by exportNdjson itself
        // (only by submit()/execute(), which this test bypasses) -- null is safe here.
        service = new ExportService(null, entityReplayService, conn, null, objectMapper, null);
        tempDir = Files.createTempDirectory("export-ndjson-test");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) conn.close();
        if (tempDir != null) {
            Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> p.toFile().delete());
        }
    }

    @Test
    void exportsAllRecordsAcrossMultipleEntities() throws Exception {
        DuckDBTestSupport.insertEntityRow(conn, ENTITY_TYPE, "entity-1", bucketOf("entity-1"), "created",
                "orders.events", 0, 0L, Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T00:00:00.100Z"), "key-1",
                "{\"amount\":10}".getBytes(StandardCharsets.UTF_8));
        DuckDBTestSupport.insertEntityRow(conn, ENTITY_TYPE, "entity-1", bucketOf("entity-1"), "updated",
                "orders.events", 0, 1L, Instant.parse("2026-01-01T00:01:00Z"),
                Instant.parse("2026-01-01T00:01:00.100Z"), "key-1",
                "{\"amount\":20}".getBytes(StandardCharsets.UTF_8));
        DuckDBTestSupport.insertEntityRow(conn, ENTITY_TYPE, "entity-2", bucketOf("entity-2"), "created",
                "orders.events", 0, 2L, Instant.parse("2026-01-01T00:02:00Z"),
                Instant.parse("2026-01-01T00:02:00.100Z"), "key-2",
                "{\"amount\":30}".getBytes(StandardCharsets.UTF_8));

        String outputPath = tempDir.resolve("out.ndjson").toString();
        long count = service.exportNdjson("job-1", ENTITY_TYPE, List.of("entity-1", "entity-2"),
                null, null, null, outputPath);

        assertThat(count).isEqualTo(3L);

        List<String> lines = Files.readAllLines(java.nio.file.Path.of(outputPath));
        assertThat(lines).hasSize(3);

        ObjectMapper mapper = new ObjectMapper();
        List<String> entityIdsSeen = lines.stream().map(line -> {
            JsonNode node = mapper.readTree(line);
            assertThat(node.get("topic").asText()).isEqualTo("orders.events");
            return node.get("entityId").asText();
        }).toList();
        assertThat(entityIdsSeen).containsExactly("entity-1", "entity-1", "entity-2");
    }

    @Test
    void returnsZeroAndWritesNoFileWhenNoRecordsMatch() throws Exception {
        String outputPath = tempDir.resolve("empty.ndjson").toString();
        long count = service.exportNdjson("job-2", ENTITY_TYPE, List.of("no-such-entity"),
                null, null, null, outputPath);

        assertThat(count).isEqualTo(0L);
        assertThat(Files.exists(java.nio.file.Path.of(outputPath))).isFalse();
    }

    /**
     * Entity tables are {@code SET PARTITIONED BY (bucket)} and every per-entity read
     * prunes to that partition, so test rows must be written at the bucket the recorder
     * would have chosen. Derived, never hard-coded.
     */
    private static int bucketOf(String entityId) {
        return MessageRouter.computeBucket(ENTITY_TYPE, entityId, 256);
    }
}

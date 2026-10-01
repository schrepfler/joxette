package com.joxette.exports;

import com.joxette.config.JoxetteProperties;
import com.joxette.replay.EntityReplayService;
import com.joxette.replay.MessageRouter;
import com.joxette.support.DuckDBTestSupport;
import com.joxette.support.SqlRecordingConnection;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises {@link ExportService#exportParquet} directly (package-private for this reason). */
class ExportServiceParquetTest {

    private static final String ENTITY_TYPE = "parquettest";

    private Connection conn;
    private SqlRecordingConnection shared;
    private ExportService service;
    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        conn = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createEntityTable(conn, ENTITY_TYPE);
        EntityReplayService entityReplayService =
                new EntityReplayService(DSL.using(conn, SQLDialect.DUCKDB), conn, new JoxetteProperties());
        shared = SqlRecordingConnection.wrap(conn);
        // repository/properties/taskRegistry are only used by submit()/execute(), bypassed here.
        service = new ExportService(null, entityReplayService, shared.connection(), null, new ObjectMapper(), null);
        tempDir = Files.createTempDirectory("export-parquet-test");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) conn.close();
        if (tempDir != null) {
            Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> p.toFile().delete());
        }
    }

    /**
     * The export's temp table, batch INSERT and COPY (which may write to s3://) used to run
     * on the shared connection without its lock — racing every other statement on the one
     * native handle, and, for a remote path, tying it up for the length of the upload.
     */
    @Test
    void writesEveryRecord_withoutUsingTheSharedConnection() throws Exception {
        for (int i = 0; i < 3; i++) {
            String id = "entity-" + (i % 2);
            DuckDBTestSupport.insertEntityRow(conn, ENTITY_TYPE, id, bucketOf(id), "created",
                    "orders.events", 0, i, Instant.parse("2026-01-01T00:00:00Z").plusSeconds(i),
                    Instant.parse("2026-01-01T00:00:01Z").plusSeconds(i), "key-" + i,
                    ("{\"n\":" + i + "}").getBytes(StandardCharsets.UTF_8));
        }
        String outputPath = tempDir.resolve("out.parquet").toString();

        long count = service.exportParquet("job-1", ENTITY_TYPE, List.of("entity-0", "entity-1"),
                null, null, null, outputPath);

        assertThat(count).isEqualTo(3L);
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM read_parquet('" + outputPath + "')")) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(3L);
        }
        assertThat(shared.sql()).as("SQL issued on the shared connection")
                .noneMatch(sql -> sql.contains("export_tmp_"));
    }

    private static int bucketOf(String entityId) {
        return MessageRouter.computeBucket(ENTITY_TYPE, entityId, 256);
    }
}

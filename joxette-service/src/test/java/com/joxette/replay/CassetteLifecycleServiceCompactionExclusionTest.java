package com.joxette.replay;

import com.joxette.api.error.ConflictException;
import com.joxette.compaction.CompactionLockManager;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.recording.RecordingCoordinator;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.ObjectMapper;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Compaction runs on its own DuckDB connection, so {@code synchronized(duckDB)} no longer
 * keeps it apart from operations that rewrite a whole cassette table. These operations
 * must instead take the same per-target lock compaction uses, and refuse (409) rather
 * than race an in-flight merge.
 */
class CassetteLifecycleServiceCompactionExclusionTest {

    private static final String ENTITY_TYPE = "order";
    private static final String TOPIC = "seed-topic";

    @TempDir
    Path tempDir;

    private Connection duckDB;
    private RecordingCoordinator recordingCoordinator;
    private CompactionLockManager lockManager;
    private CassetteLifecycleService service;

    @BeforeEach
    void setUp() throws SQLException {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createEntityTable(duckDB, ENTITY_TYPE);
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC);
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 0L,
                Instant.parse("2024-01-01T00:00:00Z"), Instant.now(), "k1", "v1".getBytes());

        JoxetteProperties properties = new JoxetteProperties();
        properties.getCatalog().setPath(tempDir.resolve("joxette.ducklake").toString());

        recordingCoordinator = mock(RecordingCoordinator.class);
        when(recordingCoordinator.activeTopics()).thenReturn(Set.of(TOPIC));
        lockManager = new CompactionLockManager(duckDB, properties, DuckDBTestSupport.newInstanceRegistry(duckDB));

        service = new CassetteLifecycleService(duckDB, properties, new ConfigRepository(duckDB, properties),
                Optional.empty(), recordingCoordinator, new ObjectMapper(), lockManager);
    }

    @AfterEach
    void tearDown() throws SQLException {
        duckDB.close();
    }

    @Test
    void truncateEntityCassette_isRefusedWhileCompactionHoldsTheEntityType() throws Exception {
        holdWhile(CompactionLockManager.targetForEntityType(ENTITY_TYPE), () ->
                assertThatThrownBy(() -> service.truncateEntityCassette(ENTITY_TYPE))
                        .isInstanceOf(ConflictException.class));
    }

    @Test
    void deleteEntityFromCassette_isRefusedWhileCompactionHoldsTheEntityType() throws Exception {
        holdWhile(CompactionLockManager.targetForEntityType(ENTITY_TYPE), () ->
                assertThatThrownBy(() -> service.deleteEntityFromCassette(ENTITY_TYPE, "e1"))
                        .isInstanceOf(ConflictException.class));
    }

    @Test
    void truncateTopicCassette_isRefusedWhileCompactionHoldsTheTopic() throws Exception {
        holdWhile(CompactionLockManager.targetForLakeTable("general_seed_topic").orElseThrow(), () ->
                assertThatThrownBy(() -> service.truncateTopicCassette(TOPIC))
                        .isInstanceOf(ConflictException.class));
    }

    @Test
    void restoreSnapshot_isRefusedBeforePausingRecordersWhileAnyTableIsBeingCompacted() throws Exception {
        service.createSnapshot("snap");

        holdWhile(CompactionLockManager.targetForEntityType(ENTITY_TYPE), () ->
                assertThatThrownBy(() -> service.restoreSnapshot("snap"))
                        .isInstanceOf(ConflictException.class));

        verify(recordingCoordinator, never()).stopAll();
    }

    @Test
    void operationsReleaseTheirLocksWhenDone() throws Exception {
        service.truncateEntityCassette(ENTITY_TYPE);
        service.deleteEntityFromCassette(ENTITY_TYPE, "e1");
        service.truncateTopicCassette(TOPIC);
        service.createSnapshot("snap");
        service.restoreSnapshot("snap");

        for (String target : new String[]{
                CompactionLockManager.targetForEntityType(ENTITY_TYPE),
                CompactionLockManager.targetForLakeTable("general_seed_topic").orElseThrow()}) {
            assertThat(lockManager.tryAcquireExclusive(target)).as(target).isTrue();
            lockManager.releaseExclusive(target);
        }
    }

    private void holdWhile(String target, ThrowingRunnable body) throws Exception {
        assertThat(lockManager.tryAcquireExclusive(target)).isTrue();
        try {
            body.run();
        } finally {
            lockManager.releaseExclusive(target);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}

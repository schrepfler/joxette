package com.joxette.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.SnapshotVerificationException;
import com.joxette.config.JoxetteProperties;
import com.joxette.management.ConfigRepository;
import com.joxette.recording.RecordingCoordinator;
import com.joxette.support.DuckDBTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the recorder pause/resume exception-safety of
 * {@link CassetteLifecycleService#restoreSnapshot(String)}.
 *
 * <p>Covers the two failure modes identified in review of commit {@code 6bf8884}:
 * <ol>
 *   <li>{@link RecordingCoordinator#stopAll()} throwing must not skip the resume
 *       of already-paused topics — the pause set is captured before any stop
 *       attempt, and every topic in it is resumed in the {@code finally} block
 *       regardless of what happens in the {@code try}.</li>
 *   <li>A single {@link RecordingCoordinator#restartTopic(String)} failure during
 *       resume must not (a) skip resuming the remaining topics or (b) mask an
 *       exception the {@code try} block was already propagating (e.g. a
 *       {@link SnapshotVerificationException}).</li>
 * </ol>
 *
 * <p>Uses a real in-memory DuckDB connection (via {@link DuckDBTestSupport}) so
 * {@code createSnapshot}/{@code restoreSnapshot} exercise real {@code EXPORT
 * DATABASE}/{@code IMPORT DATABASE} SQL, but mocks {@link RecordingCoordinator}
 * (a plain Spring-managed class, not an actor) to inject per-topic failures.
 */
class CassetteLifecycleServiceRestoreResilienceTest {

    private static final String SNAPSHOT_NAME = "resilience-snap";
    private static final Set<String> PAUSED_TOPICS = Set.of("topic-a", "topic-b", "topic-c");

    @TempDir
    Path tempDir;

    private Connection duckDB;
    private RecordingCoordinator recordingCoordinator;
    private CassetteLifecycleService service;

    @BeforeEach
    void setUp() throws SQLException {
        duckDB = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, "seed-topic");
        DuckDBTestSupport.insertCassetteRow(duckDB, "seed-topic", 0, 0L,
                Instant.parse("2024-01-01T00:00:00Z"), Instant.now(), "k1", "v1".getBytes());

        JoxetteProperties properties = new JoxetteProperties();
        properties.getCatalog().setPath(tempDir.resolve("joxette.ducklake").toString());

        ConfigRepository configRepo = new ConfigRepository(duckDB, properties);
        recordingCoordinator = mock(RecordingCoordinator.class);
        when(recordingCoordinator.activeTopics()).thenReturn(PAUSED_TOPICS);

        service = new CassetteLifecycleService(
                duckDB, properties, configRepo, Optional.empty(),
                recordingCoordinator, new ObjectMapper());

        service.createSnapshot(SNAPSHOT_NAME);
    }

    @AfterEach
    void tearDown() throws SQLException {
        duckDB.close();
    }

    @Test
    void restoreSnapshot_whenStopAllThrows_stillResumesEveryPausedTopic() throws SQLException {
        // Regression test for the original bug: stopAll() was called BEFORE the
        // try block, so a throw here meant the finally-resume block never ran at all.
        doThrow(new RuntimeException("stopAll boom")).when(recordingCoordinator).stopAll();

        assertThatThrownBy(() -> service.restoreSnapshot(SNAPSHOT_NAME))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("stopAll boom");

        for (String topic : PAUSED_TOPICS) {
            verify(recordingCoordinator, times(1)).restartTopic(topic);
        }
    }

    @Test
    void restoreSnapshot_whenOneTopicFailsToResume_othersStillResume() throws SQLException {
        doThrow(new RuntimeException("resume boom")).when(recordingCoordinator).restartTopic(eq("topic-b"));

        // No stored row_counts mismatch here — restore itself succeeds, so this
        // proves the per-topic resume failure is swallowed rather than propagated.
        service.restoreSnapshot(SNAPSHOT_NAME);

        for (String topic : PAUSED_TOPICS) {
            verify(recordingCoordinator, times(1)).restartTopic(topic);
        }
    }

    @Test
    void restoreSnapshot_whenResumeFailsAfterVerificationFailure_originalExceptionSurvives() throws SQLException {
        // Corrupt the stored row_counts so verifyRestoredRowCounts() detects a
        // mismatch and throws SnapshotVerificationException from the try block.
        try (Statement st = duckDB.createStatement()) {
            st.execute("UPDATE snapshots SET row_counts = '{\"general_seed_topic\": 999}' " +
                    "WHERE name = '" + SNAPSHOT_NAME + "'");
        }
        // Every resume also fails — if the finally block's own exception could mask
        // the try block's exception, this test would see a resume RuntimeException
        // instead of the SnapshotVerificationException.
        doThrow(new RuntimeException("resume boom")).when(recordingCoordinator).restartTopic(org.mockito.ArgumentMatchers.anyString());

        assertThatThrownBy(() -> service.restoreSnapshot(SNAPSHOT_NAME))
                .isInstanceOf(SnapshotVerificationException.class);

        for (String topic : PAUSED_TOPICS) {
            verify(recordingCoordinator, times(1)).restartTopic(topic);
        }
    }
}

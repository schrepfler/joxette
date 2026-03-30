package com.joxette.management;

import com.joxette.config.JoxetteProperties;
import com.joxette.recording.RecordingCoordinator;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.util.*;

/**
 * DuckDB-backed store for runtime topic and entity-type configuration.
 *
 * <p>On startup ({@link #initialize()}):
 * <ol>
 *   <li>Bootstrap entries from {@code application.yml} are seeded into the
 *       {@code lake.config_*} tables (idempotent — conflicts are ignored).</li>
 *   <li>All non-paused topics are started via {@link RecordingCoordinator}.</li>
 * </ol>
 *
 * <p>All public methods synchronise on the shared DuckDB connection, matching
 * the pattern used throughout the rest of the codebase.
 *
 * <p>{@code @DependsOn("schemaManager")} guarantees that the config tables
 * exist before this bean's {@link PostConstruct} runs.
 */
@Repository
@DependsOn("schemaManager")
public class ConfigRepository {

    private static final Set<String> VALID_MODES = Set.of("general", "entity_only", "both");

    private final Connection duckDB;
    private final JoxetteProperties properties;
    private final RecordingCoordinator coordinator;

    public ConfigRepository(Connection duckDB, JoxetteProperties properties,
                            RecordingCoordinator coordinator) {
        this.duckDB      = duckDB;
        this.properties  = properties;
        this.coordinator = coordinator;
    }

    @PostConstruct
    public void initialize() throws SQLException {
        seedFromBootstrap();
        for (TopicConfig tc : listTopics()) {
            if (!tc.paused()) {
                coordinator.startTopic(tc.topic());
            }
        }
    }

    // -------------------------------------------------------------------------
    // Topic CRUD
    // -------------------------------------------------------------------------

    public List<TopicConfig> listTopics() throws SQLException {
        List<TopicConfig> result = new ArrayList<>();
        Set<String> active = coordinator.activeTopics();
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT topic, mode, paused FROM lake.config_topics ORDER BY topic")) {
                while (rs.next()) {
                    String topic = rs.getString("topic");
                    result.add(new TopicConfig(topic, rs.getString("mode"),
                            rs.getBoolean("paused"), active.contains(topic)));
                }
            }
        }
        return result;
    }

    public Optional<TopicConfig> findTopic(String topic) throws SQLException {
        Set<String> active = coordinator.activeTopics();
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT topic, mode, paused FROM lake.config_topics WHERE topic = ?")) {
                ps.setString(1, topic);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return Optional.of(new TopicConfig(rs.getString("topic"),
                                rs.getString("mode"), rs.getBoolean("paused"),
                                active.contains(topic)));
                    }
                }
            }
        }
        return Optional.empty();
    }

    public TopicConfig upsertTopic(String topic, String mode, boolean paused) throws SQLException {
        validateMode(mode);
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO lake.config_topics (topic, mode, paused) VALUES (?, ?, ?)
                    ON CONFLICT (topic) DO UPDATE SET mode = excluded.mode, paused = excluded.paused
                    """)) {
                ps.setString(1, topic);
                ps.setString(2, mode);
                ps.setBoolean(3, paused);
                ps.executeUpdate();
            }
        }
        return new TopicConfig(topic, mode, paused, coordinator.activeTopics().contains(topic));
    }

    public boolean deleteTopic(String topic) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM lake.config_topics WHERE topic = ?")) {
                ps.setString(1, topic);
                return ps.executeUpdate() > 0;
            }
        }
    }

    public boolean setPaused(String topic, boolean paused) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "UPDATE lake.config_topics SET paused = ? WHERE topic = ?")) {
                ps.setBoolean(1, paused);
                ps.setString(2, topic);
                return ps.executeUpdate() > 0;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Entity type CRUD
    // -------------------------------------------------------------------------

    public List<EntityTypeConfig> listEntityTypes() throws SQLException {
        List<EntityTypeConfig> result = new ArrayList<>();
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT entity_type, buckets FROM lake.config_entities ORDER BY entity_type")) {
                while (rs.next()) {
                    result.add(new EntityTypeConfig(rs.getString("entity_type"),
                            rs.getInt("buckets")));
                }
            }
        }
        return result;
    }

    public Optional<EntityTypeConfig> findEntityType(String type) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT entity_type, buckets FROM lake.config_entities WHERE entity_type = ?")) {
                ps.setString(1, type);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return Optional.of(new EntityTypeConfig(rs.getString("entity_type"),
                                rs.getInt("buckets")));
                    }
                }
            }
        }
        return Optional.empty();
    }

    public EntityTypeConfig upsertEntityType(String type, int buckets) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO lake.config_entities (entity_type, buckets) VALUES (?, ?)
                    ON CONFLICT (entity_type) DO UPDATE SET buckets = excluded.buckets
                    """)) {
                ps.setString(1, type);
                ps.setInt(2, buckets);
                ps.executeUpdate();
            }
        }
        return new EntityTypeConfig(type, buckets);
    }

    public boolean deleteEntityType(String type) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM lake.config_entity_sources WHERE entity_type = ?")) {
                ps.setString(1, type);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM lake.config_entities WHERE entity_type = ?")) {
                ps.setString(1, type);
                return ps.executeUpdate() > 0;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Entity source CRUD
    // -------------------------------------------------------------------------

    public List<EntitySourceConfig> listSources(String entityType) throws SQLException {
        List<EntitySourceConfig> result = new ArrayList<>();
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    SELECT entity_type, topic, id_source, id_expression
                    FROM lake.config_entity_sources
                    WHERE entity_type = ?
                    ORDER BY topic
                    """)) {
                ps.setString(1, entityType);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        result.add(new EntitySourceConfig(
                                rs.getString("entity_type"),
                                rs.getString("topic"),
                                rs.getString("id_source"),
                                rs.getString("id_expression")));
                    }
                }
            }
        }
        return result;
    }

    public EntitySourceConfig upsertSource(String entityType, String topic,
                                           String idSource, String idExpression) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO lake.config_entity_sources (entity_type, topic, id_source, id_expression)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (entity_type, topic) DO UPDATE SET
                        id_source = excluded.id_source,
                        id_expression = excluded.id_expression
                    """)) {
                ps.setString(1, entityType);
                ps.setString(2, topic);
                ps.setString(3, idSource);
                ps.setString(4, idExpression);
                ps.executeUpdate();
            }
        }
        return new EntitySourceConfig(entityType, topic, idSource, idExpression);
    }

    public boolean deleteSource(String entityType, String topic) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM lake.config_entity_sources WHERE entity_type = ? AND topic = ?")) {
                ps.setString(1, entityType);
                ps.setString(2, topic);
                return ps.executeUpdate() > 0;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Bootstrap seeding
    // -------------------------------------------------------------------------

    private void seedFromBootstrap() throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO lake.config_topics (topic, mode, paused) VALUES (?, ?, false)
                    ON CONFLICT DO NOTHING
                    """)) {
                for (var e : properties.getBootstrap().getTopics()) {
                    ps.setString(1, e.getTopic());
                    ps.setString(2, e.getMode());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO lake.config_entities (entity_type, buckets) VALUES (?, ?)
                    ON CONFLICT DO NOTHING
                    """)) {
                for (var e : properties.getBootstrap().getEntities()) {
                    ps.setString(1, e.getType());
                    ps.setInt(2, e.getBuckets());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO lake.config_entity_sources (entity_type, topic, id_source, id_expression)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """)) {
                for (var e : properties.getBootstrap().getEntities()) {
                    for (var src : e.getSources()) {
                        ps.setString(1, e.getType());
                        ps.setString(2, src.getTopic());
                        ps.setString(3, src.getEntityId().getSource());
                        ps.setString(4, src.getEntityId().getExpression());
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Validation
    // -------------------------------------------------------------------------

    private static void validateMode(String mode) {
        if (!VALID_MODES.contains(mode)) {
            throw new IllegalArgumentException(
                    "Invalid mode '%s': must be one of %s".formatted(mode, VALID_MODES));
        }
    }
}

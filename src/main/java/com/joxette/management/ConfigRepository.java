package com.joxette.management;

import com.joxette.config.JoxetteProperties;
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
 *       config tables (idempotent — conflicts are ignored).</li>
 * </ol>
 *
 * <p>Topic recording is started by {@link RecordingStartupRunner} after the
 * full application context is ready, avoiding circular bean dependencies.
 */
@Repository("managementConfigRepository")
@DependsOn("dbSchemaManager")
public class ConfigRepository {

    private static final Set<String> VALID_MODES = Set.of("general", "entity_only", "both");

    private final Connection duckDB;
    private final JoxetteProperties properties;

    public ConfigRepository(Connection duckDB, JoxetteProperties properties) {
        this.duckDB     = duckDB;
        this.properties = properties;
    }

    @PostConstruct
    public void initialize() throws SQLException {
        seedFromBootstrap();
    }

    // -------------------------------------------------------------------------
    // Topic CRUD
    // -------------------------------------------------------------------------

    public List<TopicConfig> listTopics() throws SQLException {
        List<TopicConfig> result = new ArrayList<>();
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT topic, mode, paused, retention_days, start_from, broker_id FROM topic_configs ORDER BY topic")) {
                while (rs.next()) {
                    result.add(new TopicConfig(rs.getString("topic"), rs.getString("mode"),
                            rs.getBoolean("paused"), false,
                            (Integer) rs.getObject("retention_days"),
                            rs.getString("start_from"),
                            rs.getString("broker_id")));
                }
            }
        }
        return result;
    }

    public Optional<TopicConfig> findTopic(String topic) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT topic, mode, paused, retention_days, start_from, broker_id FROM topic_configs WHERE topic = ?")) {
                ps.setString(1, topic);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return Optional.of(new TopicConfig(rs.getString("topic"),
                                rs.getString("mode"), rs.getBoolean("paused"), false,
                                (Integer) rs.getObject("retention_days"),
                                rs.getString("start_from"),
                                rs.getString("broker_id")));
                    }
                }
            }
        }
        return Optional.empty();
    }

    public TopicConfig upsertTopic(String topic, String mode, boolean paused, String startFrom) throws SQLException {
        validateMode(mode);
        String resolvedStartFrom = "earliest".equals(startFrom) ? "earliest" : "latest";
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO topic_configs (topic, mode, paused, start_from) VALUES (?, ?, ?, ?)
                    ON CONFLICT (topic) DO UPDATE SET mode = excluded.mode, paused = excluded.paused
                    RETURNING topic, mode, paused, retention_days, start_from, broker_id
                    """)) {
                ps.setString(1, topic);
                ps.setString(2, mode);
                ps.setBoolean(3, paused);
                ps.setString(4, resolvedStartFrom);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return new TopicConfig(rs.getString("topic"), rs.getString("mode"),
                                rs.getBoolean("paused"), false,
                                (Integer) rs.getObject("retention_days"),
                                rs.getString("start_from"),
                                rs.getString("broker_id"));
                    }
                }
            }
        }
        return new TopicConfig(topic, mode, paused, false, null, resolvedStartFrom, null);
    }

    /** Convenience overload for callers that don't specify startFrom (defaults to "latest"). */
    public TopicConfig upsertTopic(String topic, String mode, boolean paused) throws SQLException {
        return upsertTopic(topic, mode, paused, "latest");
    }

    public TopicConfig setTopicRetentionDays(String topic, Integer days) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "UPDATE topic_configs SET retention_days = ? WHERE topic = ?")) {
                ps.setObject(1, days);
                ps.setString(2, topic);
                ps.executeUpdate();
            }
        }
        return findTopic(topic).orElseThrow(
                () -> new NoSuchElementException("Topic not found: " + topic));
    }

    public boolean deleteTopic(String topic) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM topic_configs WHERE topic = ?")) {
                ps.setString(1, topic);
                return ps.executeUpdate() > 0;
            }
        }
    }

    public boolean setPaused(String topic, boolean paused) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "UPDATE topic_configs SET paused = ? WHERE topic = ?")) {
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
                         "SELECT entity_type, bucket_count AS buckets, retention_days FROM entity_type_configs ORDER BY entity_type")) {
                while (rs.next()) {
                    result.add(new EntityTypeConfig(rs.getString("entity_type"),
                            rs.getInt("buckets"), (Integer) rs.getObject("retention_days")));
                }
            }
        }
        return result;
    }

    public Optional<EntityTypeConfig> findEntityType(String type) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT entity_type, bucket_count AS buckets, retention_days FROM entity_type_configs WHERE entity_type = ?")) {
                ps.setString(1, type);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return Optional.of(new EntityTypeConfig(rs.getString("entity_type"),
                                rs.getInt("buckets"), (Integer) rs.getObject("retention_days")));
                    }
                }
            }
        }
        return Optional.empty();
    }

    public EntityTypeConfig upsertEntityType(String type, int buckets) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO entity_type_configs (entity_type, bucket_count) VALUES (?, ?)
                    ON CONFLICT (entity_type) DO UPDATE SET bucket_count = excluded.bucket_count
                    RETURNING entity_type, bucket_count AS buckets, retention_days
                    """)) {
                ps.setString(1, type);
                ps.setInt(2, buckets);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return new EntityTypeConfig(rs.getString("entity_type"),
                                rs.getInt("buckets"), (Integer) rs.getObject("retention_days"));
                    }
                }
            }
        }
        return new EntityTypeConfig(type, buckets, null);
    }

    public EntityTypeConfig setEntityRetentionDays(String type, Integer days) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "UPDATE entity_type_configs SET retention_days = ? WHERE entity_type = ?")) {
                ps.setObject(1, days);
                ps.setString(2, type);
                ps.executeUpdate();
            }
        }
        return findEntityType(type).orElseThrow(
                () -> new NoSuchElementException("Entity type not found: " + type));
    }

    public boolean deleteEntityType(String type) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM entity_source_matchers WHERE entity_type = ?")) {
                ps.setString(1, type);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM entity_source_mappings WHERE entity_type = ?")) {
                ps.setString(1, type);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM entity_type_configs WHERE entity_type = ?")) {
                ps.setString(1, type);
                return ps.executeUpdate() > 0;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Entity source CRUD
    // -------------------------------------------------------------------------

    /**
     * Lists all source mappings (with their matchers) for {@code entityType}.
     */
    public List<EntitySourceConfig> listSources(String entityType) throws SQLException {
        // Load mappings first
        Map<String, EntitySourceConfig.Builder> builders = new LinkedHashMap<>();
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    SELECT entity_type, topic, mode
                    FROM entity_source_mappings
                    WHERE entity_type = ?
                    ORDER BY topic
                    """)) {
                ps.setString(1, entityType);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String topic = rs.getString("topic");
                        builders.put(topic, new EntitySourceConfig.Builder(
                                rs.getString("entity_type"), topic, rs.getString("mode")));
                    }
                }
            }
            // Load matchers and attach to their mappings
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    SELECT topic, message_type, id_source, id_expression
                    FROM entity_source_matchers
                    WHERE entity_type = ?
                    ORDER BY topic, id
                    """)) {
                ps.setString(1, entityType);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String topic = rs.getString("topic");
                        EntitySourceConfig.Builder b = builders.get(topic);
                        if (b != null) {
                            b.addMatcher(new EntitySourceConfig.MatcherConfig(
                                    rs.getString("message_type"),
                                    rs.getString("id_source"),
                                    rs.getString("id_expression")));
                        }
                    }
                }
            }
        }
        return builders.values().stream().map(EntitySourceConfig.Builder::build).toList();
    }

    /**
     * Upserts the mapping header ({@code entity_source_mappings}) and replaces
     * all matchers for this {@code (entityType, topic)} pair.
     */
    public EntitySourceConfig upsertSource(String entityType, String topic,
                                            String mode,
                                            List<EntitySourceConfig.MatcherConfig> matchers)
            throws SQLException {
        String resolvedMode = "both".equals(mode) ? "both" : "entity_only";
        synchronized (duckDB) {
            // Upsert the mapping header
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO entity_source_mappings (entity_type, topic, mode)
                    VALUES (?, ?, ?)
                    ON CONFLICT (entity_type, topic) DO UPDATE SET mode = excluded.mode
                    """)) {
                ps.setString(1, entityType);
                ps.setString(2, topic);
                ps.setString(3, resolvedMode);
                ps.executeUpdate();
            }
            // Replace all matchers for this mapping
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM entity_source_matchers WHERE entity_type = ? AND topic = ?")) {
                ps.setString(1, entityType);
                ps.setString(2, topic);
                ps.executeUpdate();
            }
            if (matchers != null && !matchers.isEmpty()) {
                try (PreparedStatement ps = duckDB.prepareStatement("""
                        INSERT INTO entity_source_matchers
                            (entity_type, topic, message_type, id_source, id_expression)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (entity_type, topic, message_type) DO UPDATE SET
                            id_source = excluded.id_source,
                            id_expression = excluded.id_expression
                        """)) {
                    for (EntitySourceConfig.MatcherConfig m : matchers) {
                        ps.setString(1, entityType);
                        ps.setString(2, topic);
                        ps.setString(3, m.messageType());
                        ps.setString(4, m.idSource());
                        ps.setString(5, m.idExpression());
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
            }
        }
        List<EntitySourceConfig.MatcherConfig> resolved =
                matchers != null ? List.copyOf(matchers) : List.of();
        return new EntitySourceConfig(entityType, topic, resolvedMode, resolved);
    }

    public boolean deleteSource(String entityType, String topic) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM entity_source_matchers WHERE entity_type = ? AND topic = ?")) {
                ps.setString(1, entityType);
                ps.setString(2, topic);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM entity_source_mappings WHERE entity_type = ? AND topic = ?")) {
                ps.setString(1, entityType);
                ps.setString(2, topic);
                return ps.executeUpdate() > 0;
            }
        }
    }

    /**
     * Inserts or updates a single matcher for an existing source mapping.
     * Does NOT touch other matchers for the same (entityType, topic) pair.
     */
    public EntitySourceConfig.MatcherConfig addEntityMatcher(
            String entityType, String topic, EntitySourceConfig.MatcherConfig matcher)
            throws SQLException {
        validateMatcherSource(matcher.idSource());
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO entity_source_matchers
                        (entity_type, topic, message_type, id_source, id_expression)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (entity_type, topic, message_type) DO UPDATE SET
                        id_source     = excluded.id_source,
                        id_expression = excluded.id_expression
                    """)) {
                ps.setString(1, entityType);
                ps.setString(2, topic);
                ps.setString(3, matcher.messageType());
                ps.setString(4, matcher.idSource());
                ps.setString(5, matcher.idExpression());
                ps.executeUpdate();
            }
        }
        return matcher;
    }

    /** Deletes a single matcher by messageType. Returns {@code true} if a row was removed. */
    public boolean deleteEntityMatcher(String entityType, String topic, String messageType)
            throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM entity_source_matchers" +
                    " WHERE entity_type = ? AND topic = ? AND message_type = ?")) {
                ps.setString(1, entityType);
                ps.setString(2, topic);
                ps.setString(3, messageType);
                return ps.executeUpdate() > 0;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Topic message-type matchers CRUD
    // -------------------------------------------------------------------------

    public List<TopicMatcherConfig> listTopicMatchers(String topic) throws SQLException {
        List<TopicMatcherConfig> result = new ArrayList<>();
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT topic, message_type, id_source, id_expression" +
                    " FROM topic_message_type_matchers WHERE topic = ?" +
                    " ORDER BY rowid")) {
                ps.setString(1, topic);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        result.add(new TopicMatcherConfig(
                                rs.getString("topic"),
                                rs.getString("message_type"),
                                rs.getString("id_source"),
                                rs.getString("id_expression")));
                    }
                }
            }
        }
        return result;
    }

    public List<TopicMatcherConfig> listAllTopicMatchers() throws SQLException {
        List<TopicMatcherConfig> result = new ArrayList<>();
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT topic, message_type, id_source, id_expression" +
                         " FROM topic_message_type_matchers ORDER BY topic, rowid")) {
                while (rs.next()) {
                    result.add(new TopicMatcherConfig(
                            rs.getString("topic"),
                            rs.getString("message_type"),
                            rs.getString("id_source"),
                            rs.getString("id_expression")));
                }
            }
        }
        return result;
    }

    public TopicMatcherConfig upsertTopicMatcher(String topic, String messageType,
                                                  String idSource, String idExpression)
            throws SQLException {
        validateMatcherSource(idSource);
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO topic_message_type_matchers (topic, message_type, id_source, id_expression)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (topic, message_type) DO UPDATE SET
                        id_source    = excluded.id_source,
                        id_expression = excluded.id_expression
                    """)) {
                ps.setString(1, topic);
                ps.setString(2, messageType);
                ps.setString(3, idSource);
                ps.setString(4, idExpression);
                ps.executeUpdate();
            }
        }
        return new TopicMatcherConfig(topic, messageType, idSource, idExpression);
    }

    public boolean deleteTopicMatcher(String topic, String messageType) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM topic_message_type_matchers WHERE topic = ? AND message_type = ?")) {
                ps.setString(1, topic);
                ps.setString(2, messageType);
                return ps.executeUpdate() > 0;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Counts (used by ConfigController for the domain summary)
    // -------------------------------------------------------------------------

    public int countTopics() throws SQLException {
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM topic_configs")) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }

    public int countEntityTypes() throws SQLException {
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM entity_type_configs")) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }

    public int countSourceMappings() throws SQLException {
        synchronized (duckDB) {
            try (Statement st = duckDB.createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM entity_source_mappings")) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Bootstrap seeding
    // -------------------------------------------------------------------------

    private void seedFromBootstrap() throws SQLException {
        synchronized (duckDB) {
            // Topics
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO topic_configs (topic, mode, paused) VALUES (?, ?, false)
                    ON CONFLICT DO NOTHING
                    """)) {
                for (var e : properties.getBootstrap().getTopics()) {
                    ps.setString(1, e.getTopic());
                    ps.setString(2, e.getMode());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            // Entity types
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO entity_type_configs (entity_type, bucket_count) VALUES (?, ?)
                    ON CONFLICT DO NOTHING
                    """)) {
                for (var e : properties.getBootstrap().getEntities()) {
                    ps.setString(1, e.getType());
                    ps.setInt(2, e.getBuckets());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            // Source mappings + matchers
            try (PreparedStatement psMapping = duckDB.prepareStatement("""
                    INSERT INTO entity_source_mappings (entity_type, topic, mode)
                    VALUES (?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """);
                 PreparedStatement psMatcher = duckDB.prepareStatement("""
                    INSERT INTO entity_source_matchers
                        (entity_type, topic, message_type, id_source, id_expression)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """)) {
                for (var e : properties.getBootstrap().getEntities()) {
                    for (var src : e.getSources()) {
                        String m = "both".equals(src.getMode()) ? "both" : "entity_only";
                        psMapping.setString(1, e.getType());
                        psMapping.setString(2, src.getTopic());
                        psMapping.setString(3, m);
                        psMapping.addBatch();

                        for (var matcher : src.getMatchers()) {
                            psMatcher.setString(1, e.getType());
                            psMatcher.setString(2, src.getTopic());
                            psMatcher.setString(3, matcher.getMessageType());
                            psMatcher.setString(4, matcher.getSource());
                            psMatcher.setString(5, matcher.getExpression());
                            psMatcher.addBatch();
                        }
                    }
                }
                psMapping.executeBatch();
                psMatcher.executeBatch();
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

    private static final Set<String> VALID_ID_SOURCES = Set.of("key", "value", "header");

    private static void validateMatcherSource(String idSource) {
        if (!VALID_ID_SOURCES.contains(idSource)) {
            throw new IllegalArgumentException(
                    "Invalid id_source '%s': must be one of %s".formatted(idSource, VALID_ID_SOURCES));
        }
    }
}

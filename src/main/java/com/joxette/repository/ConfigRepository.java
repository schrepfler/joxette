package com.joxette.repository;

import com.joxette.model.EntitySourceMapping;
import com.joxette.model.EntityTypeConfig;
import com.joxette.model.TopicConfig;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Plain-JDBC repository backed by the shared DuckDB {@link Connection}.
 *
 * <p>All three config tables ({@code topic_configs}, {@code entity_type_configs},
 * {@code entity_source_mappings}) are created on first use via {@link #initSchema()}.
 *
 * <p>DuckDB serialises writes internally, so no external locking is needed.
 */
@Repository
public class ConfigRepository {

    private final Connection conn;

    public ConfigRepository(Connection duckDbConnection) {
        this.conn = duckDbConnection;
    }

    @PostConstruct
    public void initSchema() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("""
                    CREATE TABLE IF NOT EXISTS topic_configs (
                        topic VARCHAR PRIMARY KEY,
                        mode  VARCHAR NOT NULL
                    )
                    """);
            st.execute("""
                    CREATE TABLE IF NOT EXISTS entity_type_configs (
                        entity_type VARCHAR PRIMARY KEY,
                        buckets     INTEGER NOT NULL
                    )
                    """);
            st.execute("""
                    CREATE TABLE IF NOT EXISTS entity_source_mappings (
                        entity_type          VARCHAR NOT NULL,
                        topic                VARCHAR NOT NULL,
                        entity_id_source     VARCHAR NOT NULL,
                        entity_id_expression VARCHAR NOT NULL,
                        PRIMARY KEY (entity_type, topic)
                    )
                    """);
        }
    }

    // -----------------------------------------------------------------------
    // TopicConfig
    // -----------------------------------------------------------------------

    public List<TopicConfig> findAllTopics() throws SQLException {
        List<TopicConfig> result = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT topic, mode FROM topic_configs ORDER BY topic")) {
            while (rs.next()) {
                result.add(new TopicConfig(rs.getString("topic"), rs.getString("mode")));
            }
        }
        return result;
    }

    public Optional<TopicConfig> findTopic(String topic) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT topic, mode FROM topic_configs WHERE topic = ?")) {
            ps.setString(1, topic);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(new TopicConfig(rs.getString("topic"), rs.getString("mode")));
                }
            }
        }
        return Optional.empty();
    }

    public void upsertTopic(TopicConfig config) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO topic_configs (topic, mode) VALUES (?, ?)
                ON CONFLICT (topic) DO UPDATE SET mode = excluded.mode
                """)) {
            ps.setString(1, config.topic());
            ps.setString(2, config.mode());
            ps.executeUpdate();
        }
    }

    public void deleteTopic(String topic) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM topic_configs WHERE topic = ?")) {
            ps.setString(1, topic);
            ps.executeUpdate();
        }
    }

    /** Returns {@code true} when the {@code topic_configs} table has no rows. */
    public boolean isTopicConfigEmpty() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM topic_configs")) {
            return rs.next() && rs.getLong(1) == 0;
        }
    }

    // -----------------------------------------------------------------------
    // EntityTypeConfig
    // -----------------------------------------------------------------------

    public List<EntityTypeConfig> findAllEntityTypes() throws SQLException {
        List<EntityTypeConfig> result = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT entity_type, buckets FROM entity_type_configs ORDER BY entity_type")) {
            while (rs.next()) {
                result.add(new EntityTypeConfig(rs.getString("entity_type"), rs.getInt("buckets")));
            }
        }
        return result;
    }

    public Optional<EntityTypeConfig> findEntityType(String entityType) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT entity_type, buckets FROM entity_type_configs WHERE entity_type = ?")) {
            ps.setString(1, entityType);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(
                            new EntityTypeConfig(rs.getString("entity_type"), rs.getInt("buckets")));
                }
            }
        }
        return Optional.empty();
    }

    public void upsertEntityType(EntityTypeConfig config) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO entity_type_configs (entity_type, buckets) VALUES (?, ?)
                ON CONFLICT (entity_type) DO UPDATE SET buckets = excluded.buckets
                """)) {
            ps.setString(1, config.entityType());
            ps.setInt(2, config.buckets());
            ps.executeUpdate();
        }
    }

    public void deleteEntityType(String entityType) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM entity_type_configs WHERE entity_type = ?")) {
            ps.setString(1, entityType);
            ps.executeUpdate();
        }
    }

    // -----------------------------------------------------------------------
    // EntitySourceMapping
    // -----------------------------------------------------------------------

    public List<EntitySourceMapping> findAllMappings() throws SQLException {
        List<EntitySourceMapping> result = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("""
                     SELECT entity_type, topic, entity_id_source, entity_id_expression
                     FROM entity_source_mappings
                     ORDER BY entity_type, topic
                     """)) {
            while (rs.next()) {
                result.add(toMapping(rs));
            }
        }
        return result;
    }

    public List<EntitySourceMapping> findMappingsByEntityType(String entityType) throws SQLException {
        List<EntitySourceMapping> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("""
                SELECT entity_type, topic, entity_id_source, entity_id_expression
                FROM entity_source_mappings
                WHERE entity_type = ?
                ORDER BY topic
                """)) {
            ps.setString(1, entityType);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(toMapping(rs));
                }
            }
        }
        return result;
    }

    public List<EntitySourceMapping> findMappingsByTopic(String topic) throws SQLException {
        List<EntitySourceMapping> result = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("""
                SELECT entity_type, topic, entity_id_source, entity_id_expression
                FROM entity_source_mappings
                WHERE topic = ?
                ORDER BY entity_type
                """)) {
            ps.setString(1, topic);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(toMapping(rs));
                }
            }
        }
        return result;
    }

    public void upsertMapping(EntitySourceMapping mapping) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO entity_source_mappings
                    (entity_type, topic, entity_id_source, entity_id_expression)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (entity_type, topic) DO UPDATE SET
                    entity_id_source     = excluded.entity_id_source,
                    entity_id_expression = excluded.entity_id_expression
                """)) {
            ps.setString(1, mapping.entityType());
            ps.setString(2, mapping.topic());
            ps.setString(3, mapping.entityIdSource());
            ps.setString(4, mapping.entityIdExpression());
            ps.executeUpdate();
        }
    }

    public void deleteMapping(String entityType, String topic) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM entity_source_mappings WHERE entity_type = ? AND topic = ?")) {
            ps.setString(1, entityType);
            ps.setString(2, topic);
            ps.executeUpdate();
        }
    }

    public void deleteMappingsByEntityType(String entityType) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM entity_source_mappings WHERE entity_type = ?")) {
            ps.setString(1, entityType);
            ps.executeUpdate();
        }
    }

    private EntitySourceMapping toMapping(ResultSet rs) throws SQLException {
        return new EntitySourceMapping(
                rs.getString("entity_type"),
                rs.getString("topic"),
                rs.getString("entity_id_source"),
                rs.getString("entity_id_expression")
        );
    }
}

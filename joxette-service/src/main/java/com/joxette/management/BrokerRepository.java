package com.joxette.management;

import com.joxette.config.JoxetteProperties;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * DuckDB-backed store for Kafka broker connection configurations.
 *
 * <p>On startup ({@link #initialize()}):
 * <ol>
 *   <li>Bootstrap broker entries from {@code application.yml} are seeded into
 *       {@code broker_configs} (idempotent — conflicts are ignored).</li>
 *   <li>If no row with {@code broker_id = "default"} exists after seeding, one
 *       is inserted using {@code joxette.kafka.bootstrapServers} with
 *       {@code PLAINTEXT} security.</li>
 * </ol>
 *
 * <p>All write methods use {@code synchronized(duckDB)} — the same pattern as
 * {@link ConfigRepository} — because DuckDB serialises writes internally but
 * does not support concurrent writers from different threads.
 */
@Repository
@DependsOn("dbSchemaManager")
public class BrokerRepository {

    private static final String UPSERT_SQL = """
            INSERT INTO broker_configs (
                broker_id, bootstrap_servers, security_protocol,
                sasl_mechanism, sasl_username, sasl_password,
                ssl_truststore_path, ssl_truststore_password,
                ssl_keystore_path, ssl_keystore_password
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (broker_id) DO UPDATE SET
                bootstrap_servers       = excluded.bootstrap_servers,
                security_protocol       = excluded.security_protocol,
                sasl_mechanism          = excluded.sasl_mechanism,
                sasl_username           = excluded.sasl_username,
                sasl_password           = excluded.sasl_password,
                ssl_truststore_path     = excluded.ssl_truststore_path,
                ssl_truststore_password = excluded.ssl_truststore_password,
                ssl_keystore_path       = excluded.ssl_keystore_path,
                ssl_keystore_password   = excluded.ssl_keystore_password,
                updated_at              = now()
            """;

    private final Connection duckDB;
    private final JoxetteProperties properties;

    public BrokerRepository(Connection duckDB, JoxetteProperties properties) {
        this.duckDB     = duckDB;
        this.properties = properties;
    }

    @PostConstruct
    public void initialize() throws SQLException {
        // Seed bootstrap broker entries (ON CONFLICT DO NOTHING semantics).
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement("""
                    INSERT INTO broker_configs (
                        broker_id, bootstrap_servers, security_protocol,
                        sasl_mechanism, sasl_username, sasl_password,
                        ssl_truststore_path, ssl_truststore_password,
                        ssl_keystore_path, ssl_keystore_password
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """)) {
                for (JoxetteProperties.Kafka.BrokerEntry e : properties.getKafka().getBrokers()) {
                    ps.setString(1, e.getId());
                    ps.setString(2, e.getBootstrapServers());
                    ps.setString(3, e.getSecurityProtocol() != null ? e.getSecurityProtocol() : "PLAINTEXT");
                    ps.setString(4, e.getSaslMechanism());
                    ps.setString(5, e.getSaslUsername());
                    ps.setString(6, e.getSaslPassword());
                    ps.setString(7, e.getSslTruststorePath());
                    ps.setString(8, e.getSslTruststorePassword());
                    ps.setString(9, e.getSslKeystorePath());
                    ps.setString(10, e.getSslKeystorePassword());
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            // Ensure the "default" broker always exists.
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT COUNT(*) FROM broker_configs WHERE broker_id = ?")) {
                ps.setString(1, BrokerConfig.DEFAULT_BROKER_ID);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next() && rs.getInt(1) == 0) {
                        insertDefault();
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Queries
    // -------------------------------------------------------------------------

    public List<BrokerConfig> listBrokers() throws SQLException {
        List<BrokerConfig> result = new ArrayList<>();
        try (Statement st = duckDB.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT broker_id, bootstrap_servers, security_protocol," +
                     " sasl_mechanism, sasl_username, sasl_password," +
                     " ssl_truststore_path, ssl_truststore_password," +
                     " ssl_keystore_path, ssl_keystore_password" +
                     " FROM broker_configs ORDER BY broker_id")) {
            while (rs.next()) {
                result.add(fromResultSet(rs));
            }
        }
        return result;
    }

    public Optional<BrokerConfig> findBroker(String brokerId) throws SQLException {
        try (PreparedStatement ps = duckDB.prepareStatement(
                "SELECT broker_id, bootstrap_servers, security_protocol," +
                " sasl_mechanism, sasl_username, sasl_password," +
                " ssl_truststore_path, ssl_truststore_password," +
                " ssl_keystore_path, ssl_keystore_password" +
                " FROM broker_configs WHERE broker_id = ?")) {
            ps.setString(1, brokerId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(fromResultSet(rs));
                }
            }
        }
        return Optional.empty();
    }

    // -------------------------------------------------------------------------
    // Mutations
    // -------------------------------------------------------------------------

    public BrokerConfig upsertBroker(BrokerConfig cfg) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(UPSERT_SQL)) {
                ps.setString(1,  cfg.brokerId());
                ps.setString(2,  cfg.bootstrapServers());
                ps.setString(3,  cfg.securityProtocol() != null ? cfg.securityProtocol() : "PLAINTEXT");
                ps.setString(4,  cfg.saslMechanism());
                ps.setString(5,  cfg.saslUsername());
                ps.setString(6,  cfg.saslPassword());
                ps.setString(7,  cfg.sslTruststorePath());
                ps.setString(8,  cfg.sslTruststorePassword());
                ps.setString(9,  cfg.sslKeystorePath());
                ps.setString(10, cfg.sslKeystorePassword());
                ps.executeUpdate();
            }
        }
        return cfg;
    }

    /**
     * Deletes a broker configuration.
     *
     * @throws com.joxette.api.error.ConflictException if any topic in
     *         {@code topic_configs} still references {@code brokerId}.
     */
    public boolean deleteBroker(String brokerId) throws SQLException {
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "SELECT COUNT(*) FROM topic_configs WHERE broker_id = ?")) {
                ps.setString(1, brokerId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next() && rs.getInt(1) > 0) {
                        throw com.joxette.api.error.ConflictException.brokerInUse(brokerId);
                    }
                }
            }
            try (PreparedStatement ps = duckDB.prepareStatement(
                    "DELETE FROM broker_configs WHERE broker_id = ?")) {
                ps.setString(1, brokerId);
                return ps.executeUpdate() > 0;
            }
        }
    }

    /**
     * Resolves the effective broker configuration for {@code brokerId}.
     *
     * <ul>
     *   <li>If {@code brokerId} is null or blank, looks up {@code "default"}.</li>
     *   <li>If no matching row is found, returns a synthetic default built from
     *       {@code joxette.kafka.bootstrapServers} with PLAINTEXT security.</li>
     * </ul>
     */
    public BrokerConfig resolveBroker(String brokerId) throws SQLException {
        String id = (brokerId == null || brokerId.isBlank())
                ? BrokerConfig.DEFAULT_BROKER_ID
                : brokerId;
        return findBroker(id).orElseGet(() ->
                new BrokerConfig(
                        BrokerConfig.DEFAULT_BROKER_ID,
                        properties.getKafka().getBootstrapServers(),
                        "PLAINTEXT",
                        null, null, null, null, null, null, null));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void insertDefault() throws SQLException {
        try (PreparedStatement ps = duckDB.prepareStatement("""
                INSERT INTO broker_configs (
                    broker_id, bootstrap_servers, security_protocol
                ) VALUES (?, ?, 'PLAINTEXT')
                ON CONFLICT DO NOTHING
                """)) {
            ps.setString(1, BrokerConfig.DEFAULT_BROKER_ID);
            ps.setString(2, properties.getKafka().getBootstrapServers());
            ps.executeUpdate();
        }
    }

    private static BrokerConfig fromResultSet(ResultSet rs) throws SQLException {
        return new BrokerConfig(
                rs.getString("broker_id"),
                rs.getString("bootstrap_servers"),
                rs.getString("security_protocol"),
                rs.getString("sasl_mechanism"),
                rs.getString("sasl_username"),
                rs.getString("sasl_password"),
                rs.getString("ssl_truststore_path"),
                rs.getString("ssl_truststore_password"),
                rs.getString("ssl_keystore_path"),
                rs.getString("ssl_keystore_password"));
    }
}

package com.joxette.config;

import com.joxette.kafka.ConsumerSettings;
import com.joxette.kafka.ProducerSettings;
import com.joxette.management.BrokerConfig;
import com.joxette.management.BrokerRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Builds Kafka client configurations per broker, delegating security property
 * assembly to the stored {@link BrokerConfig} looked up from {@link BrokerRepository}.
 */
@Component
public class BrokerConnectionFactory {

    private final BrokerRepository brokerRepository;

    public BrokerConnectionFactory(BrokerRepository brokerRepository) {
        this.brokerRepository = brokerRepository;
    }

    /**
     * Builds {@link ConsumerSettings} for the given broker ID.
     *
     * <p>Hardcoded consumer defaults:
     * <ul>
     *   <li>{@code enable.auto.commit = false} — offsets committed explicitly after
     *       a successful DuckLake write.</li>
     *   <li>{@code auto.offset.reset = latest}</li>
     *   <li>Key deserializer: {@link StringDeserializer}</li>
     *   <li>Value deserializer: {@link ByteArrayDeserializer}</li>
     * </ul>
     */
    public ConsumerSettings<String, byte[]> consumerSettings(String brokerId) {
        BrokerConfig cfg = resolve(brokerId);
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        applySecurityProps(props, cfg);
        return ConsumerSettings.create(props, new StringDeserializer(), new ByteArrayDeserializer());
    }

    /**
     * Builds {@link ProducerSettings} for the given broker ID.
     *
     * <p>Hardcoded producer defaults:
     * <ul>
     *   <li>{@code acks = all}</li>
     *   <li>{@code enable.idempotence = true}</li>
     *   <li>{@code batch.size = 65536}</li>
     *   <li>{@code linger.ms = 5}</li>
     * </ul>
     */
    public ProducerSettings<byte[], byte[]> producerSettings(String brokerId) {
        BrokerConfig cfg = resolve(brokerId);
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        applySecurityProps(props, cfg);
        return ProducerSettings.create(props, new ByteArraySerializer(), new ByteArraySerializer());
    }

    /**
     * Creates an {@link AdminClient} for the given broker ID. The caller owns
     * the lifecycle and must call {@link AdminClient#close()} when done.
     *
     * <p>Hardcoded timeouts:
     * <ul>
     *   <li>{@code request.timeout.ms = 3000}</li>
     *   <li>{@code default.api.timeout.ms = 5000}</li>
     * </ul>
     */
    public AdminClient adminClient(String brokerId) {
        BrokerConfig cfg = resolve(brokerId);
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5_000);
        applySecurityProps(props, cfg);
        return AdminClient.create(props);
    }

    // -------------------------------------------------------------------------
    // Security helpers
    // -------------------------------------------------------------------------

    private void applySecurityProps(Map<String, Object> props, BrokerConfig cfg) {
        String protocol = cfg.securityProtocol() != null ? cfg.securityProtocol() : "PLAINTEXT";
        props.put("security.protocol", protocol);
        if (cfg.requiresSasl()) {
            String mechanism = cfg.saslMechanism() != null ? cfg.saslMechanism() : "PLAIN";
            props.put("sasl.mechanism", mechanism);
            props.put("sasl.jaas.config", buildJaasConfig(cfg));
        }
        if (cfg.requiresSsl()) {
            if (cfg.sslTruststorePath() != null) {
                props.put("ssl.truststore.location", cfg.sslTruststorePath());
                props.put("ssl.truststore.password", cfg.sslTruststorePassword());
            }
            if (cfg.sslKeystorePath() != null) {
                props.put("ssl.keystore.location", cfg.sslKeystorePath());
                props.put("ssl.keystore.password", cfg.sslKeystorePassword());
            }
        }
    }

    private String buildJaasConfig(BrokerConfig cfg) {
        String cls = switch (cfg.saslMechanism()) {
            case "PLAIN"         -> "org.apache.kafka.common.security.plain.PlainLoginModule";
            case "SCRAM-SHA-256",
                 "SCRAM-SHA-512" -> "org.apache.kafka.common.security.scram.ScramLoginModule";
            default -> throw new IllegalArgumentException("Unknown SASL mechanism: " + cfg.saslMechanism());
        };
        return "%s required username=\"%s\" password=\"%s\";".formatted(
                cls,
                cfg.saslUsername()  != null ? cfg.saslUsername()  : "",
                cfg.saslPassword()  != null ? cfg.saslPassword()  : "");
    }

    private BrokerConfig resolve(String brokerId) {
        try {
            return brokerRepository.resolveBroker(brokerId);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to resolve broker config for id: " + brokerId, e);
        }
    }
}

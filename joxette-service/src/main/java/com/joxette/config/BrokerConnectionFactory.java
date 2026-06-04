package com.joxette.config;

import com.joxette.management.BrokerConfig;
import com.joxette.management.BrokerRepository;
import com.softwaremill.jox.kafka.ConsumerSettings;
import com.softwaremill.jox.kafka.ProducerSettings;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
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
    private final JoxetteProperties properties;

    public BrokerConnectionFactory(BrokerRepository brokerRepository, JoxetteProperties properties) {
        this.brokerRepository = brokerRepository;
        this.properties = properties;
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
        String groupProtocol = properties.getKafka().getGroupProtocol();
        ConsumerSettings<String, byte[]> settings = ConsumerSettings
                .defaults(properties.getKafka().getConsumerGroup())
                .bootstrapServers(cfg.bootstrapServers())
                .keyDeserializer(new StringDeserializer())
                .valueDeserializer(new ByteArrayDeserializer())
                .autoOffsetReset(ConsumerSettings.AutoOffsetReset.LATEST)
                .property("enable.auto.commit", "false")
                // KIP-848: enables server-side rebalance on Kafka 3.7+; auto-falls-back to
                // classic cooperative protocol on older brokers — no operator action needed.
                .property("group.protocol", groupProtocol)
                // Back off slowly when the broker is unreachable — default is 50 ms which
                // causes thundering-herd reconnects. 1 s initial, 30 s ceiling.
                .property("reconnect.backoff.ms",     "1000")
                .property("reconnect.backoff.max.ms", "30000")
                // Throttle metadata/fetch retries when the broker is down — default 100 ms
                // causes the client background thread to log "Rebootstrapping" thousands of
                // times per minute. 1 s initial, 30 s ceiling matches the reconnect backoff.
                .property("retry.backoff.ms",         "1000")
                .property("retry.backoff.max.ms",     "30000")
                // Timeout must cover the time to receive the largest possible fetch response.
                // max.partition.fetch.bytes × partitions / link bandwidth sets the floor.
                // 30 s gives headroom for slow VPN/cluster links; still fast enough for
                // TopicLifecycleActor's backoff supervisor to react to genuine broker failures.
                .property("request.timeout.ms",       "30000")
                .property("default.api.timeout.ms",   "35000")
                // Fetch throughput tunables.
                // fetch.min.bytes=1: respond immediately — avoids broker wait penalising near-pace
                //   topics. Raise to e.g. 65536 on dedicated catchup nodes.
                // max.poll.records: more records per poll() call during backlog drain.
                // max.partition.fetch.bytes: larger per-partition payload per fetch round-trip.
                .property("fetch.min.bytes",             String.valueOf(properties.getKafka().getFetchMinBytes()))
                .property("fetch.max.wait.ms",           String.valueOf(properties.getKafka().getFetchMaxWaitMs()))
                .property("max.poll.records",            String.valueOf(properties.getKafka().getMaxPollRecords()))
                .property("max.partition.fetch.bytes",   String.valueOf(properties.getKafka().getMaxPartitionFetchBytes()));
        settings = applySecurityProps(settings, cfg);
        return settings;
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
        ProducerSettings<byte[], byte[]> settings = ProducerSettings
                .defaults()
                .bootstrapServers(cfg.bootstrapServers())
                .keySerializer(new ByteArraySerializer())
                .valueSerializer(new ByteArraySerializer())
                .property("acks", "all")
                .property("enable.idempotence", "true")
                .property("batch.size", "65536")
                .property("linger.ms", "5");
        settings = applySecurityProps(settings, cfg);
        return settings;
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
        // Back off slowly when the broker is unreachable — mirrors consumer settings.
        // Without these the AdminClient hammers bootstrap at ~50ms intervals.
        props.put("reconnect.backoff.ms",      "1000");
        props.put("reconnect.backoff.max.ms",  "30000");
        // Don't hold idle TCP connections open indefinitely.
        props.put("connections.max.idle.ms",   "60000");
        applySecurityPropsMap(props, cfg);
        return AdminClient.create(props);
    }

    // -------------------------------------------------------------------------
    // Security helpers
    // -------------------------------------------------------------------------

    private ConsumerSettings<String, byte[]> applySecurityProps(
            ConsumerSettings<String, byte[]> settings, BrokerConfig cfg) {
        String protocol = cfg.securityProtocol() != null ? cfg.securityProtocol() : "PLAINTEXT";
        settings = settings.property("security.protocol", protocol);
        if (cfg.requiresSasl()) {
            String mechanism = cfg.saslMechanism() != null ? cfg.saslMechanism() : "PLAIN";
            settings = settings
                    .property("sasl.mechanism", mechanism)
                    .property("sasl.jaas.config", buildJaasConfig(cfg));
        }
        if (cfg.requiresSsl()) {
            if (cfg.sslTruststorePath() != null) {
                settings = settings
                        .property("ssl.truststore.location", cfg.sslTruststorePath())
                        .property("ssl.truststore.password", cfg.sslTruststorePassword());
            }
            if (cfg.sslKeystorePath() != null) {
                settings = settings
                        .property("ssl.keystore.location", cfg.sslKeystorePath())
                        .property("ssl.keystore.password", cfg.sslKeystorePassword());
            }
        }
        return settings;
    }

    private ProducerSettings<byte[], byte[]> applySecurityProps(
            ProducerSettings<byte[], byte[]> settings, BrokerConfig cfg) {
        String protocol = cfg.securityProtocol() != null ? cfg.securityProtocol() : "PLAINTEXT";
        settings = settings.property("security.protocol", protocol);
        if (cfg.requiresSasl()) {
            String mechanism = cfg.saslMechanism() != null ? cfg.saslMechanism() : "PLAIN";
            settings = settings
                    .property("sasl.mechanism", mechanism)
                    .property("sasl.jaas.config", buildJaasConfig(cfg));
        }
        if (cfg.requiresSsl()) {
            if (cfg.sslTruststorePath() != null) {
                settings = settings
                        .property("ssl.truststore.location", cfg.sslTruststorePath())
                        .property("ssl.truststore.password", cfg.sslTruststorePassword());
            }
            if (cfg.sslKeystorePath() != null) {
                settings = settings
                        .property("ssl.keystore.location", cfg.sslKeystorePath())
                        .property("ssl.keystore.password", cfg.sslKeystorePassword());
            }
        }
        return settings;
    }

    private void applySecurityPropsMap(Map<String, Object> props, BrokerConfig cfg) {
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

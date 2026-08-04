package com.joxette.config;

import com.joxette.management.BrokerConfig;
import com.joxette.management.BrokerRepository;
import com.softwaremill.jox.kafka.ConsumerSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link BrokerConnectionFactory#consumerSettings(String)}, specifically the
 * conditional added to guard {@code session.timeout.ms} / {@code heartbeat.interval.ms}.
 *
 * <p>{@code kafka-clients:4.1.2} rejects those two properties at {@code KafkaConsumer}
 * construction time when {@code group.protocol=consumer} (KIP-848 server-side rebalance,
 * the default per {@link JoxetteProperties.Kafka#getGroupProtocol()}) — they are
 * classic-protocol-only. See {@link BrokerConnectionFactory#consumerSettings(String)}.
 */
@ExtendWith(MockitoExtension.class)
class BrokerConnectionFactoryTest {

    private static final String BROKER_ID = "default";

    @Mock
    BrokerRepository brokerRepository;

    private BrokerConnectionFactory factory(String groupProtocol) throws SQLException {
        JoxetteProperties properties = new JoxetteProperties();
        properties.getKafka().setGroupProtocol(groupProtocol);
        properties.getKafka().setConsumerGroup("joxette-recorder");

        when(brokerRepository.resolveBroker(BROKER_ID)).thenReturn(new BrokerConfig(
                BROKER_ID, "localhost:9092", "PLAINTEXT",
                null, null, null, null, null, null, null));

        return new BrokerConnectionFactory(brokerRepository, properties);
    }

    @ParameterizedTest
    @ValueSource(strings = {"consumer", "CONSUMER", "Consumer"})
    void consumerProtocolOmitsClassicOnlySessionProperties(String groupProtocol) throws SQLException {
        ConsumerSettings<String, byte[]> settings = factory(groupProtocol).consumerSettings(BROKER_ID);

        Map<String, String> otherProperties = settings.otherProperties();
        assertThat(otherProperties)
                .doesNotContainKey("session.timeout.ms")
                .doesNotContainKey("heartbeat.interval.ms");
        // group.protocol itself is still forwarded unconditionally.
        assertThat(otherProperties).containsEntry("group.protocol", groupProtocol);
    }

    @Test
    void classicProtocolSetsSessionAndHeartbeatTimeouts() throws SQLException {
        ConsumerSettings<String, byte[]> settings = factory("classic").consumerSettings(BROKER_ID);

        Map<String, String> otherProperties = settings.otherProperties();
        assertThat(otherProperties)
                .containsEntry("session.timeout.ms", "180000")
                .containsEntry("heartbeat.interval.ms", "30000")
                .containsEntry("group.protocol", "classic");
    }

    /**
     * {@code max.poll.interval.ms} must be configured (not left at Kafka's 5-minute
     * default) and comfortably above the realistic lock-hold durations of
     * compaction/retention/snapshot operations contending with
     * {@code KnownEntitiesRepository.upsertBatch()} on the shared DuckDB connection — see
     * {@code docs/write-resilience.md} "Kafka consumer poll interval vs. shared-connection
     * lock contention" and {@link JoxetteProperties.Kafka#getMaxPollIntervalMs()}.
     * The floor here (10 minutes) is deliberately looser than the 15-minute default so
     * this test asserts the safety-margin property, not the exact tuned value.
     */
    @Test
    void maxPollIntervalMsIsConfiguredWellAboveKafkaDefault() throws SQLException {
        ConsumerSettings<String, byte[]> settings = factory("consumer").consumerSettings(BROKER_ID);

        Map<String, String> otherProperties = settings.otherProperties();
        assertThat(otherProperties).containsKey("max.poll.interval.ms");
        int configuredMs = Integer.parseInt(otherProperties.get("max.poll.interval.ms"));
        assertThat(configuredMs)
                .as("max.poll.interval.ms must be raised well above Kafka's 5-minute default "
                        + "(300000) to tolerate shared-connection lock contention")
                .isGreaterThan(600_000);
    }

    @Test
    void maxPollIntervalMsIsConfigurable() throws SQLException {
        // Build a factory with an overridden value to prove the property actually flows
        // through to the consumer settings, rather than being a hardcoded literal.
        JoxetteProperties properties = new JoxetteProperties();
        properties.getKafka().setGroupProtocol("consumer");
        properties.getKafka().setConsumerGroup("joxette-recorder");
        properties.getKafka().setMaxPollIntervalMs(1_200_000);
        when(brokerRepository.resolveBroker(BROKER_ID)).thenReturn(new BrokerConfig(
                BROKER_ID, "localhost:9092", "PLAINTEXT",
                null, null, null, null, null, null, null));
        BrokerConnectionFactory overridden = new BrokerConnectionFactory(brokerRepository, properties);

        ConsumerSettings<String, byte[]> settings = overridden.consumerSettings(BROKER_ID);

        assertThat(settings.otherProperties()).containsEntry("max.poll.interval.ms", "1200000");
    }
}

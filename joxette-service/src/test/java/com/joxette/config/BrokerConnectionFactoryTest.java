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
}

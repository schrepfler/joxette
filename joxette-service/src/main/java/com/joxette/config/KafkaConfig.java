package com.joxette.config;

import com.softwaremill.jox.kafka.ConsumerSettings;
import com.joxette.management.BrokerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Base Kafka client configuration for the Joxette recording pipeline.
 *
 * <p>The recording pipeline uses raw Kafka consumer instances (not Spring Kafka),
 * configured via {@link ConsumerSettings} from {@code com.softwaremill.jox:kafka}.
 *
 * <p>Both beans delegate to {@link BrokerConnectionFactory} so that security
 * properties (SASL, SSL) are assembled from the stored {@link BrokerConfig}
 * rather than hard-coded here.
 */
@Configuration
public class KafkaConfig {

    /**
     * Base consumer settings shared by all per-topic recorder instances
     * created by the recording pipeline.
     *
     * <p>Each {@link com.joxette.recording.TopicRecorder} derives its own settings
     * from this base via {@link ConsumerSettings#groupId} and
     * {@link ConsumerSettings#autoOffsetReset}, overriding per topic.
     */
    @Bean
    public ConsumerSettings<String, byte[]> baseKafkaConsumerSettings(BrokerConnectionFactory f) {
        return f.consumerSettings(BrokerConfig.DEFAULT_BROKER_ID);
    }

    /**
     * Shared {@link AdminClient} used by the health endpoint to compute
     * consumer lag. Destroyed automatically by Spring on context close.
     */
    @Bean(destroyMethod = "close")
    public AdminClient kafkaAdminClient(BrokerConnectionFactory f) {
        return f.adminClient(BrokerConfig.DEFAULT_BROKER_ID);
    }
}

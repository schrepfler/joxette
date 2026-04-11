package com.joxette.config;

import com.joxette.kafka.ConsumerSettings;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Base Kafka client configuration for the Joxette recording pipeline.
 *
 * <p>The recording pipeline uses raw Kafka consumer instances (not Spring Kafka),
 * configured via {@link ConsumerSettings} from {@code com.joxette.kafka} — a
 * local package that mirrors the API of the unpublished
 * {@code com.softwaremill.jox:kafka} module. See {@link ConsumerSettings} for the
 * migration path once that module is published.
 */
@Configuration
public class KafkaConfig {

    /**
     * Base consumer settings shared by all per-topic {@link com.joxette.kafka.KafkaSource}
     * instances created by the recording pipeline.
     *
     * <p>Each {@link com.joxette.recording.TopicRecorder} derives its own settings
     * from this base via {@link ConsumerSettings#withProperty}, overriding
     * {@code group.id} (and optionally {@code auto.offset.reset}) per topic.
     */
    @Bean
    public ConsumerSettings<String, byte[]> baseKafkaConsumerSettings(JoxetteProperties properties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        // Disable auto-commit; the pipeline commits offsets explicitly after a
        // successful DuckLake write to guarantee at-least-once semantics.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return ConsumerSettings.create(props, new StringDeserializer(), new ByteArrayDeserializer());
    }

    /**
     * Shared {@link AdminClient} used by the health endpoint to compute
     * consumer lag. Destroyed automatically by Spring on context close.
     */
    @Bean(destroyMethod = "close")
    public AdminClient kafkaAdminClient(JoxetteProperties properties) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        cfg.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_000);
        cfg.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5_000);
        return AdminClient.create(cfg);
    }
}

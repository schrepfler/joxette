package com.joxette.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Base Kafka client configuration for the Jox Kafka integration.
 *
 * <p>Jox Kafka uses raw {@link org.apache.kafka.clients.consumer.KafkaConsumer}
 * instances (not Spring Kafka), so we expose property maps that the recording
 * pipeline can use to construct per-topic consumers.
 *
 * <p>Message keys and values are consumed as raw bytes and decoded later by the
 * routing and storage layers (e.g., JSON parsing for entity extraction).
 */
@Configuration
public class KafkaConfig {

    /**
     * Base consumer properties shared by all per-topic {@code KafkaConsumer}
     * instances created by the recording pipeline.
     *
     * <p>Each {@code TopicRecorder} will create its own consumer with these base
     * properties, overriding {@code group.id} per topic as needed.
     */
    @Bean
    public Map<String, Object> baseKafkaConsumerProperties(JoxetteProperties properties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        // Disable auto-commit; the pipeline commits offsets explicitly after a
        // successful DuckLake write to guarantee at-least-once semantics.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // Use the Kafka producer timestamp as the canonical message timestamp.
        props.put(ConsumerConfig.LOG_TIMESTAMP_TYPE_CONFIG, "CreateTime");
        return props;
    }
}

package com.joxette.replay.sink.kafka;

import com.joxette.config.BrokerConnectionFactory;
import com.joxette.management.BrokerConfig;
import com.joxette.metrics.JoxetteMetrics;
import com.joxette.replay.sink.RecordSink;
import com.softwaremill.jox.kafka.ProducerSettings;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves a {@link RecordSink} bound to a specific broker id, constructing
 * (and caching) a {@link KafkaProducer} per broker.
 *
 * <p>The engine consumes sinks as single-destination: each call to
 * {@link #forBroker(String)} returns a sink that sends to the resolved broker.
 * Multi-broker routing lives here — the engine itself stays broker-agnostic so
 * it remains reusable outside the Spring service.
 *
 * <p>Kafka producers are expensive to create, so one is kept per broker for the
 * lifetime of the application and closed in {@link #shutdown()}.
 */
@Component
public class KafkaRecordSinkFactory {

    private static final Logger log = LoggerFactory.getLogger(KafkaRecordSinkFactory.class);

    private final BrokerConnectionFactory brokerFactory;
    private final JoxetteMetrics joxetteMetrics;
    private final Map<String, KafkaProducer<byte[], byte[]>> producers = new ConcurrentHashMap<>();
    private final Map<String, RecordSink> sinks = new ConcurrentHashMap<>();

    public KafkaRecordSinkFactory(BrokerConnectionFactory brokerFactory, JoxetteMetrics joxetteMetrics) {
        this.brokerFactory  = brokerFactory;
        this.joxetteMetrics = joxetteMetrics;
    }

    /**
     * Returns a sink bound to {@code brokerId}, or the default broker when
     * {@code brokerId} is null.
     */
    public RecordSink forBroker(String brokerId) {
        String id = brokerId != null ? brokerId : BrokerConfig.DEFAULT_BROKER_ID;
        return sinks.computeIfAbsent(id, this::buildSink);
    }

    private RecordSink buildSink(String id) {
        KafkaProducer<byte[], byte[]> producer = producers.computeIfAbsent(id, this::buildProducer);
        return new KafkaRecordSink(producer);
    }

    private KafkaProducer<byte[], byte[]> buildProducer(String id) {
        ProducerSettings<byte[], byte[]> settings = brokerFactory.producerSettings(id);
        KafkaProducer<byte[], byte[]> producer = settings.toProducer();
        joxetteMetrics.bindKafkaProducerMetrics(producer.metrics(), id);
        return producer;
    }

    @PreDestroy
    public void shutdown() {
        log.info("KafkaRecordSinkFactory closing {} producer(s)", producers.size());
        producers.values().forEach(p -> {
            try { p.close(); } catch (Exception e) {
                log.warn("Failed to close Kafka producer", e);
            }
        });
        producers.clear();
        sinks.clear();
        log.info("KafkaRecordSinkFactory closed");
    }
}

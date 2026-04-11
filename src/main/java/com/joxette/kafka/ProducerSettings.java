package com.joxette.kafka;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Immutable configuration descriptor for a Kafka producer.
 *
 * <p>Mirrors the {@code ProducerSettings} class from the unpublished
 * {@code com.softwaremill.jox:kafka} module. See {@link ConsumerSettings} for
 * the migration path once that module is published to Maven Central.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
public final class ProducerSettings<K, V> {

    private final Map<String, Object> properties;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    private ProducerSettings(
            Map<String, Object> properties,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    /**
     * Creates a new {@code ProducerSettings} from the given base properties and
     * explicit serializer instances.
     *
     * <p>The {@code properties} map should NOT contain {@code key.serializer}
     * or {@code value.serializer} class configs; pass serializers directly as
     * arguments. {@link KafkaSink} constructs the producer via the three-argument
     * {@code KafkaProducer} constructor so the explicit instances take effect.
     */
    public static <K, V> ProducerSettings<K, V> create(
            Map<String, Object> properties,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer) {
        return new ProducerSettings<>(properties, keySerializer, valueSerializer);
    }

    /**
     * Returns a new {@code ProducerSettings} with one property overridden
     * (copy-on-write — the original is not mutated).
     */
    public ProducerSettings<K, V> withProperty(String key, Object value) {
        Map<String, Object> copy = new HashMap<>(properties);
        copy.put(key, value);
        return new ProducerSettings<>(copy, keySerializer, valueSerializer);
    }

    /**
     * Returns an unmodifiable view of the Kafka properties map.
     * Pass this to the three-argument {@link org.apache.kafka.clients.producer.KafkaProducer}
     * constructor together with {@link #keySerializer()} and {@link #valueSerializer()}.
     */
    public Map<String, Object> toProperties() {
        return properties;
    }

    public Serializer<K> keySerializer() {
        return keySerializer;
    }

    public Serializer<V> valueSerializer() {
        return valueSerializer;
    }
}

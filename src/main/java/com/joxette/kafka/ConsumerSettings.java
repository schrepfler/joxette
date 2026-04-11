package com.joxette.kafka;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Immutable configuration descriptor for a Kafka consumer.
 *
 * <p>Mirrors the {@code ConsumerSettings} class from the unpublished
 * {@code com.softwaremill.jox:kafka} module (v0.5.3 exists in source at
 * <a href="https://github.com/softwaremill/jox/tree/master/kafka">github.com/softwaremill/jox</a>
 * but has not been published to Maven Central). When that module is released,
 * delete {@code com.joxette.kafka} and update the two import sites.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
public final class ConsumerSettings<K, V> {

    private final Map<String, Object> properties;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    private ConsumerSettings(
            Map<String, Object> properties,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    /**
     * Creates a new {@code ConsumerSettings} from the given base properties and
     * explicit deserializer instances.
     *
     * <p>The {@code properties} map should NOT contain {@code key.deserializer}
     * or {@code value.deserializer} class configs; pass deserializers directly
     * as arguments. {@link KafkaSource} constructs the consumer via the
     * three-argument {@code KafkaConsumer} constructor so the explicit instances
     * take effect.
     */
    public static <K, V> ConsumerSettings<K, V> create(
            Map<String, Object> properties,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        return new ConsumerSettings<>(properties, keyDeserializer, valueDeserializer);
    }

    /**
     * Returns a new {@code ConsumerSettings} with one property overridden
     * (copy-on-write — the original is not mutated).
     */
    public ConsumerSettings<K, V> withProperty(String key, Object value) {
        Map<String, Object> copy = new HashMap<>(properties);
        copy.put(key, value);
        return new ConsumerSettings<>(copy, keyDeserializer, valueDeserializer);
    }

    /**
     * Returns an unmodifiable view of the Kafka properties map.
     * Pass this to the three-argument {@link org.apache.kafka.clients.consumer.KafkaConsumer}
     * constructor together with {@link #keyDeserializer()} and {@link #valueDeserializer()}.
     */
    public Map<String, Object> toProperties() {
        return properties;
    }

    public Deserializer<K> keyDeserializer() {
        return keyDeserializer;
    }

    public Deserializer<V> valueDeserializer() {
        return valueDeserializer;
    }
}

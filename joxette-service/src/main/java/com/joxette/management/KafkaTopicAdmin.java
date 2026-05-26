package com.joxette.management;

import com.joxette.api.error.ForbiddenException;
import com.joxette.api.error.UpstreamUnavailableException;
import com.joxette.config.BrokerConnectionFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Thin wrapper around the Kafka {@link AdminClient} for topic existence checks
 * and on-demand topic creation.
 *
 * <p>Each public method opens its own {@link AdminClient} and closes it before
 * returning — callers never hold an open client.
 */
@Component
public class KafkaTopicAdmin {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicAdmin.class);
    private static final int TIMEOUT_SEC = 10;

    private final BrokerConnectionFactory connectionFactory;

    public KafkaTopicAdmin(BrokerConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * Returns {@code true} if the named topic exists on the broker.
     *
     * @param brokerId broker ID (may be {@code null} for default)
     * @param topic    topic name
     */
    public boolean exists(String brokerId, String topic) {
        try (AdminClient admin = connectionFactory.adminClient(brokerId)) {
            Set<String> names = admin.listTopics()
                    .names()
                    .get(TIMEOUT_SEC, TimeUnit.SECONDS);
            return names.contains(topic);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw UpstreamUnavailableException.broker(brokerId, e);
        } catch (ExecutionException | TimeoutException e) {
            throw UpstreamUnavailableException.broker(brokerId, e);
        }
    }

    /**
     * Creates a Kafka topic with the given partition count and replication factor.
     *
     * <p>If the caller's Kafka credentials lack the {@code CREATE} ACL on the topic,
     * a {@link ForbiddenException} is thrown rather than a generic upstream error.
     *
     * @param brokerId          broker ID (may be {@code null} for default)
     * @param topic             topic name
     * @param numPartitions     number of partitions; 1 if {@code null}
     * @param replicationFactor replication factor; 1 if {@code null}
     */
    public void createTopic(String brokerId, String topic,
                            Integer numPartitions, Short replicationFactor) {
        int parts = numPartitions != null ? numPartitions : 1;
        short rf   = replicationFactor != null ? replicationFactor : 1;

        NewTopic newTopic = new NewTopic(topic, Optional.of(parts), Optional.of(rf));

        try (AdminClient admin = connectionFactory.adminClient(brokerId)) {
            admin.createTopics(List.of(newTopic))
                    .all()
                    .get(TIMEOUT_SEC, TimeUnit.SECONDS);
            log.info("Created Kafka topic '{}' with {} partition(s), RF {}", topic, parts, rf);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TopicAuthorizationException) {
                throw ForbiddenException.kafkaTopicCreate(topic, cause);
            }
            throw UpstreamUnavailableException.broker(brokerId, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw UpstreamUnavailableException.broker(brokerId, e);
        } catch (TimeoutException e) {
            throw UpstreamUnavailableException.broker(brokerId, e);
        }
    }
}

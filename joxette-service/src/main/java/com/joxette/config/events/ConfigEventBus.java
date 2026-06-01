package com.joxette.config.events;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.pubsub.Topic;
import org.springframework.stereotype.Component;

/**
 * Cluster-wide pub/sub bus for configuration change events.
 *
 * <p>Backed by Pekko's typed {@link Topic} actor, which is local in single-node
 * mode and distributes across cluster members in multi-node mode (Phase 2).
 *
 * <p>Publishers call {@link #publishTopicConfig(TopicConfigChanged)} or
 * {@link #publishEntityConfig(EntityConfigChanged)} from any Spring bean.
 * Subscribers register their actor ref via
 * {@link #subscribeToTopicConfig(ActorRef)} / {@link #subscribeToEntityConfig(ActorRef)}.
 */
@Component
public class ConfigEventBus {

    private final ActorRef<Topic.Command<TopicConfigChanged>>  topicConfigTopic;
    private final ActorRef<Topic.Command<EntityConfigChanged>> entityConfigTopic;

    public ConfigEventBus(ActorSystem<Void> system) {
        topicConfigTopic  = system.systemActorOf(
                Topic.create(TopicConfigChanged.class,  "topic-config-changed"),
                "topic-config-changed-bus",
                org.apache.pekko.actor.typed.Props.empty());
        entityConfigTopic = system.systemActorOf(
                Topic.create(EntityConfigChanged.class, "entity-config-changed"),
                "entity-config-changed-bus",
                org.apache.pekko.actor.typed.Props.empty());
    }

    public void publishTopicConfig(TopicConfigChanged event) {
        topicConfigTopic.tell(Topic.publish(event));
    }

    public void publishEntityConfig(EntityConfigChanged event) {
        entityConfigTopic.tell(Topic.publish(event));
    }

    public void subscribeToTopicConfig(ActorRef<TopicConfigChanged> subscriber) {
        topicConfigTopic.tell(Topic.subscribe(subscriber));
    }

    public void subscribeToEntityConfig(ActorRef<EntityConfigChanged> subscriber) {
        entityConfigTopic.tell(Topic.subscribe(subscriber));
    }

    public void unsubscribeFromTopicConfig(ActorRef<TopicConfigChanged> subscriber) {
        topicConfigTopic.tell(Topic.unsubscribe(subscriber));
    }

    public void unsubscribeFromEntityConfig(ActorRef<EntityConfigChanged> subscriber) {
        entityConfigTopic.tell(Topic.unsubscribe(subscriber));
    }
}

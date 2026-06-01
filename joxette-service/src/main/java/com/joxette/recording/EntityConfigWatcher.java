package com.joxette.recording;

import com.joxette.config.events.ConfigEventBus;
import com.joxette.config.events.EntityConfigChanged;
import com.joxette.replay.MessageRouter;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.sql.SQLException;

/**
 * Subscribes to {@link EntityConfigChanged} pub/sub events and reloads the local
 * {@link MessageRouter} routing tables immediately on every change.
 *
 * <p>This ensures every node's in-memory entity routing is consistent within
 * milliseconds of a catalog write, regardless of which node received the HTTP request.
 */
@Component
public class EntityConfigWatcher {

    private static final Logger log = LoggerFactory.getLogger(EntityConfigWatcher.class);

    private final MessageRouter messageRouter;
    private final ConfigEventBus eventBus;
    private final ActorSystem<Void> system;

    public EntityConfigWatcher(MessageRouter messageRouter,
                                ConfigEventBus eventBus,
                                ActorSystem<Void> system) {
        this.messageRouter = messageRouter;
        this.eventBus      = eventBus;
        this.system        = system;
    }

    @PostConstruct
    public void subscribe() {
        ActorRef<EntityConfigChanged> subscriber = system.systemActorOf(
                entityWatcher(messageRouter),
                "entity-config-watcher",
                org.apache.pekko.actor.typed.Props.empty());
        eventBus.subscribeToEntityConfig(subscriber);
        log.info("EntityConfigWatcher: subscribed to entity config changes");
    }

    private static Behavior<EntityConfigChanged> entityWatcher(MessageRouter router) {
        return Behaviors.receive((ctx, event) -> {
            LoggerFactory.getLogger(EntityConfigWatcher.class)
                    .debug("EntityConfigWatcher: reloading router (event={}/{})",
                            event.changeType(), event.entityType());
            try {
                router.reload();
            } catch (SQLException e) {
                LoggerFactory.getLogger(EntityConfigWatcher.class)
                        .warn("EntityConfigWatcher: router reload failed: {}", e.getMessage());
            }
            return Behaviors.same();
        });
    }
}

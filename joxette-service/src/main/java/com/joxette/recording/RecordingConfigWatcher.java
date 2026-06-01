package com.joxette.recording;

import com.joxette.config.JoxetteProperties;
import com.joxette.config.events.ConfigEventBus;
import com.joxette.config.events.TopicConfigChanged;
import com.joxette.management.ConfigRepository;
import com.joxette.management.TopicConfig;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.TimerScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

/**
 * Watches the catalog for topic config changes and reconciles running Kafka consumers.
 *
 * <p>Replaces {@code RecordingStartupRunner}. Uses two complementary mechanisms:
 *
 * <h2>Option A — Polling (catalog as source of truth)</h2>
 * A Pekko timer fires every {@value #RECONCILE_INTERVAL_SECONDS} seconds, reads the
 * current {@code topic_configs} from the catalog, and sends
 * {@link RecordingCoordinatorActor.ReconcileTopics} to the local coordinator.
 * This guarantees convergence even if pub/sub notifications are missed.
 *
 * <h2>Option B — Pub/sub (immediate notification)</h2>
 * Subscribes to the {@code TopicConfigChanged} Pekko {@code Topic} published by
 * {@link com.joxette.management.TopicController} after every catalog write.
 * On receipt, triggers an immediate reconciliation without waiting for the poll tick.
 *
 * <p>Does nothing when {@code joxette.recording.enabled=false}.
 */
@Component
public class RecordingConfigWatcher implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(RecordingConfigWatcher.class);
    static final int RECONCILE_INTERVAL_SECONDS = 30;

    private final JoxetteProperties properties;
    private final ConfigRepository configRepository;
    private final RecordingCoordinator coordinator;
    private final ActorRef<RecordingCoordinatorActor.CoordinatorCommand> coordinatorActor;
    private final ConfigEventBus eventBus;
    private final ActorSystem<Void> system;

    public RecordingConfigWatcher(
            JoxetteProperties properties,
            ConfigRepository configRepository,
            @Lazy RecordingCoordinator coordinator,
            ActorRef<RecordingCoordinatorActor.CoordinatorCommand> recordingCoordinatorActor,
            ConfigEventBus eventBus,
            ActorSystem<Void> system) {
        this.properties       = properties;
        this.configRepository = configRepository;
        this.coordinator      = coordinator;
        this.coordinatorActor = recordingCoordinatorActor;
        this.eventBus         = eventBus;
        this.system           = system;
    }

    // -------------------------------------------------------------------------
    // Watcher actor — lives for the service lifetime when recording is enabled
    // -------------------------------------------------------------------------

    sealed interface Cmd {}
    private record Tick() implements Cmd {}
    private record ConfigEvent(TopicConfigChanged event) implements Cmd {}

    static Behavior<Cmd> watcherBehavior(
            ConfigRepository configRepository,
            ActorRef<RecordingCoordinatorActor.CoordinatorCommand> coordinatorActor,
            ActorSystem<Void> system) {

        return Behaviors.setup(ctx ->
                Behaviors.withTimers(timers -> {
                    timers.startTimerWithFixedDelay("reconcile-tick",
                            new Tick(),
                            Duration.ofSeconds(RECONCILE_INTERVAL_SECONDS));
                    return watching(ctx, timers, configRepository, coordinatorActor, system);
                }));
    }

    private static Behavior<Cmd> watching(
            ActorContext<Cmd> ctx,
            TimerScheduler<Cmd> timers,
            ConfigRepository configRepository,
            ActorRef<RecordingCoordinatorActor.CoordinatorCommand> coordinatorActor,
            ActorSystem<Void> system) {

        return Behaviors.receive(Cmd.class)
                .onMessage(Tick.class, msg -> {
                    reconcile(ctx, configRepository, coordinatorActor, system, "poll");
                    return Behaviors.same();
                })
                .onMessage(ConfigEvent.class, msg -> {
                    reconcile(ctx, configRepository, coordinatorActor, system,
                            "event:" + msg.event().changeType() + ":" + msg.event().topic());
                    return Behaviors.same();
                })
                .build();
    }

    private static void reconcile(
            ActorContext<Cmd> ctx,
            ConfigRepository configRepository,
            ActorRef<RecordingCoordinatorActor.CoordinatorCommand> coordinatorActor,
            ActorSystem<Void> system,
            String trigger) {
        List<TopicConfig> desired;
        try {
            desired = configRepository.listTopics();
        } catch (SQLException e) {
            log.warn("RecordingConfigWatcher: failed to read topic_configs (trigger={}): {}", trigger, e.getMessage());
            return;
        }
        AskPattern.<RecordingCoordinatorActor.CoordinatorCommand, RecordingCoordinatorActor.ReconcileReply>ask(
                coordinatorActor,
                replyTo -> new RecordingCoordinatorActor.ReconcileTopics(desired, replyTo),
                Duration.ofSeconds(10),
                system.scheduler()
        ).whenComplete((reply, ex) -> {
            if (ex != null) {
                log.warn("RecordingConfigWatcher: reconcile ask failed (trigger={}): {}", trigger, ex.getMessage());
            } else {
                log.debug("RecordingConfigWatcher: reconcile done (trigger={}) started={} stopped={} unchanged={}",
                        trigger, reply.started(), reply.stopped(), reply.unchanged());
            }
        });
    }

    // -------------------------------------------------------------------------
    // Spring ApplicationRunner — spawns the watcher actor on startup
    // -------------------------------------------------------------------------

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.getRecording().isEnabled()) {
            log.info("RecordingConfigWatcher: recording disabled — Kafka consumers will not start");
            return;
        }

        // Spawn the watcher actor
        ActorRef<Cmd> watcher = system.systemActorOf(
                watcherBehavior(configRepository, coordinatorActor, system),
                "recording-config-watcher",
                org.apache.pekko.actor.typed.Props.empty());

        // Subscribe to pub/sub events (Option B) — forward to watcher
        ActorRef<TopicConfigChanged> eventAdapter = system.systemActorOf(
                Behaviors.<TopicConfigChanged>receive((ctx, event) -> {
                    watcher.tell(new ConfigEvent(event));
                    return Behaviors.same();
                }),
                "topic-config-event-adapter",
                org.apache.pekko.actor.typed.Props.empty());

        eventBus.subscribeToTopicConfig(eventAdapter);

        // Trigger initial reconciliation immediately (don't wait for first tick)
        watcher.tell(new Tick());

        log.info("RecordingConfigWatcher: started (poll interval={}s, pub/sub enabled)", RECONCILE_INTERVAL_SECONDS);
    }
}

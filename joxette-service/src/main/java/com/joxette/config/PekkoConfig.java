package com.joxette.config;

import com.joxette.compaction.CompactionService;
import com.joxette.compaction.CompactionSingletonActor;
import com.joxette.management.ConfigRepository;
import com.joxette.recording.DuckLakeWriteChannel;
import com.joxette.recording.RecordingCoordinator;
import com.joxette.recording.RecordingCoordinatorActor;
import com.joxette.replay.KnownEntitiesRepository;
import com.joxette.replay.MessageRouter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import jakarta.annotation.PreDestroy;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.typed.ClusterSingleton;
import org.apache.pekko.cluster.typed.SingletonActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Bootstraps the Pekko {@link ActorSystem} and exposes it as a Spring bean.
 *
 * <h2>Design</h2>
 * <ul>
 *   <li>The actor system is named {@code "joxette"} — this matches the
 *       {@code pekko://joxette@…} seed-node addresses in {@code pekko.conf}.</li>
 *   <li>The root behavior is an empty guardian ({@code Behaviors.empty()}) because
 *       all actors are spawned as named children by their respective owner beans
 *       (e.g. {@code ClusterEventListener}).</li>
 *   <li>The remote port can be overridden via {@code PEKKO_REMOTE_PORT} environment
 *       variable or the {@code joxette.pekko.remote-port} Spring property.
 *       Port {@code 0} means the OS picks a free port (useful for local dev and tests).</li>
 *   <li>A {@link Executor} backed by virtual threads is provided for offloading
 *       blocking work (DuckDB, Kafka) from actor mailbox threads via
 *       {@code context.pipeToSelf(CompletableFuture.supplyAsync(…, vtExecutor), …)}.</li>
 * </ul>
 */
@Configuration
public class PekkoConfig {

    private static final Logger log = LoggerFactory.getLogger(PekkoConfig.class);

    @Value("${joxette.pekko.remote-port:0}")
    private int remotePort;

    @Autowired @Lazy private CompactionService compactionService;
    @Autowired private JoxetteProperties joxetteProperties;
    @Autowired @Lazy private ConfigRepository configRepository;
    @Autowired @Lazy private DuckLakeWriteChannel writeChannel;
    @Autowired @Lazy private MessageRouter messageRouter;
    @Autowired @Lazy private KnownEntitiesRepository knownEntitiesRepository;
    @Autowired @Lazy private RecordingCoordinator recordingCoordinator;

    private ActorSystem<Void> actorSystem;

    @Bean
    public ActorSystem<Void> actorSystem() {
        Config base = ConfigFactory.load("pekko");
        Config overrides = remotePort > 0
                ? ConfigFactory.parseString(
                        "pekko.remote.artery.canonical.port = " + remotePort)
                : ConfigFactory.empty();
        Config config = overrides.withFallback(base);

        actorSystem = ActorSystem.create(Behaviors.empty(), "joxette", config);
        log.info("Pekko ActorSystem 'joxette' started (remote port={})",
                remotePort > 0 ? remotePort : "OS-assigned");
        return actorSystem;
    }

    /**
     * Virtual-thread executor for offloading blocking work (DuckDB, Kafka, S3)
     * from Pekko actor mailbox threads via {@code context.pipeToSelf}.
     */
    @Bean(name = "pekkoVtExecutor")
    public Executor pekkoVtExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * Registers the compaction singleton and returns its proxy reference.
     *
     * <p>Exactly one {@link CompactionSingletonActor} runs across the cluster at any
     * time.  Pekko migrates it automatically on node failure.  All callers (scheduler
     * and REST controller) communicate via this proxy, which buffers messages during
     * migration so callers need no awareness of re-election.
     */
    @Bean
    public ActorRef<CompactionSingletonActor.CompactionCommand> compactionSingleton(
            ActorSystem<Void> system,
            @org.springframework.beans.factory.annotation.Qualifier("pekkoVtExecutor") Executor vtExecutor) {
        ClusterSingleton singleton = ClusterSingleton.get(system);
        return singleton.init(
                SingletonActor.of(
                        CompactionSingletonActor.create(compactionService, joxetteProperties, vtExecutor),
                        "compaction-singleton"));
    }

    /**
     * Spawns the recording coordinator actor and exposes it as a Spring bean.
     *
     * <p>The coordinator manages per-topic {@link com.joxette.recording.TopicLifecycleActor}
     * children.  Each child runs with a Pekko exponential-backoff supervisor, replacing
     * the Resilience4j retry used by the old {@code RecordingCoordinator}.
     */
    @Bean
    public ActorRef<RecordingCoordinatorActor.CoordinatorCommand> recordingCoordinatorActor(
            ActorSystem<Void> system,
            BrokerConnectionFactory brokerConnectionFactory,
            @Qualifier("pekkoVtExecutor") Executor vtExecutor) {
        return system.systemActorOf(
                RecordingCoordinatorActor.create(
                        joxetteProperties,
                        brokerConnectionFactory,
                        configRepository,
                        writeChannel,
                        messageRouter,
                        knownEntitiesRepository,
                        vtExecutor),
                "recording-coordinator",
                org.apache.pekko.actor.typed.Props.empty());
    }

    /**
     * Ordered shutdown: recorders → write channel → actor system.
     *
     * <p>The order matters:
     * <ol>
     *   <li>Stop all topic recorders first — each stop() ask goes to the coordinator actor,
     *       so the actor system must still be alive at this point.</li>
     *   <li>Drain the write channel — lets any in-flight DuckDB batches complete before
     *       the catalog connection closes.</li>
     *   <li>Terminate the actor system last — coordinator actor is no longer needed.</li>
     * </ol>
     *
     * <p>Pekko's own JVM shutdown hook is disabled in {@code pekko.conf}
     * ({@code coordinated-shutdown.run-by-jvm-shutdown-hook = off}) so it cannot
     * race with Spring's hook and kill actors before step 1 completes.
     */
    @PreDestroy
    void shutdown() {
        // Step 1: stop all topic recorders (asks go to the recording-coordinator actor).
        try {
            recordingCoordinator.stopAll();
        } catch (Exception e) {
            log.warn("RecordingCoordinator.stopAll() failed during shutdown: {}", e.getMessage());
        }

        // Step 2: drain the write channel so in-flight DuckDB writes complete.
        try {
            writeChannel.stop();
        } catch (Exception e) {
            log.warn("DuckLakeWriteChannel.stop() failed during shutdown: {}", e.getMessage());
        }

        // Step 3: terminate the Pekko actor system.
        if (actorSystem != null) {
            log.info("Terminating Pekko ActorSystem 'joxette'…");
            actorSystem.terminate();
            try {
                actorSystem.getWhenTerminated().toCompletableFuture()
                        .get(10, java.util.concurrent.TimeUnit.SECONDS);
            } catch (Exception e) {
                log.warn("Pekko ActorSystem did not terminate cleanly within 10 s: {}", e.getMessage());
            }
        }
    }
}

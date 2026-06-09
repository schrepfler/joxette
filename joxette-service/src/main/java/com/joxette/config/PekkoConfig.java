package com.joxette.config;

import com.joxette.compaction.CompactionService;
import com.joxette.compaction.CompactionSingletonActor;
import com.joxette.management.ConfigRepository;
import com.joxette.recording.DuckLakeWriteChannel;
import com.joxette.recording.RecordingCoordinator;
import com.joxette.recording.RecordingCoordinatorActor;
import com.joxette.replay.KnownEntitiesRepository;
import com.joxette.replay.MessageRouter;
import com.joxette.replay.ReplayCoordinatorActor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import jakarta.annotation.PreDestroy;
import org.apache.pekko.actor.Address;
import org.apache.pekko.actor.CoordinatedShutdown;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.typed.Cluster;
import org.apache.pekko.cluster.typed.ClusterSingleton;
import org.apache.pekko.cluster.typed.Join;
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
import java.util.concurrent.TimeUnit;

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
    // Stored directly (not via @Lazy) so shutdown() holds the real ActorRef, not a Spring proxy.
    private ActorRef<com.joxette.replay.ReplayCoordinatorActor.Cmd> replayCoordinatorRef;

    /**
     * {@code destroyMethod = ""} suppresses Spring's auto-detection of
     * {@code ActorSystem.close()}.  Without this, Spring calls {@code close()}
     * during context teardown, which triggers Pekko's {@code CoordinatedShutdown}
     * BEFORE {@link #shutdown()} runs — leaving the recording-coordinator actor
     * already terminated when step 1 tries to send it {@code StopAll}.
     * {@link #shutdown()} handles the full ordered teardown explicitly.
     */
    @Bean(destroyMethod = "")
    public ActorSystem<Void> actorSystem() throws InterruptedException {
        JoxetteProperties.Clustering clustering = joxetteProperties.getClustering();
        boolean pekkoManagement =
                clustering.getMode() == JoxetteProperties.ClusteringMode.PEKKO_MANAGEMENT;

        Config config = buildConfig(clustering, pekkoManagement);

        actorSystem = ActorSystem.create(Behaviors.empty(), "joxette", config);
        log.info("Pekko ActorSystem 'joxette' started (mode={}, remote port={})",
                clustering.getMode(), remotePort > 0 ? remotePort : "OS-assigned");

        if (pekkoManagement) {
            startClusterBootstrap();
            // Membership is driven by Cluster Bootstrap discovery; do NOT self-join
            // (a self-join would form a separate one-member cluster and defeat
            // discovery). Reaching Up may take longer than one node, so we do not
            // block on it here — ClusterSingleton/coordinator actors tolerate a
            // not-yet-Up cluster and activate once membership converges.
        } else {
            selfJoin();
        }

        return actorSystem;
    }

    /**
     * Assembles the effective HOCON config. The base is {@code pekko.conf}; the
     * remote port override applies in both modes. In {@code PEKKO_MANAGEMENT}
     * mode an overlay wires kubernetes-api discovery, the management port, the
     * canonical hostname from {@code POD_IP}, and the lease-backed split-brain
     * resolver (replacing the {@code keep-majority} strategy used for self-join).
     */
    private Config buildConfig(JoxetteProperties.Clustering clustering, boolean pekkoManagement) {
        Config base = ConfigFactory.load("pekko");
        Config overlay = ConfigFactory.empty();
        if (remotePort > 0) {
            overlay = overlay.withFallback(ConfigFactory.parseString(
                    "pekko.remote.artery.canonical.port = " + remotePort));
        }
        if (pekkoManagement) {
            overlay = overlay.withFallback(buildManagementOverlay(clustering));
        }
        return overlay.withFallback(base);
    }

    private Config buildManagementOverlay(JoxetteProperties.Clustering clustering) {
        return buildManagementOverlay(clustering, System.getenv("POD_IP"));
    }

    /**
     * Pure, testable overlay builder. {@code podIp} is passed explicitly (from the
     * downward-API {@code POD_IP} env var in production) so the HOCON can be
     * validated in a unit test without environment manipulation.
     */
    static Config buildManagementOverlay(JoxetteProperties.Clustering clustering, String podIp) {
        String hostnameLine = (podIp != null && !podIp.isBlank())
                ? "pekko.remote.artery.canonical.hostname = \"" + podIp + "\"\n"
                : "";
        String labelSelector = clustering.getPodLabelSelector()
                .formatted(clustering.getServiceName());
        return ConfigFactory.parseString(
                hostnameLine +
                "pekko.management.http.port = " + clustering.getManagementPort() + "\n" +
                "pekko.management.http.hostname = \"0.0.0.0\"\n" +
                "pekko.management.cluster.bootstrap.contact-point-discovery {\n" +
                "  discovery-method = kubernetes-api\n" +
                "  service-name = \"" + clustering.getServiceName() + "\"\n" +
                "  required-contact-point-nr = " + clustering.getRequiredContactPointNr() + "\n" +
                "}\n" +
                "pekko.discovery.kubernetes-api.pod-label-selector = \"" + labelSelector + "\"\n" +
                "pekko.cluster.downing-provider-class = " +
                "  \"org.apache.pekko.cluster.sbr.SplitBrainResolverProvider\"\n" +
                "pekko.cluster.split-brain-resolver.active-strategy = lease-majority\n" +
                "pekko.cluster.split-brain-resolver.lease-majority.lease-implementation = " +
                "  \"pekko.coordination.lease.kubernetes\"\n" +
                "pekko.coordination.lease.kubernetes.lease-class = " +
                "  \"org.apache.pekko.coordination.lease.kubernetes.NativeKubernetesLease\"\n");
    }

    /** Starts the management HTTP server, then Cluster Bootstrap discovery. */
    private void startClusterBootstrap() {
        // Management HTTP must start before ClusterBootstrap — bootstrap registers
        // its contact-point route on the management server.
        org.apache.pekko.management.javadsl.PekkoManagement.get(actorSystem).start();
        org.apache.pekko.management.cluster.bootstrap.ClusterBootstrap.get(actorSystem).start();
        log.info("Pekko Cluster Bootstrap started (management port={}, discovery=kubernetes-api)",
                joxetteProperties.getClustering().getManagementPort());
    }

    /**
     * Self-join so this node forms a single-node cluster immediately.
     * pekko.conf has seed-nodes=[] so without this the node stays in
     * Joining indefinitely, and every cluster-phase timeout fires on shutdown.
     */
    private void selfJoin() throws InterruptedException {
        Cluster cluster = Cluster.get(actorSystem);
        Address self = cluster.selfMember().address();
        cluster.manager().tell(Join.create(self));
        log.info("Pekko self-join issued for {}", self);

        // Wait for self to reach Up so ClusterSingleton and coordinator actor see a live cluster.
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (cluster.selfMember().status().equals(MemberStatus.up())) break;
            Thread.sleep(100);
        }
        log.info("Pekko ActorSystem '{}' cluster Up at {}", actorSystem.name(), self);
    }

    /**
     * Virtual-thread executor for offloading blocking work (DuckDB, Kafka, S3)
     * from Pekko actor mailbox threads via {@code context.pipeToSelf}.
     *
     * <p>{@code destroyMethod = ""} suppresses Spring's auto-detection of
     * {@code ExecutorService.close()}.  Java 21+ {@code close()} calls
     * {@code awaitTermination(MAX_VALUE)} — it would block indefinitely while
     * recorder VTs are still running, preventing {@link #shutdown()} from ever
     * being called (deadlock).  PekkoConfig.shutdown() is responsible for
     * orderly teardown; the executor itself does not need explicit shutdown.
     */
    @Bean(name = "pekkoVtExecutor", destroyMethod = "")
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
            @Qualifier("pekkoVtExecutor") Executor vtExecutor,
            com.joxette.metrics.JoxetteMetrics joxetteMetrics) {
        return system.systemActorOf(
                RecordingCoordinatorActor.create(
                        joxetteProperties,
                        brokerConnectionFactory,
                        configRepository,
                        writeChannel,
                        messageRouter,
                        knownEntitiesRepository,
                        vtExecutor,
                        joxetteMetrics),
                "recording-coordinator",
                org.apache.pekko.actor.typed.Props.empty());
    }

    /**
     * Spawns the replay coordinator actor and exposes it as a Spring bean.
     *
     * <p>Manages per-request {@link com.joxette.replay.ReplayActor} children,
     * replacing the old {@code ActiveReplayTracker} ConcurrentHashMap with a
     * proper actor hierarchy that supports proactive cancellation.
     */
    @Bean
    public ActorRef<ReplayCoordinatorActor.Cmd> replayCoordinatorActor(
            ActorSystem<Void> system,
            com.joxette.metrics.JoxetteMetrics joxetteMetrics) {
        replayCoordinatorRef = system.systemActorOf(
                ReplayCoordinatorActor.create(),
                "replay-coordinator",
                org.apache.pekko.actor.typed.Props.empty());
        // Register active-replays gauge — reads from the coordinator via ask on each scrape
        joxetteMetrics.registerActiveReplaysGauge(() -> {
            try {
                return org.apache.pekko.actor.typed.javadsl.AskPattern.<ReplayCoordinatorActor.Cmd, java.util.List<ReplayCoordinatorActor.ReplaySnapshot>>ask(
                        replayCoordinatorRef,
                        ReplayCoordinatorActor.ListActive::new,
                        java.time.Duration.ofSeconds(1),
                        system.scheduler()
                ).toCompletableFuture().join().size();
            } catch (Exception e) {
                return 0;
            }
        });
        return replayCoordinatorRef;
    }

    /**
     * Ordered shutdown: recorders → replays → write channel → actor system.
     *
     * <p>The order matters:
     * <ol>
     *   <li>Stop all topic recorders — each stop() ask goes to the recording-coordinator actor.</li>
     *   <li>Cancel all active replays — interrupts VTs via the replay-coordinator actor.</li>
     *   <li>Drain the write channel — lets any in-flight DuckDB batches complete before
     *       the catalog connection closes.</li>
     *   <li>Terminate the actor system last.</li>
     * </ol>
     *
     * <p>Pekko's own JVM shutdown hook is disabled in {@code pekko.conf}
     * ({@code coordinated-shutdown.run-by-jvm-shutdown-hook = off}) so it cannot
     * race with Spring's hook and kill actors before step 1 completes.
     */
    @PreDestroy
    void shutdown() {
        long t = System.currentTimeMillis();
        log.info("=== Joxette shutdown begin ===");

        // Step 1: stop all topic recorders (asks go to the recording-coordinator actor).
        try {
            log.info("[shutdown 1/4] stopping all topic recorders...");
            recordingCoordinator.stopAll();
            log.info("[shutdown 1/4] recorders stopped ({} ms)", elapsed(t));
        } catch (Exception e) {
            log.warn("[shutdown 1/4] stopAll failed: {}", e.getMessage());
        }

        // Step 2: cancel any active replay-to-topic VTs so they release the DuckDB connection.
        try {
            log.info("[shutdown 2/4] cancelling active replays...");
            int cancelled = org.apache.pekko.actor.typed.javadsl.AskPattern.<com.joxette.replay.ReplayCoordinatorActor.Cmd, Integer>ask(
                    replayCoordinatorRef,
                    com.joxette.replay.ReplayCoordinatorActor.CancelAll::new,
                    java.time.Duration.ofSeconds(5),
                    actorSystem.scheduler()
            ).toCompletableFuture().join();
            log.info("[shutdown 2/4] {} replay(s) cancelled ({} ms)", cancelled, elapsed(t));
        } catch (Exception e) {
            log.warn("[shutdown 2/4] replay cancel failed: {}", e.getMessage());
        }

        // Step 3: drain the write channel so in-flight DuckDB writes complete.
        try {
            log.info("[shutdown 3/4] draining write channel...");
            writeChannel.stop();
            log.info("[shutdown 3/4] write channel drained ({} ms)", elapsed(t));
        } catch (Exception e) {
            log.warn("[shutdown 3/4] write channel stop failed: {}", e.getMessage());
        }

        // Step 3: run Pekko coordinated-shutdown and wait for it to complete.
        // CoordinatedShutdown.runAll() runs every phase sequentially (cluster-leave →
        // cluster-exiting → … → actor-system-terminate → after-actor-system-terminate)
        // and blocks until all phases finish or their per-phase timeout fires.
        // This is the only call that actually stops Artery transport threads; plain
        // actorSystem.terminate() fires and returns immediately, leaving those threads alive.
        if (actorSystem != null) {
            log.info("[shutdown 4/4] running Pekko coordinated-shutdown...");
            try {
                CoordinatedShutdown.get(actorSystem)
                        .runAll(CoordinatedShutdown.clusterLeavingReason())
                        .toCompletableFuture()
                        .get(20, TimeUnit.SECONDS);
                log.info("[shutdown 4/4] coordinated-shutdown complete ({} ms)", elapsed(t));
            } catch (Exception e) {
                log.warn("[shutdown 4/4] coordinated-shutdown did not finish cleanly: {}", e.getMessage());
            }
        }

        log.info("=== Joxette shutdown complete ({} ms) ===", elapsed(t));
    }

    private static long elapsed(long startMs) {
        return System.currentTimeMillis() - startMs;
    }
}

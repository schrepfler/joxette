package com.joxette.cluster;

import jakarta.annotation.PostConstruct;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.ClusterEvent;
import org.apache.pekko.cluster.Member;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.typed.Cluster;
import org.apache.pekko.cluster.typed.Subscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Spawns a child actor that subscribes to Pekko cluster membership events and
 * maintains an in-memory view of all cluster members.
 *
 * <h2>Why this exists</h2>
 * <p>The {@code joxette_instances} DB table uses a 30-second heartbeat with a
 * 90-second staleness window. Pekko's phi-accrual failure detector detects node
 * loss in ~10 s. This listener gives {@link InstanceController} a real-time
 * membership view that is independent of the DuckDB catalog — so the cluster
 * plane survives catalog file loss.
 *
 * <h2>DB table relationship</h2>
 * <p>The existing {@link InstanceRegistry} and its DB table are kept as a
 * write-through projection (useful for SQL queries when the catalog backend is
 * PostgreSQL). The ground-truth for membership is now this actor's in-memory state.
 *
 * <h2>Thread safety</h2>
 * <p>All state mutations happen inside the actor's single-threaded mailbox.
 * {@link #currentMembers()} reads a volatile snapshot — safe from any thread.
 */
@Component
public class ClusterEventListener {

    private static final Logger log = LoggerFactory.getLogger(ClusterEventListener.class);
    private static final Duration ASK_TIMEOUT = Duration.ofSeconds(5);

    private final ActorSystem<Void> system;

    // Spawned actor reference — set during @PostConstruct
    private ActorRef<ClusterListenerCommand> listenerActor;

    // -------------------------------------------------------------------------
    // Command protocol (internal)
    // -------------------------------------------------------------------------

    sealed interface ClusterListenerCommand {}
    record GetMembers(ActorRef<List<MemberView>> replyTo) implements ClusterListenerCommand {}
    private record MemberEvent(ClusterEvent.MemberEvent event)  implements ClusterListenerCommand {}
    private record ReachabilityEvent(ClusterEvent.ReachabilityEvent event) implements ClusterListenerCommand {}
    private record Ignored() implements ClusterListenerCommand {}

    // -------------------------------------------------------------------------
    // Public view type
    // -------------------------------------------------------------------------

    public record MemberView(
            String address,
            String status,
            boolean reachable,
            java.util.Set<String> roles
    ) {}

    // -------------------------------------------------------------------------
    // Spring lifecycle
    // -------------------------------------------------------------------------

    public ClusterEventListener(ActorSystem<Void> system) {
        this.system = system;
    }

    @PostConstruct
    void start() {
        listenerActor = system.systemActorOf(
                clusterListenerBehavior(),
                "cluster-event-listener",
                org.apache.pekko.actor.typed.Props.empty());
        log.info("ClusterEventListener actor spawned");
    }

    /**
     * Returns a snapshot of all currently known cluster members.
     * Safe to call from any thread (e.g. REST handler).
     */
    public List<MemberView> currentMembers() {
        return AskPattern.ask(
                listenerActor,
                GetMembers::new,
                ASK_TIMEOUT,
                system.scheduler()
        ).toCompletableFuture().join();
    }

    // -------------------------------------------------------------------------
    // Actor behaviour
    // -------------------------------------------------------------------------

    private org.apache.pekko.actor.typed.Behavior<ClusterListenerCommand> clusterListenerBehavior() {
        return Behaviors.setup(ctx -> {
            Cluster cluster = Cluster.get(system);

            // Wrap raw cluster events as typed commands so they enter the mailbox.
            ActorRef<ClusterEvent.ClusterDomainEvent> eventAdapter =
                    ctx.messageAdapter(ClusterEvent.ClusterDomainEvent.class, e -> {
                        if (e instanceof ClusterEvent.MemberEvent me) return new MemberEvent(me);
                        if (e instanceof ClusterEvent.ReachabilityEvent re) return new ReachabilityEvent(re);
                        return new Ignored();
                    });

            cluster.subscriptions().tell(
                    new Subscribe<>(eventAdapter, ClusterEvent.ClusterDomainEvent.class));

            // Mutable state is safe here because all access is single-threaded (mailbox).
            java.util.Map<String, MemberView> members = new java.util.LinkedHashMap<>();

            return Behaviors.receive(ClusterListenerCommand.class)
                    .onMessage(MemberEvent.class, msg -> {
                        ClusterEvent.MemberEvent e = msg.event();
                        Member m = e.member();
                        String addr = m.address().toString();

                        if (e instanceof ClusterEvent.MemberRemoved) {
                            members.remove(addr);
                            log.info("Cluster member removed: {}", addr);
                        } else {
                            String status = memberStatusLabel(m.status());
                            boolean reachable = members.containsKey(addr)
                                    ? members.get(addr).reachable()
                                    : true;
                            members.put(addr, new MemberView(addr, status, reachable, m.getRoles()));
                            log.info("Cluster member {}: {} status={}", addr, e.getClass().getSimpleName(), status);
                        }
                        return Behaviors.same();
                    })
                    .onMessage(ReachabilityEvent.class, msg -> {
                        String addr = msg.event().member().address().toString();
                        boolean reachable = msg.event() instanceof ClusterEvent.ReachableMember;
                        MemberView prev = members.get(addr);
                        if (prev != null) {
                            members.put(addr, new MemberView(prev.address(), prev.status(), reachable, prev.roles()));
                        }
                        log.info("Cluster member {} reachability: {}", addr, reachable ? "reachable" : "unreachable");
                        return Behaviors.same();
                    })
                    .onMessage(GetMembers.class, msg -> {
                        msg.replyTo().tell(List.copyOf(members.values()));
                        return Behaviors.same();
                    })
                    .onMessage(Ignored.class, msg -> Behaviors.same())
                    .build();
        });
    }

    private static String memberStatusLabel(MemberStatus status) {
        return status.toString().toLowerCase();
    }
}

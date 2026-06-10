package com.joxette.operator.cluster;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Reconciles a {@link JoxetteCluster} into the Kubernetes workloads that run
 * joxette-service.
 *
 * <p>Order of operations:
 * <ol>
 *   <li><b>Guardrail</b> ({@link CatalogGuardrail}) — reject unsafe specs
 *       (embedded catalog with replicas&gt;1 / split scale-out / pekko-management,
 *       or a shared backend without a URI) with {@code phase=Rejected} and a
 *       Warning event; create/scale nothing.</li>
 *   <li><b>Apply</b> — server-side apply the desired objects from
 *       {@link JoxetteClusterResources} (each owned by the CR for GC).</li>
 *   <li><b>Status</b> — write {@code phase=Ready} + observedGeneration.</li>
 * </ol>
 *
 * <p>Phase 1 applies the workload, Service, and (in pekko-management mode) RBAC.
 * Live {@code /health} status polling and the RecordedTopic/EntityType reconcilers
 * are later phases.
 */
@Component
@ControllerConfiguration
public class JoxetteClusterReconciler implements Reconciler<JoxetteCluster> {

    private static final Logger log = LoggerFactory.getLogger(JoxetteClusterReconciler.class);

    @Override
    public UpdateControl<JoxetteCluster> reconcile(JoxetteCluster cluster,
                                                   Context<JoxetteCluster> context) {
        String name = cluster.getMetadata().getName();
        Long generation = cluster.getMetadata().getGeneration();

        // 1. Guardrail — refuse to create/scale anything unsafe.
        Optional<String> rejection = CatalogGuardrail.validate(cluster.getSpec());
        if (rejection.isPresent()) {
            log.warn("JoxetteCluster '{}' rejected: {}", name, rejection.get());
            cluster.setStatus(rejected(cluster, rejection.get(), generation));
            return UpdateControl.patchStatus(cluster);
        }

        // 2. Apply desired resources, each owned by the CR (cascading delete on CR removal).
        KubernetesClient client = context.getClient();
        List<HasMetadata> desired = JoxetteClusterResources.build(cluster);
        for (HasMetadata resource : desired) {
            applyOwned(client, cluster, resource);
        }
        log.info("JoxetteCluster '{}' reconciled: backend={}, mode={}, {} object(s) applied",
                name,
                cluster.getSpec().getCatalog().getBackend(),
                cluster.getSpec().getClustering().getMode(),
                desired.size());

        // 3. Read workload readiness back and set phase (Ready vs Progressing).
        WorkloadReadiness readiness = readiness(client, cluster, desired);
        cluster.setStatus(statusFrom(cluster, readiness, generation));

        // Re-poll while still rolling out so .status converges without waiting for the
        // next spec change. Once everything is Ready, the normal watch suffices.
        UpdateControl<JoxetteCluster> control = UpdateControl.patchStatus(cluster);
        return readiness.allReady() ? control : control.rescheduleAfter(Duration.ofSeconds(10));
    }

    /** Sums desired/ready replicas across the Deployments and StatefulSets just applied. */
    private WorkloadReadiness readiness(KubernetesClient client, JoxetteCluster cluster,
                                        List<HasMetadata> desired) {
        String ns = cluster.getMetadata().getNamespace();
        WorkloadReadiness acc = WorkloadReadiness.zero();
        for (HasMetadata r : desired) {
            if (r instanceof Deployment d) {
                Deployment live = client.apps().deployments().inNamespace(ns)
                        .withName(d.getMetadata().getName()).get();
                if (live != null && live.getStatus() != null) {
                    acc = acc.plus(live.getSpec().getReplicas(), live.getStatus().getReadyReplicas());
                } else {
                    acc = acc.plus(d.getSpec().getReplicas(), 0);
                }
            } else if (r instanceof StatefulSet s) {
                StatefulSet live = client.apps().statefulSets().inNamespace(ns)
                        .withName(s.getMetadata().getName()).get();
                if (live != null && live.getStatus() != null) {
                    acc = acc.plus(live.getSpec().getReplicas(), live.getStatus().getReadyReplicas());
                } else {
                    acc = acc.plus(s.getSpec().getReplicas(), 0);
                }
            }
        }
        return acc;
    }

    /**
     * Server-side apply a resource with the CR set as its owner reference.
     * Package-private and overridable so tests can capture applied resources
     * without depending on the Fabric8 mock server's (absent) SSA support.
     */
    void applyOwned(KubernetesClient client, JoxetteCluster owner, HasMetadata resource) {
        // Stamp the owner's namespace first — addOwnerReference rejects an owner/owned
        // pair in different namespaces, and the built resources are namespace-less.
        resource.getMetadata().setNamespace(owner.getMetadata().getNamespace());
        resource.addOwnerReference(owner);
        client.resource(resource).inNamespace(owner.getMetadata().getNamespace())
                .serverSideApply();
    }

    private JoxetteClusterStatus statusFrom(JoxetteCluster cluster, WorkloadReadiness readiness,
                                            Long generation) {
        JoxetteClusterStatus status = new JoxetteClusterStatus(readiness.phase(), readiness.message());
        status.setCatalogBackend(cluster.getSpec().getCatalog().getBackend().name().toUpperCase());
        status.setObservedGeneration(generation);
        status.setDesiredReplicas(readiness.desired());
        status.setReadyReplicas(readiness.ready());
        return status;
    }

    private JoxetteClusterStatus rejected(JoxetteCluster cluster, String reason, Long generation) {
        JoxetteClusterStatus status = new JoxetteClusterStatus("Rejected", reason);
        status.setCatalogBackend(cluster.getSpec().getCatalog().getBackend().name().toUpperCase());
        status.setObservedGeneration(generation);
        return status;
    }
}

package com.joxette.operator.cluster;

import com.joxette.operator.cluster.JoxetteClusterSpec.CatalogBackend;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Drives {@link JoxetteClusterReconciler#reconcile} to prove the guardrail
 * short-circuit and the apply path.
 *
 * <p>The Fabric8 mock server (CRUD mode) does not implement server-side-apply
 * PATCH, so the apply path is exercised with a capturing subclass that records the
 * resources the reconciler would apply (the SSA call itself is correct production
 * behaviour, verified against real clusters; the resource shapes are verified in
 * {@link JoxetteClusterResourcesTest}). The rejection path runs end-to-end against
 * the mock since it applies nothing.
 */
@EnableKubernetesMockClient(crud = true)
class JoxetteClusterReconcilerTest {

    KubernetesClient client;

    /** Reconciler that records applied resources instead of calling the API. */
    static class CapturingReconciler extends JoxetteClusterReconciler {
        final List<HasMetadata> applied = new ArrayList<>();
        @Override
        void applyOwned(KubernetesClient c, JoxetteCluster owner, HasMetadata resource) {
            resource.getMetadata().setNamespace(owner.getMetadata().getNamespace());
            applied.add(resource);
        }
    }

    @SuppressWarnings("unchecked")
    private Context<JoxetteCluster> contextWithClient() {
        Context<JoxetteCluster> ctx = mock(Context.class);
        when(ctx.getClient()).thenReturn(client);
        return ctx;
    }

    private JoxetteCluster cluster(String name, JoxetteClusterSpec spec) {
        JoxetteCluster c = new JoxetteCluster();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(name);
        meta.setNamespace("joxette");
        meta.setGeneration(1L);
        meta.setUid("11111111-2222-3333-4444-555555555555");
        c.setMetadata(meta);
        c.setSpec(spec);
        return c;
    }

    @Test
    void unsafeSpecIsRejectedAndNothingIsApplied() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.embedded);
        spec.getTiers().getRecorder().setReplicas(3); // embedded + >1 => rejected
        JoxetteCluster c = cluster("bad", spec);

        CapturingReconciler reconciler = new CapturingReconciler();
        UpdateControl<JoxetteCluster> control = reconciler.reconcile(c, contextWithClient());

        assertThat(control.isPatchStatus()).isTrue();
        assertThat(c.getStatus().getPhase()).isEqualTo("Rejected");
        assertThat(c.getStatus().getMessage()).contains("single-writer");
        assertThat(reconciler.applied).isEmpty();   // guardrail blocked all apply
    }

    @Test
    void rejectionPathRunsEndToEndAgainstMockAndCreatesNoWorkload() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.embedded);
        spec.getTiers().getReplay().setReplicas(2); // rejected
        JoxetteCluster c = cluster("nope", spec);

        // The real reconciler (no capture) — proves the rejection path touches no API.
        new JoxetteClusterReconciler().reconcile(c, contextWithClient());

        assertThat(c.getStatus().getPhase()).isEqualTo("Rejected");
        assertThat(client.apps().statefulSets().inNamespace("joxette").withName("nope").get())
                .isNull();
    }

    @Test
    void validEmbeddedSpecAppliesStatefulSetAndServiceAccountAndReportsProgressing() {
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(CatalogBackend.embedded);
        spec.getTiers().getRecorder().setReplicas(1);
        spec.getTiers().getReplay().setReplicas(1);
        JoxetteCluster c = cluster("good", spec);

        CapturingReconciler reconciler = new CapturingReconciler();
        UpdateControl<JoxetteCluster> control = reconciler.reconcile(c, contextWithClient());

        // The all-in-one StatefulSet and the ServiceAccount are among the applied objects.
        assertThat(reconciler.applied).anyMatch(r -> r instanceof StatefulSet
                && "good".equals(r.getMetadata().getName()));
        assertThat(reconciler.applied).anyMatch(r -> r instanceof ServiceAccount
                && "good".equals(r.getMetadata().getName()));
        // No live replicas => Progressing, generation observed, status patched.
        assertThat(c.getStatus().getPhase()).isEqualTo("Progressing");
        assertThat(c.getStatus().getCatalogBackend()).isEqualTo("EMBEDDED");
        assertThat(c.getStatus().getObservedGeneration()).isEqualTo(1L);
        assertThat(control.isPatchStatus()).isTrue();
    }
}

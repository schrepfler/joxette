package com.joxette.operator.cluster;

import com.joxette.operator.cluster.JoxetteClusterSpec.CatalogBackend;
import com.joxette.operator.cluster.JoxetteClusterSpec.ClusteringMode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The catalog single-writer guardrail — the operator's core safety invariant.
 * Mirrors the Helm chart's {@code joxette.validate} and design doc §5.
 */
class CatalogGuardrailTest {

    private static JoxetteClusterSpec embedded() {
        JoxetteClusterSpec s = new JoxetteClusterSpec();
        s.setImage("joxette-service:test");
        s.getCatalog().setBackend(CatalogBackend.embedded);
        // default tiers are recorder/replay replicas=2 in spec defaults — reset to 1
        s.getTiers().getRecorder().setReplicas(1);
        s.getTiers().getReplay().setReplicas(1);
        return s;
    }

    private static JoxetteClusterSpec shared(CatalogBackend backend, String uri) {
        JoxetteClusterSpec s = new JoxetteClusterSpec();
        s.setImage("joxette-service:test");
        s.getCatalog().setBackend(backend);
        s.getCatalog().setUri(uri);
        return s;
    }

    @Test
    void embeddedSinglePodIsValid() {
        assertThat(CatalogGuardrail.validate(embedded())).isEmpty();
    }

    @Test
    void embeddedWithOverscaledRecorderIsRejected() {
        JoxetteClusterSpec s = embedded();
        s.getTiers().getRecorder().setReplicas(3);
        assertThat(CatalogGuardrail.validate(s))
                .get().asString().contains("single-writer", "recorder");
    }

    @Test
    void embeddedWithOverscaledReplayIsRejected() {
        JoxetteClusterSpec s = embedded();
        s.getTiers().getReplay().setReplicas(2);
        assertThat(CatalogGuardrail.validate(s)).get().asString().contains("replay");
    }

    @Test
    void embeddedWithPekkoManagementIsRejected() {
        JoxetteClusterSpec s = embedded();
        s.getClustering().setMode(ClusteringMode.pekko_management);
        assertThat(CatalogGuardrail.validate(s))
                .get().asString().contains("pekko-management", "incompatible");
    }

    @Test
    void quackWithoutUriIsRejected() {
        assertThat(CatalogGuardrail.validate(shared(CatalogBackend.quack, null)))
                .get().asString().contains("requires catalog.uri");
    }

    @Test
    void postgresqlWithUriIsValid() {
        assertThat(CatalogGuardrail.validate(
                shared(CatalogBackend.postgresql, "postgresql://pg/joxette"))).isEmpty();
    }

    @Test
    void quackWithUriAndPekkoManagementIsValid() {
        JoxetteClusterSpec s = shared(CatalogBackend.quack, "quack://q:5432/joxette");
        s.getClustering().setMode(ClusteringMode.pekko_management);
        assertThat(CatalogGuardrail.validate(s)).isEmpty();
    }
}

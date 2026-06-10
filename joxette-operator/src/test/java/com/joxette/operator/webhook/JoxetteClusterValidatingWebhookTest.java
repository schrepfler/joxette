package com.joxette.operator.webhook;

import com.joxette.operator.cluster.JoxetteCluster;
import com.joxette.operator.cluster.JoxetteClusterSpec;
import com.joxette.operator.cluster.JoxetteClusterSpec.CatalogBackend;
import com.joxette.operator.cluster.JoxetteClusterSpec.ClusteringMode;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionRequest;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionReview;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JoxetteClusterValidatingWebhookTest {

    private final JoxetteClusterValidatingWebhook webhook = new JoxetteClusterValidatingWebhook();

    private AdmissionReview reviewFor(JoxetteCluster cluster) {
        AdmissionRequest req = new AdmissionRequest();
        req.setUid("req-123");
        req.setObject(cluster);
        AdmissionReview review = new AdmissionReview();
        review.setRequest(req);
        return review;
    }

    private JoxetteCluster cluster(CatalogBackend backend, ClusteringMode mode, int recorderReplicas) {
        JoxetteCluster c = new JoxetteCluster();
        JoxetteClusterSpec spec = new JoxetteClusterSpec();
        spec.setImage("joxette-service:test");
        spec.getCatalog().setBackend(backend);
        spec.getClustering().setMode(mode);
        spec.getTiers().getRecorder().setReplicas(recorderReplicas);
        spec.getTiers().getReplay().setReplicas(1);
        c.setSpec(spec);
        return c;
    }

    @Test
    void allowsSafeEmbeddedCluster() {
        AdmissionReview result = webhook.validate(
                reviewFor(cluster(CatalogBackend.embedded, ClusteringMode.catalog, 1)));
        assertThat(result.getResponse().getAllowed()).isTrue();
        assertThat(result.getResponse().getUid()).isEqualTo("req-123");
    }

    @Test
    void deniesEmbeddedWithReplicasOverOne() {
        AdmissionReview result = webhook.validate(
                reviewFor(cluster(CatalogBackend.embedded, ClusteringMode.catalog, 3)));
        assertThat(result.getResponse().getAllowed()).isFalse();
        assertThat(result.getResponse().getStatus().getMessage()).contains("single-writer");
        assertThat(result.getResponse().getStatus().getCode()).isEqualTo(409);
        assertThat(result.getResponse().getUid()).isEqualTo("req-123");
    }

    @Test
    void deniesEmbeddedWithPekkoManagement() {
        AdmissionReview result = webhook.validate(
                reviewFor(cluster(CatalogBackend.embedded, ClusteringMode.pekko_management, 1)));
        assertThat(result.getResponse().getAllowed()).isFalse();
        assertThat(result.getResponse().getStatus().getMessage()).contains("pekko-management");
    }

    @Test
    void allowsRequestWithNoObject() {
        AdmissionReview review = new AdmissionReview();
        AdmissionRequest req = new AdmissionRequest();
        req.setUid("empty");
        review.setRequest(req);
        AdmissionReview result = webhook.validate(review);
        assertThat(result.getResponse().getAllowed()).isTrue();
    }

    @Test
    void convertHandlesMapShapedObject() {
        // Simulate the object arriving as a generic map (the wire form).
        java.util.Map<String, Object> spec = new java.util.LinkedHashMap<>();
        spec.put("image", "joxette-service:test");
        spec.put("catalog", java.util.Map.of("backend", "embedded"));
        java.util.Map<String, Object> obj = new java.util.LinkedHashMap<>();
        obj.put("apiVersion", "joxette.dev/v1alpha1");
        obj.put("kind", "JoxetteCluster");
        obj.put("spec", spec);

        JoxetteCluster c = JoxetteClusterValidatingWebhook.convert(obj);
        assertThat(c).isNotNull();
        assertThat(c.getSpec().getCatalog().getBackend()).isEqualTo(CatalogBackend.embedded);
    }
}

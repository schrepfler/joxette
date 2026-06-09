package com.joxette.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@code PEKKO_MANAGEMENT} clustering overlay in {@link PekkoConfig}.
 *
 * <p>These validate the HOCON produced for Track B clustering without starting an
 * ActorSystem or touching Kubernetes — they assert that the overlay sets the
 * discovery method, management port, label selector, canonical hostname, and the
 * lease-backed split-brain resolver, and that it merges cleanly over the real
 * {@code pekko.conf} base (so the keys exist in the management/lease reference.conf).
 */
class PekkoConfigClusteringTest {

    private static JoxetteProperties.Clustering clustering() {
        JoxetteProperties.Clustering c = new JoxetteProperties.Clustering();
        c.setServiceName("joxette-prod");
        c.setManagementPort(7626);
        c.setRequiredContactPointNr(3);
        c.setPodLabelSelector("app.kubernetes.io/name=%s");
        return c;
    }

    @Test
    void overlayWiresKubernetesDiscoveryAndLeaseSbr() {
        Config overlay = PekkoConfig.buildManagementOverlay(clustering(), "10.1.2.3");

        assertThat(overlay.getString(
                "pekko.management.cluster.bootstrap.contact-point-discovery.discovery-method"))
                .isEqualTo("kubernetes-api");
        assertThat(overlay.getString(
                "pekko.management.cluster.bootstrap.contact-point-discovery.service-name"))
                .isEqualTo("joxette-prod");
        assertThat(overlay.getInt(
                "pekko.management.cluster.bootstrap.contact-point-discovery.required-contact-point-nr"))
                .isEqualTo(3);
        assertThat(overlay.getInt("pekko.management.http.port")).isEqualTo(7626);
        assertThat(overlay.getString("pekko.discovery.kubernetes-api.pod-label-selector"))
                .isEqualTo("app.kubernetes.io/name=joxette-prod");
        assertThat(overlay.getString("pekko.cluster.split-brain-resolver.active-strategy"))
                .isEqualTo("lease-majority");
        assertThat(overlay.getString(
                "pekko.coordination.lease.kubernetes.lease-class"))
                .isEqualTo("org.apache.pekko.coordination.lease.kubernetes.NativeKubernetesLease");
    }

    @Test
    void canonicalHostnameSetFromPodIpWhenPresent() {
        Config withIp = PekkoConfig.buildManagementOverlay(clustering(), "10.4.5.6");
        assertThat(withIp.getString("pekko.remote.artery.canonical.hostname")).isEqualTo("10.4.5.6");
    }

    @Test
    void canonicalHostnameOmittedWhenPodIpAbsent() {
        // With no POD_IP, the overlay must not set canonical.hostname so the
        // pekko.conf default (127.0.0.1) survives the merge for local runs.
        Config noIp = PekkoConfig.buildManagementOverlay(clustering(), null);
        assertThat(noIp.hasPath("pekko.remote.artery.canonical.hostname")).isFalse();
    }

    @Test
    void overlayMergesCleanlyOverPekkoConfBase() {
        // Merging over the real base must resolve without unresolved-substitution or
        // missing-path errors — proves the management/lease reference.conf is present
        // on the classpath and the keys we override actually exist.
        Config base = ConfigFactory.load("pekko");
        Config merged = PekkoConfig.buildManagementOverlay(clustering(), "10.7.8.9")
                .withFallback(base)
                .resolve();
        assertThat(merged.getString("pekko.actor.provider")).isEqualTo("cluster");
        assertThat(merged.getString(
                "pekko.management.cluster.bootstrap.contact-point-discovery.discovery-method"))
                .isEqualTo("kubernetes-api");
        // seed-nodes stays empty in management mode — bootstrap drives membership.
        assertThat(merged.getList("pekko.cluster.seed-nodes")).isEmpty();
    }

    @Test
    void defaultClusteringModeIsCatalog() {
        assertThat(new JoxetteProperties.Clustering().getMode())
                .isEqualTo(JoxetteProperties.ClusteringMode.CATALOG);
    }
}

package com.joxette.operator.cluster;

import com.joxette.operator.cluster.JoxetteClusterSpec.CatalogBackend;
import com.joxette.operator.cluster.JoxetteClusterSpec.ClusteringMode;

import java.util.Optional;

/**
 * The catalog single-writer guardrail — the operator's core safety check.
 *
 * <p>An embedded DuckDB catalog is single-writer per file: a second pod (or a
 * split tier, or a multi-pod Pekko cluster) corrupts it. This validates a
 * {@link JoxetteClusterSpec} and returns a rejection reason if the combination is
 * unsafe, mirroring {@code docs/operator-design.md} §5 and the Helm chart's
 * {@code joxette.validate}. Pure function — unit-tested without a cluster.
 */
public final class CatalogGuardrail {

    private CatalogGuardrail() {
    }

    /**
     * @return empty if the spec is safe to reconcile; otherwise a human-readable
     *         rejection reason for {@code status.message} + a Warning event.
     */
    public static Optional<String> validate(JoxetteClusterSpec spec) {
        JoxetteClusterSpec.Catalog catalog = spec.getCatalog();
        CatalogBackend backend = catalog.getBackend();
        boolean pekkoManagement = spec.getClustering().getMode() == ClusteringMode.pekko_management;

        if (backend == CatalogBackend.embedded) {
            if (pekkoManagement) {
                return Optional.of(
                        "clustering.mode=pekko-management forms a multi-pod cluster, which is "
                        + "incompatible with the single-writer embedded catalog. Use a "
                        + "quack/postgresql backend, or clustering.mode=catalog.");
            }
            String overscaled = firstOverscaledTier(spec.getTiers());
            if (overscaled != null) {
                return Optional.of(
                        "catalog.backend=embedded is single-writer: tiers." + overscaled
                        + ".replicas > 1 would corrupt the catalog file. Use a quack/postgresql "
                        + "backend to scale out, or set replicas to 1.");
            }
        } else {
            // Shared backends require a connection URI.
            if (catalog.getUri() == null || catalog.getUri().isBlank()) {
                return Optional.of(
                        "catalog.backend=" + backend + " requires catalog.uri "
                        + "(quack://… or postgresql://…)");
            }
        }
        return Optional.empty();
    }

    /** Returns the name of the first enabled tier with replicas > 1, or null. */
    private static String firstOverscaledTier(JoxetteClusterSpec.Tiers tiers) {
        if (tiers.getRecorder().isEnabled() && tiers.getRecorder().getReplicas() > 1) return "recorder";
        if (tiers.getReplay().isEnabled() && tiers.getReplay().getReplicas() > 1) return "replay";
        if (tiers.getCompaction().isEnabled() && tiers.getCompaction().getReplicas() > 1) return "compaction";
        return null;
    }
}

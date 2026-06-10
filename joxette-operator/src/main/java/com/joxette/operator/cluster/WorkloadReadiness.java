package com.joxette.operator.cluster;

/**
 * Aggregate readiness of the pods a {@link JoxetteCluster} owns: total desired vs
 * ready replicas, and the derived phase. Pure value type so the phase logic is
 * unit-testable without a cluster.
 */
public record WorkloadReadiness(int desired, int ready) {

    public WorkloadReadiness plus(Integer desiredReplicas, Integer readyReplicas) {
        return new WorkloadReadiness(
                desired + (desiredReplicas != null ? desiredReplicas : 0),
                ready + (readyReplicas != null ? readyReplicas : 0));
    }

    public static WorkloadReadiness zero() {
        return new WorkloadReadiness(0, 0);
    }

    /** {@code true} once every desired replica is ready (and at least one is desired). */
    public boolean allReady() {
        return desired > 0 && ready >= desired;
    }

    /** Phase string for {@code .status.phase}. */
    public String phase() {
        return allReady() ? "Ready" : "Progressing";
    }

    public String message() {
        return ready + "/" + desired + " replicas ready";
    }
}

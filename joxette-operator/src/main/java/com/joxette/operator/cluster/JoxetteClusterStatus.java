package com.joxette.operator.cluster;

/**
 * Observed state of a {@link JoxetteCluster}, written back to {@code .status}.
 */
public class JoxetteClusterStatus {

    /** Lifecycle phase: Pending | Progressing | Ready | Degraded | Rejected. */
    private String phase;

    /** Human-readable detail for the current phase (e.g. a guardrail rejection reason). */
    private String message;

    /** Catalog backend the reconciler acted on (echoes spec, uppercased). */
    private String catalogBackend;

    /** Last spec generation the reconciler observed. */
    private Long observedGeneration;

    /** Total desired pod replicas across all workloads this CR owns. */
    private Integer desiredReplicas;

    /** Ready pod replicas across all workloads this CR owns. */
    private Integer readyReplicas;

    public JoxetteClusterStatus() {
    }

    public JoxetteClusterStatus(String phase, String message) {
        this.phase = phase;
        this.message = message;
    }

    public String getPhase() { return phase; }
    public void setPhase(String phase) { this.phase = phase; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public String getCatalogBackend() { return catalogBackend; }
    public void setCatalogBackend(String catalogBackend) { this.catalogBackend = catalogBackend; }
    public Long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(Long observedGeneration) { this.observedGeneration = observedGeneration; }
    public Integer getDesiredReplicas() { return desiredReplicas; }
    public void setDesiredReplicas(Integer desiredReplicas) { this.desiredReplicas = desiredReplicas; }
    public Integer getReadyReplicas() { return readyReplicas; }
    public void setReadyReplicas(Integer readyReplicas) { this.readyReplicas = readyReplicas; }
}

package com.joxette.operator.entity;

/**
 * Observed state of an {@link EntityType}.
 */
public class EntityTypeStatus {

    private String phase;
    private String message;
    private Boolean registered;
    private Integer sourceCount;
    private Long observedGeneration;

    public EntityTypeStatus() {
    }

    public EntityTypeStatus(String phase, String message) {
        this.phase = phase;
        this.message = message;
    }

    public String getPhase() { return phase; }
    public void setPhase(String phase) { this.phase = phase; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public Boolean getRegistered() { return registered; }
    public void setRegistered(Boolean registered) { this.registered = registered; }
    public Integer getSourceCount() { return sourceCount; }
    public void setSourceCount(Integer sourceCount) { this.sourceCount = sourceCount; }
    public Long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(Long observedGeneration) { this.observedGeneration = observedGeneration; }
}

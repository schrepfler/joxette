package com.joxette.operator.topic;

/**
 * Observed state of a {@link RecordedTopic}.
 */
public class RecordedTopicStatus {

    /** Ready | Progressing | Degraded. */
    private String phase;
    private String message;
    /** Whether the topic is currently registered in the target cluster. */
    private Boolean registered;
    private Long observedGeneration;

    public RecordedTopicStatus() {
    }

    public RecordedTopicStatus(String phase, String message) {
        this.phase = phase;
        this.message = message;
    }

    public String getPhase() { return phase; }
    public void setPhase(String phase) { this.phase = phase; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public Boolean getRegistered() { return registered; }
    public void setRegistered(Boolean registered) { this.registered = registered; }
    public Long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(Long observedGeneration) { this.observedGeneration = observedGeneration; }
}

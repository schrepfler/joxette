package com.joxette.operator.topic;

/**
 * Desired state of a {@link RecordedTopic}. Mirrors {@code docs/operator-design.md}
 * §3.2 and the {@code POST /topics} request body.
 */
public class RecordedTopicSpec {

    /** Reference to the JoxetteCluster whose REST API this topic is reconciled into. */
    private ClusterRef clusterRef = new ClusterRef();

    /** Kafka topic name. */
    private String topic;

    /** Recording mode: general | entity_only | both. */
    private String mode = "general";

    /** latest | earliest. */
    private String startFrom = "latest";

    /** Broker id; null → default broker. */
    private String brokerId;

    /** What to do with server-side state when the CR is deleted: Delete | Pause | Orphan. */
    private DeletionPolicy deletionPolicy = DeletionPolicy.Pause;

    public enum DeletionPolicy { Delete, Pause, Orphan }

    /** Points at a JoxetteCluster in the same namespace (Service DNS target). */
    public static class ClusterRef {
        private String name;
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }

    public ClusterRef getClusterRef() { return clusterRef; }
    public void setClusterRef(ClusterRef clusterRef) { this.clusterRef = clusterRef; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }
    public String getStartFrom() { return startFrom; }
    public void setStartFrom(String startFrom) { this.startFrom = startFrom; }
    public String getBrokerId() { return brokerId; }
    public void setBrokerId(String brokerId) { this.brokerId = brokerId; }
    public DeletionPolicy getDeletionPolicy() { return deletionPolicy; }
    public void setDeletionPolicy(DeletionPolicy deletionPolicy) { this.deletionPolicy = deletionPolicy; }
}

package com.joxette.operator.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * Desired state of an {@link EntityType}. Mirrors {@code docs/operator-design.md}
 * §3.3 and the {@code /entities} + {@code /entities/{type}/sources} request bodies.
 */
public class EntityTypeSpec {

    private ClusterRef clusterRef = new ClusterRef();

    /** Entity type name (e.g. {@code order}). */
    private String type;

    /** Number of hash buckets. */
    private int buckets = 256;

    /** Source topic mappings for this entity type. */
    private List<Source> sources = new ArrayList<>();

    private DeletionPolicy deletionPolicy = DeletionPolicy.Orphan;

    public enum DeletionPolicy { Delete, Orphan }

    public static class ClusterRef {
        private String name;
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }

    public static class Source {
        private String topic;
        /** entity_only | both. */
        private String mode = "entity_only";
        private List<Matcher> matchers = new ArrayList<>();
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public String getMode() { return mode; }
        public void setMode(String mode) { this.mode = mode; }
        public List<Matcher> getMatchers() { return matchers; }
        public void setMatchers(List<Matcher> matchers) { this.matchers = matchers; }
    }

    public static class Matcher {
        private String messageType;
        /** key | value | header. */
        private String idSource = "value";
        private String idExpression;
        public String getMessageType() { return messageType; }
        public void setMessageType(String messageType) { this.messageType = messageType; }
        public String getIdSource() { return idSource; }
        public void setIdSource(String idSource) { this.idSource = idSource; }
        public String getIdExpression() { return idExpression; }
        public void setIdExpression(String idExpression) { this.idExpression = idExpression; }
    }

    public ClusterRef getClusterRef() { return clusterRef; }
    public void setClusterRef(ClusterRef clusterRef) { this.clusterRef = clusterRef; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public int getBuckets() { return buckets; }
    public void setBuckets(int buckets) { this.buckets = buckets; }
    public List<Source> getSources() { return sources; }
    public void setSources(List<Source> sources) { this.sources = sources; }
    public DeletionPolicy getDeletionPolicy() { return deletionPolicy; }
    public void setDeletionPolicy(DeletionPolicy deletionPolicy) { this.deletionPolicy = deletionPolicy; }
}

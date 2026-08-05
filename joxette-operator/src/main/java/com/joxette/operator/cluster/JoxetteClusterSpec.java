package com.joxette.operator.cluster;

import java.util.Map;

/**
 * Desired state of a {@link JoxetteCluster}. Field shape mirrors the Helm chart
 * values and {@code docs/operator-design.md} §3.1 so the two stay aligned.
 */
public class JoxetteClusterSpec {

    /** Fully-qualified joxette-service image (e.g. {@code ghcr.io/acme/joxette-service:0.1.0}). */
    private String image;

    private Clustering clustering = new Clustering();
    private Catalog catalog = new Catalog();
    private Kafka kafka = new Kafka();
    private ObjectStore objectStore = new ObjectStore();
    private Tiers tiers = new Tiers();

    /** Extra JOXETTE_* env applied to every pod (Spring relaxed binding). */
    private Map<String, String> extraEnv = Map.of();

    // ---- nested types -------------------------------------------------------

    public enum ClusteringMode { catalog, pekko_management }

    public static class Clustering {
        /** {@code catalog} (self-join) or {@code pekko-management} (shared cluster). */
        private ClusteringMode mode = ClusteringMode.catalog;
        private int managementPort = 7626;
        public ClusteringMode getMode() { return mode; }
        public void setMode(ClusteringMode mode) { this.mode = mode; }
        public int getManagementPort() { return managementPort; }
        public void setManagementPort(int managementPort) { this.managementPort = managementPort; }
    }

    public enum CatalogBackend { embedded, quack, postgresql }

    public static class Catalog {
        private CatalogBackend backend = CatalogBackend.embedded;
        /** Connection URI for quack/postgresql backends. */
        private String uri;
        /** Parquet data root on object storage — always remote. */
        private String objectStoragePath = "s3://joxette-data/";
        private Embedded embedded = new Embedded();

        public static class Embedded {
            private String path = "/data/joxette.ducklake";
            private String pvcSize = "20Gi";
            private String storageClass;
            public String getPath() { return path; }
            public void setPath(String path) { this.path = path; }
            public String getPvcSize() { return pvcSize; }
            public void setPvcSize(String pvcSize) { this.pvcSize = pvcSize; }
            public String getStorageClass() { return storageClass; }
            public void setStorageClass(String storageClass) { this.storageClass = storageClass; }
        }

        public CatalogBackend getBackend() { return backend; }
        public void setBackend(CatalogBackend backend) { this.backend = backend; }
        public String getUri() { return uri; }
        public void setUri(String uri) { this.uri = uri; }
        public String getObjectStoragePath() { return objectStoragePath; }
        public void setObjectStoragePath(String p) { this.objectStoragePath = p; }
        public Embedded getEmbedded() { return embedded; }
        public void setEmbedded(Embedded embedded) { this.embedded = embedded; }
    }

    public static class Kafka {
        private String bootstrapServers = "kafka:9092";
        private String consumerGroup = "joxette-recorder";
        private String groupProtocol = "consumer";
        /** Secret name with SASL/SSL material, projected as env. Optional. */
        private String secretRef;
        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String s) { this.bootstrapServers = s; }
        public String getConsumerGroup() { return consumerGroup; }
        public void setConsumerGroup(String s) { this.consumerGroup = s; }
        public String getGroupProtocol() { return groupProtocol; }
        public void setGroupProtocol(String s) { this.groupProtocol = s; }
        public String getSecretRef() { return secretRef; }
        public void setSecretRef(String s) { this.secretRef = s; }
    }

    public static class ObjectStore {
        private String endpoint;
        private String region = "us-east-1";
        /** Secret with keys access-key / secret-key. Omit for IRSA / instance profile. */
        private String secretRef;
        public String getEndpoint() { return endpoint; }
        public void setEndpoint(String endpoint) { this.endpoint = endpoint; }
        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }
        public String getSecretRef() { return secretRef; }
        public void setSecretRef(String secretRef) { this.secretRef = secretRef; }
    }

    /**
     * Role tiers. {@code replicas} and {@code enabled} apply to shared-catalog
     * backends only (ignored for embedded, which is always a single all-in-one
     * pod). {@code resources} is not ignored for embedded — the embedded
     * StatefulSet's container reuses the recorder tier's {@code resources}
     * (see {@code JoxetteClusterResources.embeddedStatefulSet()}).
     */
    public static class Tiers {
        private Tier recorder = Tier.of(2, true, "1", "2Gi");
        private Tier replay = Tier.of(2, true, "500m", "1Gi");
        private Tier compaction = Tier.of(1, true, "500m", "1Gi");
        public Tier getRecorder() { return recorder; }
        public void setRecorder(Tier recorder) { this.recorder = recorder; }
        public Tier getReplay() { return replay; }
        public void setReplay(Tier replay) { this.replay = replay; }
        public Tier getCompaction() { return compaction; }
        public void setCompaction(Tier compaction) { this.compaction = compaction; }
    }

    public static class Tier {
        private boolean enabled = true;
        private int replicas = 1;
        private Resources resources = new Resources();

        static Tier of(int replicas, boolean enabled, String requestCpu, String requestMemory) {
            Tier t = new Tier();
            t.replicas = replicas;
            t.enabled = enabled;
            t.resources.requestCpu = requestCpu;
            t.resources.requestMemory = requestMemory;
            // limitMemory is deliberately left unset here — Resources.getLimitMemory()
            // derives it from requestMemory whenever it hasn't been explicitly set.
            return t;
        }
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public int getReplicas() { return replicas; }
        public void setReplicas(int replicas) { this.replicas = replicas; }
        public Resources getResources() { return resources; }
        public void setResources(Resources resources) { this.resources = resources; }
    }

    /**
     * Container resources for a tier. {@code limitMemory} derives from
     * {@code requestMemory} whenever the CR doesn't set it explicitly (see
     * {@link #getLimitMemory()}), and there is deliberately no cpu limit — the
     * same convention already used for the operator's own Deployment
     * (deploy/operator/deployment.yaml): bound memory hard (JVM heap + DuckDB
     * native allocations must not spill onto the node) but leave cpu unbounded
     * so a burst never gets throttled.
     *
     * <p>The derivation is dynamic (computed in the getter), not copied at
     * field-declaration time, so a CR that overrides only {@code requestMemory}
     * (e.g. {@code resources: {requestMemory: 8Gi}}) still gets a consistent
     * {@code limits.memory == requests.memory} instead of falling back to this
     * class's unrelated {@code "1Gi"} field default — which would otherwise
     * produce {@code limits.memory < requests.memory} and get the pod spec
     * rejected outright by the API server.
     */
    public static class Resources {
        private String requestCpu = "500m";
        private String requestMemory = "1Gi";
        /** Null means "not explicitly set" — {@link #getLimitMemory()} derives it. */
        private String limitMemory;
        public String getRequestCpu() { return requestCpu; }
        public void setRequestCpu(String requestCpu) { this.requestCpu = requestCpu; }
        public String getRequestMemory() { return requestMemory; }
        public void setRequestMemory(String requestMemory) { this.requestMemory = requestMemory; }
        public String getLimitMemory() { return limitMemory != null ? limitMemory : requestMemory; }
        public void setLimitMemory(String limitMemory) { this.limitMemory = limitMemory; }
    }

    // ---- accessors ----------------------------------------------------------

    public String getImage() { return image; }
    public void setImage(String image) { this.image = image; }
    public Clustering getClustering() { return clustering; }
    public void setClustering(Clustering clustering) { this.clustering = clustering; }
    public Catalog getCatalog() { return catalog; }
    public void setCatalog(Catalog catalog) { this.catalog = catalog; }
    public Kafka getKafka() { return kafka; }
    public void setKafka(Kafka kafka) { this.kafka = kafka; }
    public ObjectStore getObjectStore() { return objectStore; }
    public void setObjectStore(ObjectStore objectStore) { this.objectStore = objectStore; }
    public Tiers getTiers() { return tiers; }
    public void setTiers(Tiers tiers) { this.tiers = tiers; }
    public Map<String, String> getExtraEnv() { return extraEnv; }
    public void setExtraEnv(Map<String, String> extraEnv) { this.extraEnv = extraEnv; }
}

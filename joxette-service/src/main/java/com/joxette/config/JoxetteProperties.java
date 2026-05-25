package com.joxette.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "joxette")
public class JoxetteProperties {

    private Catalog catalog = new Catalog();
    private Inline inline = new Inline();
    private Compaction compaction = new Compaction();
    private Retention retention = new Retention();
    private Kafka kafka = new Kafka();
    private Recording recording = new Recording();
    private Threading threading = new Threading();
    private Replay replay = new Replay();
    private Bootstrap bootstrap = new Bootstrap();
    private S3 s3 = new S3();
    private ObjectStore objectStore = new ObjectStore();

    // -----------------------------------------------------------------------
    // Catalog
    // -----------------------------------------------------------------------

    public static class Catalog {
        /** Path to the DuckLake catalog file (DuckDB). */
        private String path = "./data/joxette.ducklake";
        /** Root path on object storage where Parquet files are written. */
        private String objectStoragePath;
        /**
         * Whether to automatically run {@code ducklake_migrate()} on startup (DuckLake 1.0+).
         * Set to {@code false} in environments where catalog schema changes require prior approval.
         */
        private boolean autoMigrate = true;
        /**
         * When {@code true}, enables DuckLakeMetadata catalog query tracing (DuckLake 1.0+).
         * Activates {@code SET logging_type = 'DuckLakeMetadata'} and
         * {@code SET logging_level = 'DEBUG'} immediately after the DuckLake extension is
         * attached, so that per-query elapsed times are recorded for all subsequent catalog
         * operations (ATTACH, {@code ducklake_migrate()}, schema setup).  Accumulated log
         * entries are drained to SLF4J under the {@code com.joxette.catalog.ducklake} logger
         * at DEBUG level after startup completes.
         *
         * <p>Defaults to {@code false} — zero overhead in production unless explicitly enabled.
         * To see the output, also set:
         * <pre>logging.level.com.joxette.catalog.ducklake: DEBUG</pre>
         */
        private boolean metadataQueryLogging = false;
        /**
         * Override DuckLake's {@code data_inlining_row_limit} option after ATTACH
         * (DuckLake 1.0+, PR #923).  {@code null} leaves the DuckLake default in place
         * (currently 10 rows).  Set to {@code 0} to disable inlining entirely and always
         * write Parquet directly — useful for high-throughput topics where inlining adds
         * overhead rather than saving PUT requests.
         */
        private Integer inliningRowLimit = null;
        /** Optional S3-compatible storage credentials and endpoint. */
        private S3 s3 = new S3();

        public String getPath() { return path; }
        public void setPath(String path) { this.path = path; }

        public String getObjectStoragePath() { return objectStoragePath; }
        public void setObjectStoragePath(String objectStoragePath) { this.objectStoragePath = objectStoragePath; }

        public boolean isAutoMigrate() { return autoMigrate; }
        public void setAutoMigrate(boolean autoMigrate) { this.autoMigrate = autoMigrate; }

        public boolean isMetadataQueryLogging() { return metadataQueryLogging; }
        public void setMetadataQueryLogging(boolean metadataQueryLogging) { this.metadataQueryLogging = metadataQueryLogging; }

        public Integer getInliningRowLimit() { return inliningRowLimit; }
        public void setInliningRowLimit(Integer inliningRowLimit) { this.inliningRowLimit = inliningRowLimit; }

        public S3 getS3() { return s3; }
        public void setS3(S3 s3) { this.s3 = s3; }

        public static class S3 {
            /** S3 endpoint host (and optional port), e.g. {@code localhost:9000}. */
            private String endpoint;
            private String accessKeyId;
            private String secretAccessKey;
            private boolean useSsl = true;
            /** {@code "path"} or {@code "vhost"} (default: vhost). */
            private String urlStyle = "vhost";

            public String getEndpoint() { return endpoint; }
            public void setEndpoint(String endpoint) { this.endpoint = endpoint; }

            public String getAccessKeyId() { return accessKeyId; }
            public void setAccessKeyId(String accessKeyId) { this.accessKeyId = accessKeyId; }

            public String getSecretAccessKey() { return secretAccessKey; }
            public void setSecretAccessKey(String secretAccessKey) { this.secretAccessKey = secretAccessKey; }

            public boolean isUseSsl() { return useSsl; }
            public void setUseSsl(boolean useSsl) { this.useSsl = useSsl; }

            public String getUrlStyle() { return urlStyle; }
            public void setUrlStyle(String urlStyle) { this.urlStyle = urlStyle; }
        }
    }

    // -----------------------------------------------------------------------
    // Inline buffering
    // -----------------------------------------------------------------------

    public static class Inline {
        /** Flush inlined data to Parquet once this many MB are buffered. */
        private int thresholdMb = 4;
        /** Flush inlined data to Parquet once this many records are buffered. */
        private int thresholdRecords = 50_000;

        public int getThresholdMb() { return thresholdMb; }
        public void setThresholdMb(int thresholdMb) { this.thresholdMb = thresholdMb; }

        public int getThresholdRecords() { return thresholdRecords; }
        public void setThresholdRecords(int thresholdRecords) { this.thresholdRecords = thresholdRecords; }
    }

    // -----------------------------------------------------------------------
    // Compaction
    // -----------------------------------------------------------------------

    public static class Compaction {
        /**
         * Cron expression for the scheduled compaction run.
         * Uses Spring 6-field format: {@code <sec> <min> <hour> <dom> <month> <dow>}.
         * Default: daily at 03:00:00.
         */
        private String schedule = "0 0 3 * * *";
        private Entity entity = new Entity();
        private General general = new General();

        public static class Entity {
            private int minFilesPerBucket = 10;
            private int targetFileSizeMb = 256;
            private int lookbackDays = 30;
            /**
             * Parquet row-group memory budget applied during entity cassette merges
             * (DuckDB 1.5.3+ {@code write_buffer_row_group_memory_limit}).
             *
             * <p>Issued as {@code SET write_buffer_row_group_memory_limit = 'NMB'} inside
             * the same {@code synchronized(duckDB)} block before each
             * {@code ducklake_merge_adjacent_files} call.  Capping the per-row-group
             * buffer prevents OOM when a single bucket contains millions of rows — DuckDB
             * flushes the row group to Parquet once this memory threshold is reached rather
             * than waiting for the default row-count trigger.
             *
             * <p>Defaults to {@code 256} (matching {@code target-file-size-mb}) so output
             * row groups never exceed the target file size in memory before being spilled.
             * Set to {@code 0} to leave the DuckDB default in place (useful for
             * benchmarking or when the DuckDB version predates 1.5.3).
             */
            private int rowGroupMemoryLimitMb = 256;

            public int getMinFilesPerBucket() { return minFilesPerBucket; }
            public void setMinFilesPerBucket(int minFilesPerBucket) { this.minFilesPerBucket = minFilesPerBucket; }

            public int getTargetFileSizeMb() { return targetFileSizeMb; }
            public void setTargetFileSizeMb(int targetFileSizeMb) { this.targetFileSizeMb = targetFileSizeMb; }

            public int getLookbackDays() { return lookbackDays; }
            public void setLookbackDays(int lookbackDays) { this.lookbackDays = lookbackDays; }

            public int getRowGroupMemoryLimitMb() { return rowGroupMemoryLimitMb; }
            public void setRowGroupMemoryLimitMb(int rowGroupMemoryLimitMb) {
                this.rowGroupMemoryLimitMb = rowGroupMemoryLimitMb;
            }
        }

        public static class General {
            private boolean enabled = false;
            private int minFilesPerPartition = 20;
            private int targetFileSizeMb = 256;
            private int lookbackDays = 30;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }

            public int getMinFilesPerPartition() { return minFilesPerPartition; }
            public void setMinFilesPerPartition(int minFilesPerPartition) { this.minFilesPerPartition = minFilesPerPartition; }

            public int getTargetFileSizeMb() { return targetFileSizeMb; }
            public void setTargetFileSizeMb(int targetFileSizeMb) { this.targetFileSizeMb = targetFileSizeMb; }

            public int getLookbackDays() { return lookbackDays; }
            public void setLookbackDays(int lookbackDays) { this.lookbackDays = lookbackDays; }
        }

        public String getSchedule() { return schedule; }
        public void setSchedule(String schedule) { this.schedule = schedule; }

        public Entity getEntity() { return entity; }
        public void setEntity(Entity entity) { this.entity = entity; }

        public General getGeneral() { return general; }
        public void setGeneral(General general) { this.general = general; }
    }

    // -----------------------------------------------------------------------
    // Retention
    // -----------------------------------------------------------------------

    public static class Retention {
        /**
         * Cron expression for the scheduled retention enforcement run.
         * Uses Spring 6-field format: {@code <sec> <min> <hour> <dom> <month> <dow>}.
         * Default: daily at 01:00:00 (runs two hours before compaction at 03:00).
         */
        private String schedule = "0 0 1 * * *";

        public String getSchedule() { return schedule; }
        public void setSchedule(String schedule) { this.schedule = schedule; }
    }

    // -----------------------------------------------------------------------
    // Kafka
    // -----------------------------------------------------------------------

    public static class Kafka {
        private String bootstrapServers = "localhost:9092";
        private List<BrokerEntry> brokers = new ArrayList<>();

        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

        public List<BrokerEntry> getBrokers() { return brokers; }
        public void setBrokers(List<BrokerEntry> brokers) { this.brokers = brokers; }

        public static class BrokerEntry {
            private String id = "default";
            private String bootstrapServers;
            private String securityProtocol = "PLAINTEXT";
            private String saslMechanism;
            private String saslUsername;
            private String saslPassword;
            private String sslTruststorePath;
            private String sslTruststorePassword;
            private String sslKeystorePath;
            private String sslKeystorePassword;

            public String getId() { return id; }
            public void setId(String id) { this.id = id; }

            public String getBootstrapServers() { return bootstrapServers; }
            public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

            public String getSecurityProtocol() { return securityProtocol; }
            public void setSecurityProtocol(String securityProtocol) { this.securityProtocol = securityProtocol; }

            public String getSaslMechanism() { return saslMechanism; }
            public void setSaslMechanism(String saslMechanism) { this.saslMechanism = saslMechanism; }

            public String getSaslUsername() { return saslUsername; }
            public void setSaslUsername(String saslUsername) { this.saslUsername = saslUsername; }

            public String getSaslPassword() { return saslPassword; }
            public void setSaslPassword(String saslPassword) { this.saslPassword = saslPassword; }

            public String getSslTruststorePath() { return sslTruststorePath; }
            public void setSslTruststorePath(String sslTruststorePath) { this.sslTruststorePath = sslTruststorePath; }

            public String getSslTruststorePassword() { return sslTruststorePassword; }
            public void setSslTruststorePassword(String sslTruststorePassword) { this.sslTruststorePassword = sslTruststorePassword; }

            public String getSslKeystorePath() { return sslKeystorePath; }
            public void setSslKeystorePath(String sslKeystorePath) { this.sslKeystorePath = sslKeystorePath; }

            public String getSslKeystorePassword() { return sslKeystorePassword; }
            public void setSslKeystorePassword(String sslKeystorePassword) { this.sslKeystorePassword = sslKeystorePassword; }
        }
    }

    // -----------------------------------------------------------------------
    // Recording
    // -----------------------------------------------------------------------

    public static class Recording {
        /** Maximum number of messages accumulated before writing a batch. */
        private int batchSize = 10_000;
        /** Maximum time (ms) to wait before flushing an incomplete batch. */
        private long batchTimeoutMs = 1_000;
        /** Initial backoff (ms) before the first recorder restart attempt. */
        private long retryInitialIntervalMs = 5_000;
        /** Exponential multiplier applied to the backoff after each failed attempt. */
        private double retryMultiplier = 2.0;
        /** Maximum backoff (ms) between recorder restart attempts. */
        private long retryMaxIntervalMs = 300_000;   // 5 minutes

        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

        public long getBatchTimeoutMs() { return batchTimeoutMs; }
        public void setBatchTimeoutMs(long batchTimeoutMs) { this.batchTimeoutMs = batchTimeoutMs; }

        public long getRetryInitialIntervalMs() { return retryInitialIntervalMs; }
        public void setRetryInitialIntervalMs(long v) { this.retryInitialIntervalMs = v; }

        public double getRetryMultiplier() { return retryMultiplier; }
        public void setRetryMultiplier(double v) { this.retryMultiplier = v; }

        public long getRetryMaxIntervalMs() { return retryMaxIntervalMs; }
        public void setRetryMaxIntervalMs(long v) { this.retryMaxIntervalMs = v; }
    }

    // -----------------------------------------------------------------------
    // Threading
    // -----------------------------------------------------------------------

    public static class Threading {
        /** Capacity of the bounded Jox channel feeding the single DuckDB write VT. */
        private int writeChannelCapacity = 128;
        /** Default parallelism for Kafka source flows (per topic). */
        private int defaultSourceParallelism = 1;
        /** Per-topic parallelism overrides (topic name → parallelism). */
        private Map<String, Integer> topicParallelism = new HashMap<>();
        /** Thread type used for the compaction background job: "virtual" or "platform". */
        private String compactionThreadType = "virtual";

        public int getWriteChannelCapacity() { return writeChannelCapacity; }
        public void setWriteChannelCapacity(int writeChannelCapacity) { this.writeChannelCapacity = writeChannelCapacity; }

        public int getDefaultSourceParallelism() { return defaultSourceParallelism; }
        public void setDefaultSourceParallelism(int defaultSourceParallelism) { this.defaultSourceParallelism = defaultSourceParallelism; }

        public Map<String, Integer> getTopicParallelism() { return topicParallelism; }
        public void setTopicParallelism(Map<String, Integer> topicParallelism) { this.topicParallelism = topicParallelism; }

        public String getCompactionThreadType() { return compactionThreadType; }
        public void setCompactionThreadType(String compactionThreadType) { this.compactionThreadType = compactionThreadType; }
    }

    // -----------------------------------------------------------------------
    // Replay
    // -----------------------------------------------------------------------

    public static class Replay {
        /**
         * Maximum number of scheduled replays that may be pending or actively streaming
         * at any one time. Attempts to schedule beyond this limit return HTTP 429.
         */
        private int maxScheduled = 50;

        /**
         * Maximum number of steps allowed in a user-supplied transform pipeline.
         * Requests with more steps return HTTP 400.
         */
        private int maxTransformSteps = 50;

        private Follow follow = new Follow();

        public int getMaxScheduled() { return maxScheduled; }
        public void setMaxScheduled(int maxScheduled) { this.maxScheduled = maxScheduled; }

        public int getMaxTransformSteps() { return maxTransformSteps; }
        public void setMaxTransformSteps(int maxTransformSteps) { this.maxTransformSteps = maxTransformSteps; }

        public Follow getFollow() { return follow; }
        public void setFollow(Follow follow) { this.follow = follow; }

        /**
         * Tunables for the {@code follow=true} streaming replay mode.
         *
         * <p>The follow bus registers per-stream subscriptions that buffer newly-committed
         * records into a bounded in-memory queue.  These limits protect the service from
         * unbounded memory growth when a client consumes slower than writes are committing.
         */
        public static class Follow {
            /**
             * Bounded queue capacity for each follow subscription.  A subscription whose
             * queue fills up is marked overflowed and its stream is terminated by Task B.
             */
            private int bufferCapacity = 1024;

            /**
             * Maximum number of concurrent follow subscriptions across the service.
             * New follow requests beyond this limit are rejected with HTTP 503.
             */
            private int maxSubscriptions = 64;

            /**
             * Heartbeat cadence (in seconds) for idle follow streams, used by Task B
             * to keep proxies from closing long-lived SSE/NDJSON connections.
             */
            private int heartbeatSeconds = 15;

            public int getBufferCapacity() { return bufferCapacity; }
            public void setBufferCapacity(int bufferCapacity) { this.bufferCapacity = bufferCapacity; }

            public int getMaxSubscriptions() { return maxSubscriptions; }
            public void setMaxSubscriptions(int maxSubscriptions) { this.maxSubscriptions = maxSubscriptions; }

            public int getHeartbeatSeconds() { return heartbeatSeconds; }
            public void setHeartbeatSeconds(int heartbeatSeconds) { this.heartbeatSeconds = heartbeatSeconds; }
        }
    }

    // -----------------------------------------------------------------------
    // Bootstrap seed config (only applied on first start when tables are empty)
    // -----------------------------------------------------------------------

    public static class Bootstrap {
        private List<TopicEntry> topics = new ArrayList<>();
        private List<EntityEntry> entities = new ArrayList<>();

        public static class TopicEntry {
            private String topic;
            /** "general", "entity_only", or "both" */
            private String mode = "general";

            public String getTopic() { return topic; }
            public void setTopic(String topic) { this.topic = topic; }

            public String getMode() { return mode; }
            public void setMode(String mode) { this.mode = mode; }
        }

        public static class EntityEntry {
            private String type;
            private int buckets = 256;
            private List<SourceMapping> sources = new ArrayList<>();

            /**
             * One source-topic mapping for an entity type.
             *
             * <p>A single topic can carry many different message variants that all refer
             * to the same logical entity (e.g. a "fixture" entity can appear as
             * {@code marketSet}, {@code resultSet}, {@code coverage}, etc.).  Each variant
             * is expressed as a {@link Matcher} under this mapping.  The router tries every
             * matcher in declaration order and routes on the first one that yields an ID.
             */
            public static class SourceMapping {
                private String topic;
                /**
                 * Whether messages from this topic are written only to the entity cassette
                 * ({@code "entity_only"}, default) or to both the entity and general cassettes
                 * ({@code "both"}).
                 */
                private String mode = "entity_only";
                /** One or more message-variant matchers.  At least one is required. */
                private List<Matcher> matchers = new ArrayList<>();

                /**
                 * Describes how to recognise one message variant and extract the entity ID.
                 *
                 * <p>{@code messageType} is a logical label stored in the {@code message_type}
                 * column of the entity cassette so that consumers can distinguish variants
                 * (e.g. {@code "marketSet"} vs {@code "resultSet"}).
                 */
                public static class Matcher {
                    /**
                     * Logical name for this message variant (e.g. {@code "marketSet"}).
                     * Stored verbatim in the {@code message_type} column of the cassette.
                     */
                    private String messageType;
                    /** "key", "value", or "header" */
                    private String source = "value";
                    /** JSONPath expression (for value source) or header name (for header source). */
                    private String expression;

                    public String getMessageType() { return messageType; }
                    public void setMessageType(String messageType) { this.messageType = messageType; }

                    public String getSource() { return source; }
                    public void setSource(String source) { this.source = source; }

                    public String getExpression() { return expression; }
                    public void setExpression(String expression) { this.expression = expression; }
                }

                public String getTopic() { return topic; }
                public void setTopic(String topic) { this.topic = topic; }

                public String getMode() { return mode; }
                public void setMode(String mode) { this.mode = mode; }

                public List<Matcher> getMatchers() { return matchers; }
                public void setMatchers(List<Matcher> matchers) { this.matchers = matchers; }
            }

            public String getType() { return type; }
            public void setType(String type) { this.type = type; }

            public int getBuckets() { return buckets; }
            public void setBuckets(int buckets) { this.buckets = buckets; }

            public List<SourceMapping> getSources() { return sources; }
            public void setSources(List<SourceMapping> sources) { this.sources = sources; }
        }

        public List<TopicEntry> getTopics() { return topics; }
        public void setTopics(List<TopicEntry> topics) { this.topics = topics; }

        public List<EntityEntry> getEntities() { return entities; }
        public void setEntities(List<EntityEntry> entities) { this.entities = entities; }
    }

    // -----------------------------------------------------------------------
    // S3 / object-storage credentials (DuckDB httpfs / DuckLake DATA_PATH)
    // -----------------------------------------------------------------------

    /**
     * S3-compatible object storage credentials and endpoint override.
     *
     * <p>When {@code endpoint} is set DuckDB's S3 secret is configured with
     * {@code USE_SSL false} and {@code URL_STYLE path} so it works with
     * local S3-compatible servers (RustFS, MinIO, etc.).
     * Leave all fields blank in production and rely on the standard AWS credential
     * chain (instance profile, env vars, ~/.aws/credentials).
     */
    public static class S3 {
        /** Override the S3 endpoint, e.g. {@code http://localhost:9000}. Leave blank for real AWS. */
        private String endpoint = "";
        /** S3 access key / access key ID. */
        private String accessKey = "";
        /** S3 secret key. */
        private String secretKey = "";
        /** AWS region passed to DuckDB. */
        private String region = "us-east-1";

        public String getEndpoint() { return endpoint; }
        public void setEndpoint(String endpoint) { this.endpoint = endpoint; }

        public String getAccessKey() { return accessKey; }
        public void setAccessKey(String accessKey) { this.accessKey = accessKey; }

        public String getSecretKey() { return secretKey; }
        public void setSecretKey(String secretKey) { this.secretKey = secretKey; }

        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }

        /** Returns {@code true} when an explicit endpoint override is configured. */
        public boolean hasEndpoint() { return endpoint != null && !endpoint.isBlank(); }
    }

    // -----------------------------------------------------------------------
    // Object store (S3-compatible) for snapshot export via AWS SDK
    // -----------------------------------------------------------------------

    public static class ObjectStore {
        /** Override the S3 endpoint URL (e.g. for MinIO or LocalStack). Null uses AWS default. */
        private String endpointUrl;
        /** AWS access key ID. Null falls back to the default credential provider chain. */
        private String accessKey;
        /** AWS secret access key. Null falls back to the default credential provider chain. */
        private String secretKey;
        /** AWS region. */
        private String region = "us-east-1";
        /** Bucket where snapshot exports are uploaded. */
        private String bucket;
        /** Key prefix prepended to every uploaded object (no trailing slash needed). */
        private String prefix = "snapshots";
        /** Use path-style S3 URLs. Required for MinIO and LocalStack. */
        private boolean forcePathStyle = false;

        public String getEndpointUrl() { return endpointUrl; }
        public void setEndpointUrl(String endpointUrl) { this.endpointUrl = endpointUrl; }

        public String getAccessKey() { return accessKey; }
        public void setAccessKey(String accessKey) { this.accessKey = accessKey; }

        public String getSecretKey() { return secretKey; }
        public void setSecretKey(String secretKey) { this.secretKey = secretKey; }

        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }

        public String getBucket() { return bucket; }
        public void setBucket(String bucket) { this.bucket = bucket; }

        public String getPrefix() { return prefix; }
        public void setPrefix(String prefix) { this.prefix = prefix; }

        public boolean isForcePathStyle() { return forcePathStyle; }
        public void setForcePathStyle(boolean forcePathStyle) { this.forcePathStyle = forcePathStyle; }
    }

    // -----------------------------------------------------------------------
    // Root getters/setters
    // -----------------------------------------------------------------------

    public Catalog getCatalog() { return catalog; }
    public void setCatalog(Catalog catalog) { this.catalog = catalog; }

    public Inline getInline() { return inline; }
    public void setInline(Inline inline) { this.inline = inline; }

    public Compaction getCompaction() { return compaction; }
    public void setCompaction(Compaction compaction) { this.compaction = compaction; }

    public Retention getRetention() { return retention; }
    public void setRetention(Retention retention) { this.retention = retention; }

    public Kafka getKafka() { return kafka; }
    public void setKafka(Kafka kafka) { this.kafka = kafka; }

    public Recording getRecording() { return recording; }
    public void setRecording(Recording recording) { this.recording = recording; }

    public Threading getThreading() { return threading; }
    public void setThreading(Threading threading) { this.threading = threading; }

    public Replay getReplay() { return replay; }
    public void setReplay(Replay replay) { this.replay = replay; }

    public Bootstrap getBootstrap() { return bootstrap; }
    public void setBootstrap(Bootstrap bootstrap) { this.bootstrap = bootstrap; }

    public S3 getS3() { return s3; }
    public void setS3(S3 s3) { this.s3 = s3; }

    public ObjectStore getObjectStore() { return objectStore; }
    public void setObjectStore(ObjectStore objectStore) { this.objectStore = objectStore; }
}

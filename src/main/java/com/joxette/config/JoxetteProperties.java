package com.joxette.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "joxette")
public class JoxetteProperties {

    private Catalog catalog = new Catalog();
    private Inline inline = new Inline();
    private Compaction compaction = new Compaction();
    private Kafka kafka = new Kafka();
    private Recording recording = new Recording();
    private Bootstrap bootstrap = new Bootstrap();

    // -----------------------------------------------------------------------
    // Catalog
    // -----------------------------------------------------------------------

    public static class Catalog {
        /** Path to the DuckLake catalog file (DuckDB). */
        private String path = "./data/joxette.ducklake";
        /** Root path on object storage where Parquet files are written. */
        private String objectStoragePath;

        public String getPath() { return path; }
        public void setPath(String path) { this.path = path; }

        public String getObjectStoragePath() { return objectStoragePath; }
        public void setObjectStoragePath(String objectStoragePath) { this.objectStoragePath = objectStoragePath; }
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

            public int getMinFilesPerBucket() { return minFilesPerBucket; }
            public void setMinFilesPerBucket(int minFilesPerBucket) { this.minFilesPerBucket = minFilesPerBucket; }

            public int getTargetFileSizeMb() { return targetFileSizeMb; }
            public void setTargetFileSizeMb(int targetFileSizeMb) { this.targetFileSizeMb = targetFileSizeMb; }

            public int getLookbackDays() { return lookbackDays; }
            public void setLookbackDays(int lookbackDays) { this.lookbackDays = lookbackDays; }
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
    // Kafka
    // -----------------------------------------------------------------------

    public static class Kafka {
        private String bootstrapServers = "localhost:9092";

        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
    }

    // -----------------------------------------------------------------------
    // Recording
    // -----------------------------------------------------------------------

    public static class Recording {
        /** Maximum number of messages accumulated before writing a batch. */
        private int batchSize = 10_000;
        /** Maximum time (ms) to wait before flushing an incomplete batch. */
        private long batchTimeoutMs = 1_000;

        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

        public long getBatchTimeoutMs() { return batchTimeoutMs; }
        public void setBatchTimeoutMs(long batchTimeoutMs) { this.batchTimeoutMs = batchTimeoutMs; }
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

            public static class SourceMapping {
                private String topic;
                private EntityIdSpec entityId = new EntityIdSpec();

                public static class EntityIdSpec {
                    /** "key", "value", or "header" */
                    private String source = "value";
                    /** JSONPath expression to extract the entity ID. */
                    private String expression;

                    public String getSource() { return source; }
                    public void setSource(String source) { this.source = source; }

                    public String getExpression() { return expression; }
                    public void setExpression(String expression) { this.expression = expression; }
                }

                public String getTopic() { return topic; }
                public void setTopic(String topic) { this.topic = topic; }

                public EntityIdSpec getEntityId() { return entityId; }
                public void setEntityId(EntityIdSpec entityId) { this.entityId = entityId; }
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
    // Root getters/setters
    // -----------------------------------------------------------------------

    public Catalog getCatalog() { return catalog; }
    public void setCatalog(Catalog catalog) { this.catalog = catalog; }

    public Inline getInline() { return inline; }
    public void setInline(Inline inline) { this.inline = inline; }

    public Compaction getCompaction() { return compaction; }
    public void setCompaction(Compaction compaction) { this.compaction = compaction; }

    public Kafka getKafka() { return kafka; }
    public void setKafka(Kafka kafka) { this.kafka = kafka; }

    public Recording getRecording() { return recording; }
    public void setRecording(Recording recording) { this.recording = recording; }

    public Bootstrap getBootstrap() { return bootstrap; }
    public void setBootstrap(Bootstrap bootstrap) { this.bootstrap = bootstrap; }
}

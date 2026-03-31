package com.joxette.repository;

import com.joxette.db.jooq.tables.records.EntitySourceMappingsRecord;
import com.joxette.db.jooq.tables.records.EntityTypeConfigsRecord;
import com.joxette.db.jooq.tables.records.TopicConfigsRecord;
import com.joxette.model.EntitySourceMapping;
import com.joxette.model.EntityTypeConfig;
import com.joxette.model.TopicConfig;
import org.jooq.DSLContext;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import static com.joxette.db.jooq.Tables.ENTITY_SOURCE_MAPPINGS;
import static com.joxette.db.jooq.Tables.ENTITY_TYPE_CONFIGS;
import static com.joxette.db.jooq.Tables.TOPIC_CONFIGS;

/**
 * jOOQ-backed repository for the three config tables managed in the
 * {@code main} schema by {@link com.joxette.db.SchemaManager}.
 *
 * <p>DuckDB serialises writes internally, so no external locking is needed.
 * All exceptions from the DSL layer are unchecked {@link org.jooq.exception.DataAccessException}.
 */
@Repository
@DependsOn("dbSchemaManager")
public class ConfigRepository {

    private final DSLContext dsl;

    public ConfigRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    // -----------------------------------------------------------------------
    // TopicConfig
    // -----------------------------------------------------------------------

    public List<TopicConfig> findAllTopics() {
        return dsl.selectFrom(TOPIC_CONFIGS)
                .orderBy(TOPIC_CONFIGS.TOPIC)
                .fetch(this::toTopicConfig);
    }

    public Optional<TopicConfig> findTopic(String topic) {
        return dsl.selectFrom(TOPIC_CONFIGS)
                .where(TOPIC_CONFIGS.TOPIC.eq(topic))
                .fetchOptional(this::toTopicConfig);
    }

    public void upsertTopic(TopicConfig config) {
        dsl.insertInto(TOPIC_CONFIGS)
                .set(TOPIC_CONFIGS.TOPIC, config.topic())
                .set(TOPIC_CONFIGS.MODE, config.mode())
                .onConflict(TOPIC_CONFIGS.TOPIC)
                .doUpdate()
                .set(TOPIC_CONFIGS.MODE, config.mode())
                .set(TOPIC_CONFIGS.UPDATED_AT, OffsetDateTime.now())
                .execute();
    }

    public void deleteTopic(String topic) {
        dsl.deleteFrom(TOPIC_CONFIGS)
                .where(TOPIC_CONFIGS.TOPIC.eq(topic))
                .execute();
    }

    /** Returns {@code true} when the {@code topic_configs} table has no rows. */
    public boolean isTopicConfigEmpty() {
        return dsl.fetchCount(TOPIC_CONFIGS) == 0;
    }

    // -----------------------------------------------------------------------
    // EntityTypeConfig
    // -----------------------------------------------------------------------

    public List<EntityTypeConfig> findAllEntityTypes() {
        return dsl.selectFrom(ENTITY_TYPE_CONFIGS)
                .orderBy(ENTITY_TYPE_CONFIGS.ENTITY_TYPE)
                .fetch(this::toEntityTypeConfig);
    }

    public Optional<EntityTypeConfig> findEntityType(String entityType) {
        return dsl.selectFrom(ENTITY_TYPE_CONFIGS)
                .where(ENTITY_TYPE_CONFIGS.ENTITY_TYPE.eq(entityType))
                .fetchOptional(this::toEntityTypeConfig);
    }

    public void upsertEntityType(EntityTypeConfig config) {
        dsl.insertInto(ENTITY_TYPE_CONFIGS)
                .set(ENTITY_TYPE_CONFIGS.ENTITY_TYPE, config.entityType())
                .set(ENTITY_TYPE_CONFIGS.BUCKET_COUNT, config.buckets())
                .onConflict(ENTITY_TYPE_CONFIGS.ENTITY_TYPE)
                .doUpdate()
                .set(ENTITY_TYPE_CONFIGS.BUCKET_COUNT, config.buckets())
                .execute();
    }

    public void deleteEntityType(String entityType) {
        dsl.deleteFrom(ENTITY_TYPE_CONFIGS)
                .where(ENTITY_TYPE_CONFIGS.ENTITY_TYPE.eq(entityType))
                .execute();
    }

    // -----------------------------------------------------------------------
    // EntitySourceMapping
    // -----------------------------------------------------------------------

    public List<EntitySourceMapping> findAllMappings() {
        return dsl.selectFrom(ENTITY_SOURCE_MAPPINGS)
                .orderBy(ENTITY_SOURCE_MAPPINGS.ENTITY_TYPE, ENTITY_SOURCE_MAPPINGS.TOPIC)
                .fetch(this::toMapping);
    }

    public List<EntitySourceMapping> findMappingsByEntityType(String entityType) {
        return dsl.selectFrom(ENTITY_SOURCE_MAPPINGS)
                .where(ENTITY_SOURCE_MAPPINGS.ENTITY_TYPE.eq(entityType))
                .orderBy(ENTITY_SOURCE_MAPPINGS.TOPIC)
                .fetch(this::toMapping);
    }

    public List<EntitySourceMapping> findMappingsByTopic(String topic) {
        return dsl.selectFrom(ENTITY_SOURCE_MAPPINGS)
                .where(ENTITY_SOURCE_MAPPINGS.TOPIC.eq(topic))
                .orderBy(ENTITY_SOURCE_MAPPINGS.ENTITY_TYPE)
                .fetch(this::toMapping);
    }

    public void upsertMapping(EntitySourceMapping mapping) {
        dsl.insertInto(ENTITY_SOURCE_MAPPINGS)
                .set(ENTITY_SOURCE_MAPPINGS.ENTITY_TYPE, mapping.entityType())
                .set(ENTITY_SOURCE_MAPPINGS.TOPIC, mapping.topic())
                .set(ENTITY_SOURCE_MAPPINGS.ENTITY_ID_SOURCE, mapping.entityIdSource())
                .set(ENTITY_SOURCE_MAPPINGS.ENTITY_ID_EXPRESSION, mapping.entityIdExpression())
                .onConflict(ENTITY_SOURCE_MAPPINGS.ENTITY_TYPE, ENTITY_SOURCE_MAPPINGS.TOPIC)
                .doUpdate()
                .set(ENTITY_SOURCE_MAPPINGS.ENTITY_ID_SOURCE, mapping.entityIdSource())
                .set(ENTITY_SOURCE_MAPPINGS.ENTITY_ID_EXPRESSION, mapping.entityIdExpression())
                .execute();
    }

    public void deleteMapping(String entityType, String topic) {
        dsl.deleteFrom(ENTITY_SOURCE_MAPPINGS)
                .where(ENTITY_SOURCE_MAPPINGS.ENTITY_TYPE.eq(entityType)
                        .and(ENTITY_SOURCE_MAPPINGS.TOPIC.eq(topic)))
                .execute();
    }

    public void deleteMappingsByEntityType(String entityType) {
        dsl.deleteFrom(ENTITY_SOURCE_MAPPINGS)
                .where(ENTITY_SOURCE_MAPPINGS.ENTITY_TYPE.eq(entityType))
                .execute();
    }

    // -----------------------------------------------------------------------
    // Mappers
    // -----------------------------------------------------------------------

    private TopicConfig toTopicConfig(TopicConfigsRecord r) {
        return new TopicConfig(r.getTopic(), r.getMode());
    }

    private EntityTypeConfig toEntityTypeConfig(EntityTypeConfigsRecord r) {
        return new EntityTypeConfig(r.getEntityType(), r.getBucketCount());
    }

    private EntitySourceMapping toMapping(EntitySourceMappingsRecord r) {
        return new EntitySourceMapping(
                r.getEntityType(),
                r.getTopic(),
                r.getEntityIdSource(),
                r.getEntityIdExpression()
        );
    }
}

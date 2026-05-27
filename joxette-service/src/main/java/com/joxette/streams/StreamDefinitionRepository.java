package com.joxette.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.ConflictException;
import com.joxette.api.error.ResourceNotFoundException;
import com.joxette.api.error.ValidationException;
import com.joxette.replay.DedupPolicy;
import com.joxette.replay.ReplayOutputMode;
import com.joxette.replay.SolOutput;
import com.joxette.replay.StateFoldStrategy;
import com.joxette.replay.transform.TransformStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

/**
 * CRUD access for {@link StreamDefinition} rows stored in the plain-DuckDB
 * {@code stream_definitions} table (created by {@link com.joxette.db.SchemaManager}).
 *
 * <p>The full definition is stored as a JSON blob under the {@code definition}
 * column so the schema stays stable as new fields are added to the domain model.
 */
@Repository
public class StreamDefinitionRepository {

    private static final Table<?>              TBL         = DSL.table(DSL.name("stream_definitions"));
    private static final Field<String>         F_ID          = DSL.field(DSL.name("id"),          String.class);
    private static final Field<String>         F_NAME        = DSL.field(DSL.name("name"),        String.class);
    private static final Field<String>         F_ENTITY_TYPE = DSL.field(DSL.name("entity_type"), String.class);
    private static final Field<String>         F_ENTITY_ID   = DSL.field(DSL.name("entity_id"),  String.class);
    private static final Field<String>         F_DEFINITION  = DSL.field(DSL.name("definition"), String.class);
    private static final Field<OffsetDateTime> F_CREATED_AT  = DSL.field(DSL.name("created_at"), OffsetDateTime.class);
    private static final Field<OffsetDateTime> F_UPDATED_AT  = DSL.field(DSL.name("updated_at"), OffsetDateTime.class);

    private final DSLContext   dsl;
    private final ObjectMapper objectMapper;

    public StreamDefinitionRepository(DSLContext dsl, ObjectMapper objectMapper) {
        this.dsl          = dsl;
        this.objectMapper = objectMapper;
    }

    /** Returns all stream definitions ordered by id ascending. */
    public List<StreamDefinition> listAll() {
        return dsl
                .select(F_ID, F_NAME, F_ENTITY_TYPE, F_ENTITY_ID, F_DEFINITION, F_CREATED_AT, F_UPDATED_AT)
                .from(TBL)
                .orderBy(F_ID.asc())
                .fetch(this::map);
    }

    /** Returns all stream definitions for the given entity type, ordered by id. */
    public List<StreamDefinition> listByEntityType(String entityType) {
        return dsl
                .select(F_ID, F_NAME, F_ENTITY_TYPE, F_ENTITY_ID, F_DEFINITION, F_CREATED_AT, F_UPDATED_AT)
                .from(TBL)
                .where(F_ENTITY_TYPE.eq(entityType))
                .orderBy(F_ID.asc())
                .fetch(this::map);
    }

    /** Returns the stream definition with the given id, or empty if none found. */
    public Optional<StreamDefinition> findById(String id) {
        return dsl
                .select(F_ID, F_NAME, F_ENTITY_TYPE, F_ENTITY_ID, F_DEFINITION, F_CREATED_AT, F_UPDATED_AT)
                .from(TBL)
                .where(F_ID.eq(id))
                .fetchOptional(this::map);
    }

    /**
     * Creates a new stream definition.
     *
     * @throws ConflictException   if an id with this name already exists
     * @throws ValidationException if the definition cannot be serialised
     */
    public StreamDefinition create(String id, String name, String entityType, String entityId,
                                   StreamDefinition.SourceOptions source, String sol,
                                   SolOutput solOutput, List<TransformStep> transform,
                                   ReplayOutputMode output, StateFoldStrategy stateFold) {
        if (findById(id).isPresent()) {
            throw new ConflictException("Stream definition already exists: " + id);
        }
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        StreamDefinition definition = new StreamDefinition(
                id, name, entityType, entityId, source, sol,
                solOutput != null ? solOutput : SolOutput.EVENTS,
                transform,
                output != null ? output : ReplayOutputMode.EVENTS,
                stateFold,
                now.toInstant(), now.toInstant());
        String definitionJson = serialize(definition);
        dsl.insertInto(TBL)
                .columns(F_ID, F_NAME, F_ENTITY_TYPE, F_ENTITY_ID, F_DEFINITION, F_CREATED_AT, F_UPDATED_AT)
                .values(id, name, entityType, entityId, definitionJson, now, now)
                .execute();
        return findById(id).orElseThrow();
    }

    /**
     * Replaces an existing stream definition.
     *
     * @throws ResourceNotFoundException if no stream with this id exists
     * @throws ValidationException       if the definition cannot be serialised
     */
    public StreamDefinition update(String id, String name, String entityType, String entityId,
                                   StreamDefinition.SourceOptions source, String sol,
                                   SolOutput solOutput, List<TransformStep> transform,
                                   ReplayOutputMode output, StateFoldStrategy stateFold) {
        StreamDefinition existing = findById(id)
                .orElseThrow(() -> ResourceNotFoundException.stream(id));
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        StreamDefinition updated = new StreamDefinition(
                id, name, entityType, entityId, source, sol,
                solOutput != null ? solOutput : SolOutput.EVENTS,
                transform,
                output != null ? output : ReplayOutputMode.EVENTS,
                stateFold,
                existing.createdAt(), now.toInstant());
        String definitionJson = serialize(updated);
        dsl.update(TBL)
                .set(F_NAME,        name)
                .set(F_ENTITY_TYPE, entityType)
                .set(F_ENTITY_ID,   entityId)
                .set(F_DEFINITION,  definitionJson)
                .set(F_UPDATED_AT,  now)
                .where(F_ID.eq(id))
                .execute();
        return findById(id).orElseThrow();
    }

    /**
     * Deletes the stream definition with the given id.
     *
     * @return {@code true} if a row was deleted, {@code false} if not found
     */
    public boolean delete(String id) {
        return dsl.deleteFrom(TBL)
                .where(F_ID.eq(id))
                .execute() > 0;
    }

    // -------------------------------------------------------------------------
    // Mapping helpers
    // -------------------------------------------------------------------------

    private StreamDefinition map(Record r) {
        String defJson = r.get(F_DEFINITION);
        // Parse the full stored definition then re-attach the timestamps from dedicated columns
        // (the canonical source of truth for created_at / updated_at).
        try {
            StreamDefinition stored = objectMapper.readValue(defJson, StreamDefinition.class);
            OffsetDateTime createdAt = r.get(F_CREATED_AT);
            OffsetDateTime updatedAt = r.get(F_UPDATED_AT);
            return new StreamDefinition(
                    stored.id(), stored.name(), stored.entityType(), stored.entityId(),
                    stored.source(), stored.sol(), stored.solOutput(),
                    stored.transform(), stored.output(), stored.stateFold(),
                    createdAt != null ? createdAt.toInstant() : null,
                    updatedAt != null ? updatedAt.toInstant() : null);
        } catch (JsonProcessingException e) {
            // Return a minimal shell so the list endpoint doesn't explode on a corrupt row.
            OffsetDateTime createdAt = r.get(F_CREATED_AT);
            OffsetDateTime updatedAt = r.get(F_UPDATED_AT);
            return new StreamDefinition(
                    r.get(F_ID), r.get(F_NAME), r.get(F_ENTITY_TYPE), r.get(F_ENTITY_ID),
                    null, null, SolOutput.EVENTS, List.of(), ReplayOutputMode.EVENTS, null,
                    createdAt != null ? createdAt.toInstant() : null,
                    updatedAt != null ? updatedAt.toInstant() : null);
        }
    }

    private String serialize(StreamDefinition def) {
        try {
            return objectMapper.writeValueAsString(def);
        } catch (JsonProcessingException e) {
            throw new ValidationException("Stream definition could not be serialised: " + e.getOriginalMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Timestamp helper
    // -------------------------------------------------------------------------

    private static Instant toInstant(OffsetDateTime odt) {
        return odt != null ? odt.toInstant() : null;
    }
}

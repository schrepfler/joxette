package com.joxette.replay.transform;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joxette.api.error.ConflictException;
import com.joxette.api.error.ResourceNotFoundException;
import com.joxette.api.error.ValidationException;
import com.joxette.replay.transform.gap.FragmentDefinition;
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
 * CRUD access for named transform pipeline presets stored in the plain-DuckDB
 * {@code transform_presets} table (created by {@link com.joxette.db.SchemaManager}).
 *
 * <p>Steps are stored as a JSON array string and deserialised via Jackson on read.
 * On any deserialisation error the preset is returned with an empty step list.
 */
@Repository
public class TransformPresetRepository {

    private static final Table<?>              TBL         = DSL.table(DSL.name("transform_presets"));
    private static final Field<String>         F_NAME      = DSL.field(DSL.name("name"),        String.class);
    private static final Field<String>         F_DESC      = DSL.field(DSL.name("description"), String.class);
    private static final Field<String>         F_STEPS     = DSL.field(DSL.name("steps"),       String.class);
    private static final Field<String>         F_FRAGMENTS = DSL.field(DSL.name("fragments"),   String.class);
    private static final Field<OffsetDateTime> F_CREATED   = DSL.field(DSL.name("created_at"),  OffsetDateTime.class);
    private static final Field<OffsetDateTime> F_UPDATED   = DSL.field(DSL.name("updated_at"),  OffsetDateTime.class);

    private final DSLContext   dsl;
    private final ObjectMapper objectMapper;

    public TransformPresetRepository(DSLContext dsl, ObjectMapper objectMapper) {
        this.dsl          = dsl;
        this.objectMapper = objectMapper;
    }

    /** Returns all presets ordered by name ascending. */
    public List<TransformPreset> listAll() {
        return dsl
                .select(F_NAME, F_DESC, F_STEPS, F_FRAGMENTS, F_CREATED, F_UPDATED)
                .from(TBL)
                .orderBy(F_NAME.asc())
                .fetch(this::map);
    }

    /** Returns the preset with the given name, or empty if none found. */
    public Optional<TransformPreset> findByName(String name) {
        return dsl
                .select(F_NAME, F_DESC, F_STEPS, F_FRAGMENTS, F_CREATED, F_UPDATED)
                .from(TBL)
                .where(F_NAME.eq(name))
                .fetchOptional(this::map);
    }

    /**
     * Creates a new preset.
     *
     * @throws ConflictException   if a preset with this name already exists
     * @throws ValidationException if {@code steps} cannot be serialised
     */
    public TransformPreset create(String name, String description, List<TransformStep> steps) {
        if (findByName(name).isPresent()) {
            throw new ConflictException("Transform preset already exists: " + name);
        }
        String stepsJson = serializeSteps(steps);
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        dsl.insertInto(TBL)
                .columns(F_NAME, F_DESC, F_STEPS, F_CREATED, F_UPDATED)
                .values(name, description, stepsJson, now, now)
                .execute();
        return findByName(name).orElseThrow();
    }

    /**
     * Updates an existing preset's description and steps.
     *
     * @throws ResourceNotFoundException if no preset with this name exists
     * @throws ValidationException       if {@code steps} cannot be serialised
     */
    public TransformPreset update(String name, String description, List<TransformStep> steps) {
        if (findByName(name).isEmpty()) {
            throw ResourceNotFoundException.transformPreset(name);
        }
        String stepsJson = serializeSteps(steps);
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        dsl.update(TBL)
                .set(F_DESC,    description)
                .set(F_STEPS,   stepsJson)
                .set(F_UPDATED, now)
                .where(F_NAME.eq(name))
                .execute();
        return findByName(name).orElseThrow();
    }

    private String serializeSteps(List<TransformStep> steps) {
        try {
            return objectMapper.writeValueAsString(steps);
        } catch (JsonProcessingException e) {
            throw new ValidationException("Transform steps could not be serialised: " + e.getOriginalMessage());
        }
    }

    /**
     * Deletes the named preset.
     *
     * @return {@code true} if a row was deleted, {@code false} if not found
     */
    public boolean delete(String name) {
        return dsl.deleteFrom(TBL)
                .where(F_NAME.eq(name))
                .execute() > 0;
    }

    // -------------------------------------------------------------------------
    // Mapping
    // -------------------------------------------------------------------------

    private TransformPreset map(Record r) {
        String stepsJson = r.get(F_STEPS);
        List<TransformStep> steps;
        try {
            steps = objectMapper.readValue(stepsJson,
                    objectMapper.getTypeFactory()
                            .constructCollectionType(List.class, TransformStep.class));
        } catch (JsonProcessingException e) {
            steps = List.of();
        }
        String fragmentsJson = r.get(F_FRAGMENTS);
        List<FragmentDefinition> fragments;
        try {
            fragments = (fragmentsJson != null)
                    ? objectMapper.readValue(fragmentsJson,
                            objectMapper.getTypeFactory()
                                    .constructCollectionType(List.class, FragmentDefinition.class))
                    : List.of();
        } catch (JsonProcessingException e) {
            fragments = List.of();
        }
        OffsetDateTime created = r.get(F_CREATED);
        OffsetDateTime updated = r.get(F_UPDATED);
        return new TransformPreset(
                r.get(F_NAME),
                r.get(F_DESC),
                steps,
                fragments,
                created != null ? created.toInstant() : null,
                updated != null ? updated.toInstant() : null);
    }
}

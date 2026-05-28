package com.joxette.exports;

import com.joxette.api.error.ResourceNotFoundException;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class ExportJobRepository {

    private static final Table<?>              TBL               = DSL.table(DSL.name("export_jobs"));
    private static final Field<String>         F_ID              = DSL.field(DSL.name("id"),            String.class);
    private static final Field<String>         F_ENTITY_TYPE     = DSL.field(DSL.name("entity_type"),   String.class);
    private static final Field<String[]>       F_ENTITY_IDS      = DSL.field(DSL.name("entity_ids"),    String[].class);
    private static final Field<OffsetDateTime> F_FROM_TS         = DSL.field(DSL.name("from_ts"),       OffsetDateTime.class);
    private static final Field<OffsetDateTime> F_TO_TS           = DSL.field(DSL.name("to_ts"),         OffsetDateTime.class);
    private static final Field<String[]>       F_MESSAGE_TYPES   = DSL.field(DSL.name("message_types"), String[].class);
    private static final Field<String>         F_OUTPUT_FORMAT   = DSL.field(DSL.name("output_format"), String.class);
    private static final Field<String>         F_STATUS          = DSL.field(DSL.name("status"),        String.class);
    private static final Field<String>         F_OUTPUT_PATH     = DSL.field(DSL.name("output_path"),   String.class);
    private static final Field<Long>           F_ROW_COUNT       = DSL.field(DSL.name("row_count"),     Long.class);
    private static final Field<String>         F_ERROR_MESSAGE   = DSL.field(DSL.name("error_message"), String.class);
    private static final Field<OffsetDateTime> F_CREATED_AT      = DSL.field(DSL.name("created_at"),    OffsetDateTime.class);
    private static final Field<OffsetDateTime> F_STARTED_AT      = DSL.field(DSL.name("started_at"),    OffsetDateTime.class);
    private static final Field<OffsetDateTime> F_COMPLETED_AT    = DSL.field(DSL.name("completed_at"),  OffsetDateTime.class);

    private final DSLContext dsl;

    public ExportJobRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    /** Creates a new job in PENDING status and returns it. */
    public ExportJob create(String entityType, List<String> entityIds,
                            Instant from, Instant to, List<String> messageTypes,
                            ExportOutputFormat outputFormat) {
        String id = UUID.randomUUID().toString();
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        String[] idsArr     = entityIds != null ? entityIds.toArray(new String[0]) : new String[0];
        String[] typesArr   = messageTypes != null ? messageTypes.toArray(new String[0]) : null;

        dsl.insertInto(TBL)
                .columns(F_ID, F_ENTITY_TYPE, F_ENTITY_IDS, F_FROM_TS, F_TO_TS,
                         F_MESSAGE_TYPES, F_OUTPUT_FORMAT, F_STATUS, F_CREATED_AT)
                .values(id, entityType, idsArr,
                        from != null ? OffsetDateTime.ofInstant(from, ZoneOffset.UTC) : null,
                        to   != null ? OffsetDateTime.ofInstant(to,   ZoneOffset.UTC) : null,
                        typesArr,
                        outputFormat.getValue(), ExportStatus.PENDING.getValue(), now)
                .execute();
        return findById(id).orElseThrow();
    }

    /** Returns all jobs ordered newest first. */
    public List<ExportJob> listAll() {
        return dsl.select().from(TBL).orderBy(F_CREATED_AT.desc()).fetch(this::map);
    }

    /** Returns all jobs for the given entity type, newest first. */
    public List<ExportJob> listByEntityType(String entityType) {
        return dsl.select().from(TBL)
                .where(F_ENTITY_TYPE.eq(entityType))
                .orderBy(F_CREATED_AT.desc())
                .fetch(this::map);
    }

    public Optional<ExportJob> findById(String id) {
        return dsl.select().from(TBL).where(F_ID.eq(id)).fetchOptional(this::map);
    }

    /** Transitions a job to RUNNING and sets started_at. */
    public void markRunning(String id) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        dsl.update(TBL)
                .set(F_STATUS,     ExportStatus.RUNNING.getValue())
                .set(F_STARTED_AT, now)
                .where(F_ID.eq(id))
                .execute();
    }

    /** Transitions a job to COMPLETED with an output path and row count. */
    public void markCompleted(String id, String outputPath, long rowCount) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        dsl.update(TBL)
                .set(F_STATUS,       ExportStatus.COMPLETED.getValue())
                .set(F_OUTPUT_PATH,  outputPath)
                .set(F_ROW_COUNT,    rowCount)
                .set(F_COMPLETED_AT, now)
                .where(F_ID.eq(id))
                .execute();
    }

    /** Transitions a job to FAILED with an error message. */
    public void markFailed(String id, String errorMessage) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        dsl.update(TBL)
                .set(F_STATUS,       ExportStatus.FAILED.getValue())
                .set(F_ERROR_MESSAGE, errorMessage)
                .set(F_COMPLETED_AT, now)
                .where(F_ID.eq(id))
                .execute();
    }

    /** Deletes the job row. Returns true if a row was removed. */
    public boolean delete(String id) {
        return dsl.deleteFrom(TBL).where(F_ID.eq(id)).execute() > 0;
    }

    // -------------------------------------------------------------------------

    private ExportJob map(Record r) {
        String[] idsArr   = r.get(F_ENTITY_IDS);
        String[] typesArr = r.get(F_MESSAGE_TYPES);

        OffsetDateTime fromOdt  = r.get(F_FROM_TS);
        OffsetDateTime toOdt    = r.get(F_TO_TS);
        OffsetDateTime createdOdt   = r.get(F_CREATED_AT);
        OffsetDateTime startedOdt   = r.get(F_STARTED_AT);
        OffsetDateTime completedOdt = r.get(F_COMPLETED_AT);

        return new ExportJob(
                r.get(F_ID),
                r.get(F_ENTITY_TYPE),
                idsArr   != null ? Arrays.asList(idsArr)   : List.of(),
                fromOdt  != null ? fromOdt.toInstant()  : null,
                toOdt    != null ? toOdt.toInstant()    : null,
                typesArr != null ? Arrays.asList(typesArr) : null,
                ExportOutputFormat.valueOf(r.get(F_OUTPUT_FORMAT).toUpperCase()),
                ExportStatus.valueOf(r.get(F_STATUS).toUpperCase()),
                r.get(F_OUTPUT_PATH),
                r.get(F_ROW_COUNT),
                r.get(F_ERROR_MESSAGE),
                createdOdt   != null ? createdOdt.toInstant()   : null,
                startedOdt   != null ? startedOdt.toInstant()   : null,
                completedOdt != null ? completedOdt.toInstant() : null
        );
    }
}

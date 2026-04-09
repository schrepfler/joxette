package com.joxette.replay;

import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Maintains the {@code known_entities} registry (plain DuckDB, main schema).
 *
 * <p>The table records the first time and most recent time each
 * (entity_type, entity_id) pair was observed in the recording pipeline.
 * It is updated once per batch after the cassette rows have been written
 * successfully, so the registry is always a subset of what is stored in the
 * entity cassettes.
 *
 * <p>Because {@code known_entities} is a plain DuckDB table (not DuckLake),
 * the {@code PRIMARY KEY (entity_type, entity_id)} constraint is enforced and
 * {@code ON CONFLICT ... DO UPDATE} works correctly. This avoids the silent
 * duplicate-append problem that occurred when the table was DuckLake-backed.
 *
 * <h2>Upsert semantics</h2>
 * <p>On conflict, only {@code last_seen} is updated; {@code first_seen} is
 * immutable once the entity is registered.
 */
@Repository
public class KnownEntitiesRepository {

    private static final Table<?>              TABLE         = DSL.table(DSL.name("known_entities"));
    private static final Field<String>         F_ENTITY_TYPE = DSL.field(DSL.name("entity_type"),  String.class);
    private static final Field<String>         F_ENTITY_ID   = DSL.field(DSL.name("entity_id"),    String.class);
    private static final Field<OffsetDateTime> F_FIRST_SEEN  = DSL.field(DSL.name("first_seen"),   OffsetDateTime.class);
    private static final Field<OffsetDateTime> F_LAST_SEEN   = DSL.field(DSL.name("last_seen"),    OffsetDateTime.class);

    private final DSLContext dsl;

    public KnownEntitiesRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Upserts all distinct (entityType, entityId) pairs found in {@code routes}
     * using {@code observedAt} as the candidate {@code last_seen} timestamp.
     *
     * <p>Executes as a single jOOQ batch to amortise round-trip overhead.
     * The caller is responsible for deduplicating {@code routes} if desired;
     * duplicate pairs in the same batch are safe but redundant.
     *
     * @param routes     entity routes extracted from a recording batch
     * @param observedAt the wall-clock time at which this batch was processed
     * @throws SQLException if the batch execution fails
     */
    public void upsertBatch(List<EntityRoute> routes, Instant observedAt) throws SQLException {
        if (routes.isEmpty()) {
            return;
        }
        OffsetDateTime odt = observedAt.atOffset(ZoneOffset.UTC);

        var template = dsl
                .insertInto(TABLE)
                .columns(F_ENTITY_TYPE, F_ENTITY_ID, F_FIRST_SEEN, F_LAST_SEEN)
                .values((String) null, (String) null,
                        (OffsetDateTime) null, (OffsetDateTime) null)
                .onConflict(F_ENTITY_TYPE, F_ENTITY_ID)
                .doUpdate()
                .set(F_LAST_SEEN, DSL.excluded(F_LAST_SEEN));

        BatchBindStep batch = dsl.batch(template);
        for (EntityRoute route : routes) {
            batch = batch.bind(route.entityType(), route.entityId(), odt, odt);
        }
        batch.execute();
    }
}

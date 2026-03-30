package com.joxette.replay;

import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/**
 * Maintains the {@code lake.known_entities} registry.
 *
 * <p>The table records the first time and most recent time each
 * (entity_type, entity_id) pair was observed in the recording pipeline.
 * It is updated once per batch after the cassette rows have been written
 * successfully, so the registry is always a subset of what is stored in the
 * entity cassettes.
 *
 * <h2>Upsert semantics</h2>
 * <p>On conflict, only {@code last_seen} is updated. {@code first_seen} and
 * {@code entity_bucket} are immutable once the entity is registered: bucket
 * assignment is deterministic (derived from the hash of the entity type and
 * ID) so it will never differ on a subsequent sighting.
 *
 * <h2>Thread safety</h2>
 * <p>All methods synchronize on the shared DuckDB {@link Connection}. DuckDB
 * serialises writes internally, but acquiring the monitor prevents interleaved
 * statement execution from concurrent pipeline threads sharing the same
 * connection.
 */
@Repository
public class KnownEntitiesRepository {

    private static final String UPSERT_SQL = """
            INSERT INTO lake.known_entities
                (entity_type, entity_id, entity_bucket, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (entity_type, entity_id) DO UPDATE SET
                last_seen = excluded.last_seen
            """;

    private final Connection duckDB;

    public KnownEntitiesRepository(Connection duckDB) {
        this.duckDB = duckDB;
    }

    /**
     * Upserts all distinct (entityType, entityId) pairs found in {@code routes}
     * using {@code observedAt} as the candidate {@code last_seen} timestamp.
     *
     * <p>Executes as a single JDBC batch to amortise round-trip overhead.
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
        Timestamp ts = Timestamp.from(observedAt);
        synchronized (duckDB) {
            try (PreparedStatement ps = duckDB.prepareStatement(UPSERT_SQL)) {
                for (EntityRoute route : routes) {
                    ps.setString(1, route.entityType());
                    ps.setString(2, route.entityId());
                    ps.setInt(3, route.entityBucket());
                    ps.setTimestamp(4, ts); // first_seen – ignored on conflict
                    ps.setTimestamp(5, ts); // last_seen  – updated on conflict
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
    }
}

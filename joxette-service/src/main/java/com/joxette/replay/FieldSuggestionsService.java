package com.joxette.replay;

import com.joxette.db.SchemaManager;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Samples the last N messages from a cassette table and extracts JSON field
 * paths from the {@code metadata} column.
 *
 * <h2>Field path extraction</h2>
 * Uses a depth-limited recursive CTE to walk nested JSON objects, producing
 * paths like {@code $.value.address.city} in addition to top-level keys.
 * Falls back to the single-level {@code json_keys()} query if the recursive
 * CTE is not supported by the installed DuckDB version.
 *
 * <h2>Message type extraction</h2>
 * Queries {@code DISTINCT message_type} to return the event-type vocabulary
 * used in SOL MATCH patterns.
 */
@Service
public class FieldSuggestionsService {

    private static final Logger log = LoggerFactory.getLogger(FieldSuggestionsService.class);

    private static final List<String> ENVELOPE_FIELDS = List.of(
            "$.headers", "$.key", "$.offset", "$.partition", "$.timestamp", "$.topic");

    /** Maximum JSON nesting depth to traverse (avoids exponential expansion). */
    private static final int MAX_DEPTH = 4;

    private final DSLContext dsl;

    public FieldSuggestionsService(DSLContext dsl) {
        this.dsl = dsl;
    }

    // -------------------------------------------------------------------------
    // Field path extraction
    // -------------------------------------------------------------------------

    public List<String> forTopic(String topic, int limit) {
        String table = "lake.main.general_" + TopicReplayService.normalizeTopicName(topic);
        return extractFields(table, "metadata", limit);
    }

    public List<String> forEntityType(String entityType, int limit) {
        String table = "lake.main.entity_" + SchemaManager.normalize(entityType);
        return extractFields(table, "metadata", limit);
    }

    // -------------------------------------------------------------------------
    // Message type extraction (for SOL MATCH autocompletion)
    // -------------------------------------------------------------------------

    public List<String> messageTypesForTopic(String topic, int limit) {
        String table = "lake.main.general_" + TopicReplayService.normalizeTopicName(topic);
        return extractMessageTypes(table, limit);
    }

    public List<String> messageTypesForEntityType(String entityType, int limit) {
        String table = "lake.main.entity_" + SchemaManager.normalize(entityType);
        return extractMessageTypes(table, limit);
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private List<String> extractFields(String qualifiedTable, String column, int limit) {
        // Try recursive (deep) extraction first; fall back to single-level on any error.
        List<String> valueFields = extractFieldsDeep(qualifiedTable, column, limit);
        if (valueFields.isEmpty()) {
            valueFields = extractFieldsShallow(qualifiedTable, column, limit);
        }
        var result = new ArrayList<>(new TreeSet<>(valueFields));
        result.addAll(ENVELOPE_FIELDS);
        return result.stream().sorted().collect(Collectors.toList());
    }

    /**
     * Recursive CTE — walks nested JSON objects up to {@value MAX_DEPTH} levels.
     * Returns paths like {@code $.value.address.city}.
     */
    private List<String> extractFieldsDeep(String qualifiedTable, String column, int limit) {
        var valueFields = new TreeSet<String>();
        try {
            String sql = String.format("""
                    WITH sample AS (
                      SELECT %s::VARCHAR AS v
                      FROM %s
                      WHERE %s IS NOT NULL
                      ORDER BY recorded_at DESC
                      LIMIT %d
                    ),
                    flat AS (
                      SELECT v FROM sample
                      WHERE v IS NOT NULL AND v <> 'null' AND json_type(v) = 'OBJECT'
                    ),
                    RECURSIVE paths(path, val, depth) AS (
                      SELECT
                        k AS path,
                        json_extract(v, '$.' || k) AS val,
                        1 AS depth
                      FROM flat, UNNEST(json_keys(v)) AS t(k)
                      UNION ALL
                      SELECT
                        paths.path || '.' || sub.k,
                        json_extract(paths.val::VARCHAR, '$.' || sub.k),
                        paths.depth + 1
                      FROM paths, UNNEST(json_keys(paths.val::VARCHAR)) AS sub(k)
                      WHERE json_type(paths.val::VARCHAR) = 'OBJECT'
                        AND paths.depth < %d
                    )
                    SELECT DISTINCT '$.value.' || path AS field_key
                    FROM paths
                    WHERE json_type(val::VARCHAR) <> 'OBJECT'
                    ORDER BY field_key
                    """, column, qualifiedTable, column, limit, MAX_DEPTH);

            var rows = dsl.fetch(sql);
            for (var row : rows) {
                String key = row.get("field_key", String.class);
                if (key != null && !key.isBlank()) {
                    valueFields.add(key);
                }
            }
        } catch (Exception e) {
            log.debug("Deep field extraction skipped for {}: {}", qualifiedTable, e.getMessage());
        }
        return new ArrayList<>(valueFields);
    }

    /** Fallback: single-level {@code json_keys()} extraction. */
    private List<String> extractFieldsShallow(String qualifiedTable, String column, int limit) {
        var valueFields = new TreeSet<String>();
        try {
            var rows = dsl.fetch(String.format("""
                    WITH sample AS (
                      SELECT %s::VARCHAR AS v
                      FROM %s
                      WHERE %s IS NOT NULL
                      ORDER BY recorded_at DESC
                      LIMIT %d
                    )
                    SELECT DISTINCT UNNEST(json_keys(v)) AS field_key
                    FROM sample
                    WHERE v IS NOT NULL
                      AND v <> 'null'
                      AND json_type(v) = 'OBJECT'
                    ORDER BY field_key
                    """, column, qualifiedTable, column, limit));
            for (var row : rows) {
                String key = row.get("field_key", String.class);
                if (key != null && !key.isBlank()) {
                    valueFields.add("$.value." + key);
                }
            }
        } catch (Exception e) {
            log.debug("Shallow field extraction skipped for {}: {}", qualifiedTable, e.getMessage());
        }
        return new ArrayList<>(valueFields);
    }

    private List<String> extractMessageTypes(String qualifiedTable, int limit) {
        var types = new TreeSet<String>();
        try {
            var rows = dsl.fetch(String.format("""
                    SELECT DISTINCT message_type
                    FROM %s
                    WHERE message_type IS NOT NULL
                    ORDER BY message_type
                    LIMIT %d
                    """, qualifiedTable, limit));
            for (var row : rows) {
                String t = row.get("message_type", String.class);
                if (t != null && !t.isBlank()) types.add(t);
            }
        } catch (Exception e) {
            log.debug("Message type extraction skipped for {}: {}", qualifiedTable, e.getMessage());
        }
        return new ArrayList<>(types);
    }
}

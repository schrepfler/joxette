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
 * Samples the last N messages from a cassette table and extracts top-level
 * JSON keys from the {@code metadata} column using DuckDB's {@code json_keys()}.
 *
 * <p>Always appends the fixed envelope fields ($.key, $.topic, …) that are
 * available on every cassette record regardless of message content.
 */
@Service
public class FieldSuggestionsService {

    private static final Logger log = LoggerFactory.getLogger(FieldSuggestionsService.class);

    private static final List<String> ENVELOPE_FIELDS = List.of(
            "$.headers", "$.key", "$.offset", "$.partition", "$.timestamp", "$.topic");

    private final DSLContext dsl;

    public FieldSuggestionsService(DSLContext dsl) {
        this.dsl = dsl;
    }

    public List<String> forTopic(String topic, int limit) {
        String table = "lake.main.general_" + TopicReplayService.normalizeTopicName(topic);
        return extractFields(table, limit);
    }

    public List<String> forEntityType(String entityType, int limit) {
        String table = "lake.main.entity_" + SchemaManager.normalize(entityType);
        return extractFields(table, limit);
    }

    private List<String> extractFields(String qualifiedTable, int limit) {
        var valueFields = new TreeSet<String>();
        try {
            var rows = dsl.fetch(String.format("""
                    WITH sample AS (
                      SELECT metadata::VARCHAR AS v
                      FROM %s
                      WHERE metadata IS NOT NULL
                      ORDER BY recorded_at DESC
                      LIMIT %d
                    )
                    SELECT DISTINCT UNNEST(json_keys(v)) AS field_key
                    FROM sample
                    WHERE v IS NOT NULL
                      AND v <> 'null'
                      AND json_type(v) = 'OBJECT'
                    ORDER BY field_key
                    """, qualifiedTable, limit));
            for (var row : rows) {
                String key = row.get("field_key", String.class);
                if (key != null && !key.isBlank()) {
                    valueFields.add("$.value." + key);
                }
            }
        } catch (Exception e) {
            log.debug("Field extraction skipped for {}: {}", qualifiedTable, e.getMessage());
        }
        var result = new ArrayList<>(valueFields);
        result.addAll(ENVELOPE_FIELDS);
        return result.stream().sorted().collect(Collectors.toList());
    }
}

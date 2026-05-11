package com.joxette.replay;

import com.joxette.sol.EntityRecordAdapter;
import com.joxette.sol.SolResultMapper;
import com.sol.engine.SolEngine;
import com.sol.engine.SolOperation;
import com.sol.engine.SolResult;
import com.sol.model.Sequence;
import com.sol.parser.SolParseException;
import com.sol.parser.SolParser;
import org.springframework.stereotype.Service;

import com.sol.model.Tag;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runs a SOL (Sequence Operations Language) query over an entity cassette.
 *
 * <p>Loads the full ordered event sequence for one {@code (entityType, entityId)}
 * pair, converts it to the SOL model via {@link EntityRecordAdapter}, executes
 * the parsed query with {@link SolEngine}, and maps the result back to
 * {@link EntityRecord}s via {@link SolResultMapper}.
 */
@Service
public class SolMatchService {

    private final EntityReplayService entityReplayService;

    public SolMatchService(EntityReplayService entityReplayService) {
        this.entityReplayService = entityReplayService;
    }

    /**
     * Executes a SOL query against a single entity's event sequence.
     *
     * @param entityType  entity type name
     * @param entityId    entity identifier
     * @param query       SOL query string
     * @param from        optional lower bound on {@code kafka_timestamp}
     * @param to          optional upper bound on {@code kafka_timestamp}
     * @return matched / transformed records plus metadata
     * @throws SolParseException if the query cannot be parsed
     * @throws SQLException      if the cassette cannot be read
     */
    public SolMatchResult match(
            String entityType,
            String entityId,
            String query,
            Instant from,
            Instant to
    ) throws SQLException {

        List<SolOperation> ops = SolParser.parse(query);

        List<EntityRecord> records = new ArrayList<>();
        entityReplayService.streamEntityEvents(entityType, entityId, from, to, records::add);

        Sequence sequence = EntityRecordAdapter.toSequence(entityId, records);
        SolResult result  = SolEngine.execute(ops, sequence);

        List<EntityRecord> matched = SolResultMapper.toEntityRecords(result, records);

        // Build tag span map — keeps insertion order so implicit tags (SEQ, MATCHED,
        // PREFIX, SUFFIX) appear first, followed by named tags in match order.
        Map<String, TagSpan> tagSpans = new LinkedHashMap<>();
        for (Map.Entry<String, Tag> entry : result.tags().entrySet()) {
            Tag t = entry.getValue();
            tagSpans.put(entry.getKey(), new TagSpan(t.from(), t.to()));
        }

        return new SolMatchResult(
                matched,
                result.matched(),
                result.unexpectedNulls().stream()
                      .map(u -> u.location() + ": " + u.reason())
                      .toList(),
                tagSpans,
                sequence.size()
        );
    }

    /** A tag's half-open index range within the sequence. */
    public record TagSpan(int from, int to) {
        public int length() { return to - from; }
    }

    /** Result of a SOL match operation against one entity sequence. */
    public record SolMatchResult(
            List<EntityRecord> records,
            boolean matched,
            List<String> unexpectedNulls,
            /** Tag name → span within the original sequence. */
            Map<String, TagSpan> tags,
            /** Total number of events in the sequence (denominator for coverage bars). */
            int sequenceLength
    ) {}
}

package com.joxette.replay;

import com.joxette.replay.CassetteRecord.Header;
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

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
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

    /**
     * Runs the SOL query and returns ALL records with SOL match tags injected as headers.
     *
     * <p>For each event at index {@code i} in the sequence, the following headers are added:
     * <ul>
     *   <li>{@code __sol_match = "true"} if any named tag covers index {@code i}</li>
     *   <li>{@code __sol_tag:<name> = "true"} for each tag whose range covers index {@code i}</li>
     *   <li>{@code __sol_elapsed_ms:<name> = "<ms>"} on the last event of each tag span
     *       (duration from the first to the last event in the span)</li>
     * </ul>
     * Events not covered by any tag are returned unchanged.
     */
    public SolMatchResult annotate(
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
        SolResult result = SolEngine.execute(ops, sequence);

        Map<String, TagSpan> tagSpans = new LinkedHashMap<>();
        for (Map.Entry<String, Tag> entry : result.tags().entrySet()) {
            Tag t = entry.getValue();
            tagSpans.put(entry.getKey(), new TagSpan(t.from(), t.to()));
        }

        List<EntityRecord> annotated = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            EntityRecord r = records.get(i);
            List<Header> extraHeaders = buildSolHeaders(i, records, result.tags());
            if (extraHeaders.isEmpty()) {
                annotated.add(r);
            } else {
                List<Header> merged = new ArrayList<>();
                if (r.headers() != null) merged.addAll(r.headers());
                merged.addAll(extraHeaders);
                annotated.add(new EntityRecord(
                        r.entityId(), r.messageType(), r.topic(), r.partition(), r.offset(),
                        r.timestamp(), r.recordedAt(), r.key(), r.value(), merged));
            }
        }

        return new SolMatchResult(
                annotated,
                result.matched(),
                result.unexpectedNulls().stream()
                      .map(u -> u.location() + ": " + u.reason())
                      .toList(),
                tagSpans,
                sequence.size()
        );
    }

    private static List<Header> buildSolHeaders(int idx, List<EntityRecord> records, Map<String, Tag> tags) {
        List<Header> headers = new ArrayList<>();
        boolean coveredByAny = false;
        for (Map.Entry<String, Tag> entry : tags.entrySet()) {
            Tag tag = entry.getValue();
            if (idx >= tag.from() && idx < tag.to()) {
                coveredByAny = true;
                headers.add(header("__sol_tag:" + entry.getKey(), "true"));
                // Emit elapsed_ms on the last event of this tag span
                if (idx == tag.to() - 1 && tag.to() > tag.from()) {
                    Instant first = records.get(tag.from()).timestamp();
                    Instant last  = records.get(tag.to() - 1).timestamp();
                    long ms = Duration.between(first, last).toMillis();
                    headers.add(header("__sol_elapsed_ms:" + entry.getKey(), String.valueOf(ms)));
                }
            }
        }
        if (coveredByAny) headers.add(0, header("__sol_match", "true"));
        return headers;
    }

    private static Header header(String key, String value) {
        return new Header(key, Base64.getUrlEncoder().withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8)));
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

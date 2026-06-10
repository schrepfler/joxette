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

    // -----------------------------------------------------------------------
    // Batch matching across an entity type ("examples" view)
    // -----------------------------------------------------------------------

    public static final int DEFAULT_MAX_SEQUENCES = 500;
    public static final int DEFAULT_EXAMPLE_LIMIT = 25;

    /** Hard cap on events returned per example row — keeps payloads bounded. */
    static final int MAX_EXAMPLE_EVENTS = 80;

    private static final java.util.Set<String> IMPLICIT_TAGS =
            java.util.Set.of("SEQ", "MATCHED", "PREFIX", "SUFFIX");

    /**
     * Runs a SOL query across many entity sequences of one type — one sequence
     * per entity ID, in entity-listing order — and aggregates per-tag match
     * statistics plus a bounded set of example rows for the UI's examples pane.
     *
     * <p>Each sequence is loaded, executed independently, and contributes to:
     * <ul>
     *   <li>{@code totalSequences} / {@code matchedSequences} counters;</li>
     *   <li>per-tag counts ({@code model}) in pattern order, including implicit
     *       {@code Prefix}/{@code Suffix} rows and the unnamed gap regions between
     *       consecutive user tags (the {@code *} wildcards);</li>
     *   <li>up to {@code exampleLimit} examples carrying the post-pipeline event
     *       names and tag spans (spans index into {@code events}).</li>
     * </ul>
     */
    public SolBatchResult matchAcrossEntities(
            String entityType,
            String query,
            Instant from,
            Instant to,
            int maxSequences,
            int exampleLimit
    ) throws SQLException {
        List<SolOperation> ops = SolParser.parse(query);

        int total = 0;
        int matchedCount = 0;
        int prefixCount = 0;
        int suffixCount = 0;
        // User-tag name → number of sequences where the tag matched non-empty.
        // LinkedHashMap: first-seen order == pattern order (engine emits user tags
        // in match order ahead of the implicit tags).
        Map<String, Integer> tagCounts = new LinkedHashMap<>();
        // "tagA→tagB" → sequences with a non-empty gap between those adjacent tags.
        Map<String, Integer> gapCounts = new LinkedHashMap<>();
        List<SolSequenceExample> examples = new ArrayList<>();

        String cursor = null;
        outer:
        do {
            var page = entityReplayService.listEntities(
                    entityType, 100, cursor, EntityReplayService.EntitySortBy.id);
            for (EntityInfo info : page.data()) {
                if (total >= maxSequences) break outer;
                List<EntityRecord> records = new ArrayList<>();
                entityReplayService.streamEntityEvents(entityType, info.entityId(), from, to, records::add);
                if (records.isEmpty()) continue;
                total++;

                Sequence sequence = EntityRecordAdapter.toSequence(info.entityId(), records);
                SolResult result = SolEngine.execute(ops, sequence);
                if (result.matched()) matchedCount++;

                List<Map.Entry<String, Tag>> userTags = result.tags().entrySet().stream()
                        .filter(e -> !IMPLICIT_TAGS.contains(e.getKey()))
                        .filter(e -> !e.getValue().isEmpty())
                        .toList();
                for (var e : userTags) tagCounts.merge(e.getKey(), 1, Integer::sum);
                if (result.tags().containsKey("PREFIX")) prefixCount++;
                if (result.tags().containsKey("SUFFIX")) suffixCount++;
                for (int i = 0; i + 1 < userTags.size(); i++) {
                    Tag a = userTags.get(i).getValue();
                    Tag b = userTags.get(i + 1).getValue();
                    if (b.from() > a.to()) {
                        gapCounts.merge(userTags.get(i).getKey() + "→" + userTags.get(i + 1).getKey(),
                                1, Integer::sum);
                    }
                }

                if (examples.size() < exampleLimit) {
                    examples.add(toExample(info.entityId(), result));
                }
            }
            cursor = page.hasMore() ? page.nextCursor() : null;
        } while (cursor != null && total < maxSequences);

        return new SolBatchResult(total, matchedCount,
                buildModel(tagCounts, gapCounts, prefixCount, suffixCount), examples);
    }

    /** One example row: post-pipeline event names + spans indexing into them. */
    private static SolSequenceExample toExample(String entityId, SolResult result) {
        List<String> names = result.sequence().events().stream()
                .map(com.sol.model.Event::name)
                .toList();
        boolean truncated = names.size() > MAX_EXAMPLE_EVENTS;
        if (truncated) names = names.subList(0, MAX_EXAMPLE_EVENTS);

        Map<String, TagSpan> spans = new LinkedHashMap<>();
        for (Map.Entry<String, Tag> e : result.tags().entrySet()) {
            String name = e.getKey();
            if (name.equals("SEQ") || name.equals("MATCHED")) continue;   // whole/region tags add noise
            Tag t = e.getValue();
            if (t.isEmpty() || t.from() >= MAX_EXAMPLE_EVENTS) continue;
            spans.put(name, new TagSpan(t.from(), Math.min(t.to(), MAX_EXAMPLE_EVENTS)));
        }
        return new SolSequenceExample(entityId, names, spans, result.matched(), truncated);
    }

    /**
     * Pattern-ordered model rows: Prefix, then each user tag interleaved with the
     * unnamed gap regions between adjacent tags, then Suffix. Empty when no tag
     * matched anywhere (nothing to model).
     */
    private static List<ModelRow> buildModel(Map<String, Integer> tagCounts,
                                             Map<String, Integer> gapCounts,
                                             int prefixCount, int suffixCount) {
        if (tagCounts.isEmpty()) return List.of();
        List<ModelRow> rows = new ArrayList<>();
        rows.add(new ModelRow("Prefix", false, prefixCount));
        List<String> names = List.copyOf(tagCounts.keySet());
        for (int i = 0; i < names.size(); i++) {
            rows.add(new ModelRow(names.get(i), false, tagCounts.get(names.get(i))));
            if (i + 1 < names.size()) {
                int gap = gapCounts.getOrDefault(names.get(i) + "→" + names.get(i + 1), 0);
                rows.add(new ModelRow(null, true, gap));
            }
        }
        rows.add(new ModelRow("Suffix", false, suffixCount));
        return rows;
    }

    /** One row of the sequence model: a tag (or unnamed gap) and how many sequences hit it. */
    public record ModelRow(String label, boolean gap, int count) {}

    /** One example sequence for the examples pane. Spans index into {@code events}. */
    public record SolSequenceExample(
            String entityId,
            List<String> events,
            Map<String, TagSpan> tags,
            boolean matched,
            boolean truncated
    ) {}

    /** Aggregate result of {@link #matchAcrossEntities}. */
    public record SolBatchResult(
            int totalSequences,
            int matchedSequences,
            List<ModelRow> model,
            List<SolSequenceExample> examples
    ) {}

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

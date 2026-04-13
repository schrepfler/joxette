package com.joxette.replay.transform.steps;

import com.joxette.replay.transform.Predicate;
import com.joxette.replay.transform.TransformStep;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.impl.DSL;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Analyses a transform pipeline's step list and extracts {@link FilterDropStep}s
 * that can be executed as SQL {@code WHERE} predicates rather than in Java after
 * materialising rows from DuckDB.
 *
 * <h2>Eligible steps</h2>
 * <ul>
 *   <li>Only {@link FilterDropStep}s on top-level message columns are eligible.</li>
 *   <li>The caller passes an {@code eligibleFields} set of JSONPath names that
 *       correspond to actual columns in its DuckDB table.  The general-cassette
 *       service omits {@code "$.topic"} (no such column); the entity-cassette
 *       service includes it.</li>
 *   <li>{@link Predicate.Operator#MATCHES} is pushed down only for string
 *       columns ({@code topic}, {@code kafka_key}).</li>
 *   <li>{@link Predicate.Operator#CONTAINS} is pushed down only for string
 *       columns.</li>
 *   <li>Steps inside {@link ConditionalStep} or after a {@link FanOutStep} are
 *       never inspected — they remain in Java.</li>
 * </ul>
 *
 * <h2>WHERE semantics</h2>
 * <p>{@code filter_drop} drops messages <em>when</em> the predicate matches, so
 * the generated SQL {@code WHERE} clause <em>negates</em> the predicate to keep
 * only rows that would survive the drop. Example:
 * <pre>{@code
 * filter_drop  EQ  partition=3  →  WHERE "kafka_partition" != 3
 * filter_drop  IS_NULL  key     →  WHERE "kafka_key" IS NOT NULL
 * }</pre>
 */
public final class SqlPushdownAnalyzer {

    /** Mapping from logical JSONPath field name → DuckDB column name. */
    static final Map<String, String> COLUMN_MAP = Map.of(
            "$.topic",       "topic",
            "$.partition",   "kafka_partition",
            "$.offset",      "kafka_offset",
            "$.timestamp",   "kafka_timestamp",
            "$.key",         "kafka_key",
            "$.recorded_at", "recorded_at"
    );

    /** String-typed column names (support CONTAINS and MATCHES pushdown). */
    private static final Set<String> STRING_COLUMNS = Set.of("topic", "kafka_key");

    private SqlPushdownAnalyzer() {}

    /**
     * Analyses {@code steps} and separates eligible {@link FilterDropStep}s
     * (converted to a jOOQ {@code Condition}) from the steps that must still
     * execute in Java.
     *
     * @param steps         ordered step list from the pipeline
     * @param eligibleFields set of logical JSONPath field names that map to real
     *                       columns in the caller's DuckDB table
     *                       (e.g. {@code Set.of("$.partition", "$.offset", ...)})
     * @return a {@link PushdownResult} whose {@code pushdownCondition} is
     *         {@link DSL#noCondition()} when nothing was pushed down, and whose
     *         {@code remainingSteps} list preserves the original order of all
     *         non-pushed-down steps
     */
    public static PushdownResult analyze(List<TransformStep> steps,
                                         Set<String> eligibleFields) {
        Condition         pushdown  = DSL.noCondition();
        List<TransformStep> remaining = new ArrayList<>();

        for (TransformStep step : steps) {
            if (step instanceof FilterDropStep fds
                    && eligibleFields.contains(fds.field())
                    && COLUMN_MAP.containsKey(fds.field())
                    && canPushDown(fds)) {
                pushdown = pushdown.and(toPushdownCondition(fds));
            } else {
                remaining.add(step);
            }
        }

        return new PushdownResult(pushdown, List.copyOf(remaining));
    }

    // -------------------------------------------------------------------------
    // Eligibility check
    // -------------------------------------------------------------------------

    private static boolean canPushDown(FilterDropStep fds) {
        Predicate.Operator op = fds.operator();
        if (op == null) return false; // compound (and/or/not) predicates are not pushable
        String col = COLUMN_MAP.get(fds.field());
        boolean isStringCol = STRING_COLUMNS.contains(col);
        return switch (op) {
            case MATCHES, CONTAINS -> isStringCol;
            default                -> true;
        };
    }

    // -------------------------------------------------------------------------
    // Condition construction (SQL negates the drop predicate)
    // -------------------------------------------------------------------------

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Condition toPushdownCondition(FilterDropStep fds) {
        String             col      = COLUMN_MAP.get(fds.field());
        Field              rawField = DSL.field(DSL.name(col));
        Object             coerced  = coerce(fds.field(), fds.value());
        Predicate.Operator op       = fds.operator(); // non-null: canPushDown guarantees this

        // filter_drop drops when predicate matches → SQL WHERE negates the predicate
        return switch (op) {
            case EQ  -> rawField.notEqual(DSL.inline(coerced));
            case NEQ -> rawField.equal(DSL.inline(coerced));
            case GT  -> rawField.lessOrEqual(DSL.inline(coerced));
            case GTE -> rawField.lessThan(DSL.inline(coerced));
            case LT  -> rawField.greaterOrEqual(DSL.inline(coerced));
            case LTE -> rawField.greaterThan(DSL.inline(coerced));
            case CONTAINS -> {
                Field<String> sf = DSL.field(DSL.name(col), String.class);
                yield sf.notContains(DSL.inline(String.valueOf(fds.value())));
            }
            case MATCHES -> {
                Field<String> sf = DSL.field(DSL.name(col), String.class);
                yield DSL.not(DSL.condition(
                        "regexp_matches({0}, {1})",
                        sf,
                        DSL.inline(String.valueOf(fds.value()))));
            }
            case IS_NULL     -> rawField.isNotNull();
            case IS_NOT_NULL -> rawField.isNull();
        };
    }

    // -------------------------------------------------------------------------
    // Value coercion
    // -------------------------------------------------------------------------

    /**
     * Coerces the Jackson-deserialized {@code value} object to a type appropriate
     * for the given logical field so that jOOQ can emit the correct SQL literal.
     */
    private static Object coerce(String logicalField, Object value) {
        if (value == null) return null;
        return switch (logicalField) {
            case "$.partition" -> toLong(value);
            case "$.offset"    -> toLong(value);
            case "$.timestamp", "$.recorded_at" -> toOffsetDateTime(value);
            default -> value.toString();
        };
    }

    private static long toLong(Object v) {
        if (v instanceof Number n) return n.longValue();
        try { return Long.parseLong(v.toString()); }
        catch (NumberFormatException ignored) { return 0L; }
    }

    private static OffsetDateTime toOffsetDateTime(Object v) {
        if (v instanceof OffsetDateTime odt) return odt;
        String s = v.toString();
        try { return OffsetDateTime.parse(s); }
        catch (Exception ignored) { /* fall through */ }
        try { return Instant.parse(s).atOffset(ZoneOffset.UTC); }
        catch (Exception ignored) { return null; }
    }

    // -------------------------------------------------------------------------
    // Result record
    // -------------------------------------------------------------------------

    /**
     * Result of a pushdown analysis.
     *
     * @param pushdownCondition jOOQ {@code WHERE} condition to AND into the
     *                          query (never null; is {@link DSL#noCondition()} when
     *                          nothing was eligible for pushdown)
     * @param remainingSteps    steps that were not pushed down and must still be
     *                          applied in Java via {@link com.joxette.replay.transform.TransformPipeline}
     */
    public record PushdownResult(
            Condition          pushdownCondition,
            List<TransformStep> remainingSteps
    ) {}
}

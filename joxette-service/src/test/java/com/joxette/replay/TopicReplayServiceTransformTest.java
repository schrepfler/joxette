package com.joxette.replay;

import com.fasterxml.jackson.databind.node.TextNode;
import com.joxette.replay.transform.GuardedStep;
import com.joxette.replay.transform.Predicate;
import com.joxette.replay.transform.ReplayMetadataInjector;
import com.joxette.replay.transform.TransformPipeline;
import com.joxette.replay.transform.steps.FanOutStep;
import com.joxette.replay.transform.steps.FilterDropStep;
import com.joxette.replay.transform.steps.RedactStep;
import com.joxette.replay.transform.steps.SetConstantStep;
import com.joxette.replay.transform.steps.WallTimeStep;
import com.joxette.support.DuckDBTestSupport;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

import static com.joxette.replay.transform.Predicate.Operator.EQ;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link TopicReplayService} wired with a
 * {@link TransformPipeline}, against an in-memory DuckDB.
 *
 * <p>Covers:
 * <ul>
 *   <li>{@code filter_drop} on {@code $.partition} — SQL pushdown removes
 *       matching rows before materialisation.</li>
 *   <li>{@code redact} step — target field is {@code null} in every returned
 *       message's value body.</li>
 *   <li>{@code wall_time} step — {@code timestamp} field is replaced with a
 *       recent time.</li>
 *   <li>{@code set_constant} with a {@code when} guard — constant applied only
 *       to messages where the guard matches.</li>
 *   <li>{@code fan_out} step — response contains one copy per target topic per
 *       original message.</li>
 * </ul>
 *
 * <p>Follows the same setup pattern as {@link TopicReplayServiceTest}:
 * {@link DuckDBTestSupport#newConnection()} for the database,
 * {@link DSL#using(Connection, SQLDialect)} for the jOOQ context.
 */
class TopicReplayServiceTransformTest {

    private static final String TOPIC = "orders.events";

    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DEC = Base64.getUrlDecoder();

    private Connection        duckDB;
    private TopicReplayService service;

    @BeforeEach
    void setUp() throws Exception {
        duckDB  = DuckDBTestSupport.newConnection();
        DuckDBTestSupport.createGeneralCassetteTable(duckDB, TOPIC);
        DSLContext dsl = DSL.using(duckDB, SQLDialect.DUCKDB);
        service = new TopicReplayService(dsl);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (duckDB != null && !duckDB.isClosed()) duckDB.close();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static byte[] jsonBytes(String json) {
        return json.getBytes(StandardCharsets.UTF_8);
    }

    private static String decodeValue(String b64Value) {
        return new String(DEC.decode(b64Value), StandardCharsets.UTF_8);
    }

    private static String b64(String s) {
        return ENC.encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    // =========================================================================
    // filter_drop via SQL pushdown
    // =========================================================================

    @Test
    void filterDrop_onPartitionField_sqlPushdownExcludesMatchingPartition() throws Exception {
        Instant ts = Instant.parse("2024-01-01T10:00:00Z");
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 0L, ts, Instant.now(), "k0", null);
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 1, 0L, ts, Instant.now(), "k1", null);
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 2, 0L, ts, Instant.now(), "k2", null);

        // filter_drop drops partition==1; SQL pushdown converts this to WHERE kafka_partition != 1
        var pipeline = new TransformPipeline(
                List.of(new FilterDropStep(new Predicate.Leaf("$.partition", EQ, 1))), null);

        PagedResponse<CassetteRecord> page =
                service.query(TOPIC, null, null, null, null, null, 50, null, pipeline, "rid");

        assertThat(page.data()).hasSize(2);
        assertThat(page.data()).extracting(CassetteRecord::partition)
                .containsExactlyInAnyOrder(0, 2);
        assertThat(page.data()).extracting(CassetteRecord::partition).doesNotContain(1);
    }

    // =========================================================================
    // redact step
    // =========================================================================

    @Test
    void redact_targetFieldIsNullInEveryReturnedMessage() throws Exception {
        Instant ts = Instant.parse("2024-01-01T10:00:00Z");
        byte[] value = jsonBytes("{\"email\":\"user@example.com\",\"name\":\"Alice\"}");
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 0L, ts, Instant.now(), "k0", value);
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 1L, ts.plusSeconds(1), Instant.now(), "k1", value);

        var pipeline = new TransformPipeline(List.of(new RedactStep("$.value.email")), null);

        PagedResponse<CassetteRecord> page =
                service.query(TOPIC, null, null, null, null, null, 50, null, pipeline, "rid");

        assertThat(page.data()).hasSize(2);
        for (CassetteRecord r : page.data()) {
            String json = decodeValue(r.value());
            assertThat(json).doesNotContain("user@example.com");
            assertThat(json).contains("\"name\":\"Alice\"");
        }
    }

    // =========================================================================
    // wall_time step
    // =========================================================================

    @Test
    void wallTime_replacesTimestampWithRecentTimeInAllReturnedMessages() throws Exception {
        // Use an old timestamp so a replaced value is clearly distinguishable.
        Instant oldTs = Instant.parse("2020-01-01T00:00:00Z");
        Instant beforeTest = Instant.now().minusSeconds(5);

        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 0L, oldTs, Instant.now(), "k0", null);
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 1L, oldTs.plusSeconds(1), Instant.now(), "k1", null);
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 2L, oldTs.plusSeconds(2), Instant.now(), "k2", null);

        var pipeline = new TransformPipeline(List.of(new WallTimeStep("$.timestamp")), null);

        PagedResponse<CassetteRecord> page =
                service.query(TOPIC, null, null, null, null, null, 50, null, pipeline, "rid");

        assertThat(page.data()).hasSize(3);
        for (CassetteRecord r : page.data()) {
            assertThat(r.timestamp())
                    .isAfter(beforeTest)
                    .isAfter(oldTs);
        }
    }

    // =========================================================================
    // set_constant with when guard
    // =========================================================================

    @Test
    void setConstant_withWhenGuard_appliesOnlyToMatchingMessages() throws Exception {
        Instant ts = Instant.parse("2024-01-01T10:00:00Z");
        byte[] value = jsonBytes("{\"env\":\"prod\"}");

        // Partition 0 and partition 1 messages — guard targets only partition 0
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 0L, ts, Instant.now(), "k0", value);
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 1, 0L, ts, Instant.now(), "k1", value);

        var setStep     = new SetConstantStep("$.value.env", TextNode.valueOf("staging"));
        var guardedStep = new GuardedStep(new Predicate.Leaf("$.partition", EQ, 0), setStep);
        var pipeline    = new TransformPipeline(List.of(guardedStep), null);

        PagedResponse<CassetteRecord> page =
                service.query(TOPIC, null, null, null, null, null, 50, null, pipeline, "rid");

        assertThat(page.data()).hasSize(2);
        for (CassetteRecord r : page.data()) {
            String json = decodeValue(r.value());
            if (r.partition() == 0) {
                assertThat(json).contains("\"staging\"");
                assertThat(json).doesNotContain("\"prod\"");
            } else {
                assertThat(json).contains("\"prod\"");
                assertThat(json).doesNotContain("\"staging\"");
            }
        }
    }

    // =========================================================================
    // fan_out step
    // =========================================================================

    @Test
    void fanOut_responseContainsCopiesPerOriginalMessageWithCorrectTopics() throws Exception {
        Instant ts = Instant.parse("2024-01-01T10:00:00Z");
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 0L, ts, Instant.now(), "k0", null);
        DuckDBTestSupport.insertCassetteRow(duckDB, TOPIC, 0, 1L, ts.plusSeconds(1), Instant.now(), "k1", null);

        var pipeline = new TransformPipeline(
                List.of(new FanOutStep(List.of("topic-a", "topic-b", "topic-c"))), null);

        PagedResponse<CassetteRecord> page =
                service.query(TOPIC, null, null, null, null, null, 50, null, pipeline, "rid");

        // 2 original messages × 3 fan_out topics = 6 records
        assertThat(page.data()).hasSize(6);
        assertThat(page.hasMore()).isFalse();

        // Each target topic should appear exactly twice (once per original message)
        assertThat(page.data()).extracting(CassetteRecord::topic)
                .containsExactlyInAnyOrder(
                        "topic-a", "topic-a",
                        "topic-b", "topic-b",
                        "topic-c", "topic-c");
    }
}

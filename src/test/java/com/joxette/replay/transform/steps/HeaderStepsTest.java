package com.joxette.replay.transform.steps;

import com.joxette.replay.CassetteRecord;
import com.joxette.replay.transform.ReplayMessage;
import com.joxette.replay.transform.TransformPipeline;
import com.joxette.replay.transform.TransformStep;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AddHeaderStep}, {@link RemoveHeaderStep}, and
 * {@link CopyToHeaderStep} dispatched through {@link TransformPipeline}.
 */
class HeaderStepsTest {

    private static final Base64.Encoder ENC = Base64.getUrlEncoder().withoutPadding();

    private static String b64(String s) {
        return ENC.encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static CassetteRecord record(String value) {
        Instant ts = Instant.parse("2024-01-01T00:00:00Z");
        return new CassetteRecord("orders", 0, 1L, ts, ts, "key-1", value, null, null);
    }

    private static CassetteRecord record(String value, List<CassetteRecord.Header> headers) {
        Instant ts = Instant.parse("2024-01-01T00:00:00Z");
        return new CassetteRecord("orders", 0, 1L, ts, ts, "key-1", value, headers, null);
    }

    private static TransformPipeline pipeline(TransformStep... steps) {
        return new TransformPipeline(List.of(steps), null);
    }

    // =========================================================================
    // AddHeaderStep — basic add
    // =========================================================================

    @Test
    void addHeader_appendsHeader() {
        var step = new AddHeaderStep("x-env", "staging", false);
        var result = pipeline(step).apply(new ReplayMessage(record(null)), "r1");

        assertThat(result).hasSize(1);
        assertThat(result.getFirst().headers)
                .anyMatch(h -> "x-env".equals(h.key()) && "staging".equals(h.value()));
    }

    @Test
    void addHeader_defaultIfAbsentIsFalse_alwaysAdds() {
        // Default if_absent=false: add even if key already exists (Kafka allows duplicates)
        var existing = List.of(new CassetteRecord.Header("x-env", "prod"));
        var step = new AddHeaderStep("x-env", "staging", false);
        var result = pipeline(step).apply(new ReplayMessage(record(null, existing)), "r1");

        assertThat(result).hasSize(1);
        long count = result.getFirst().headers.stream()
                .filter(h -> "x-env".equals(h.key())).count();
        assertThat(count).isEqualTo(2); // both prod and staging present
    }

    @Test
    void addHeader_ifAbsentTrue_skipsWhenPresent() {
        var existing = List.of(new CassetteRecord.Header("x-env", "prod"));
        var step = new AddHeaderStep("x-env", "staging", true);
        var result = pipeline(step).apply(new ReplayMessage(record(null, existing)), "r1");

        assertThat(result).hasSize(1);
        // Only the original 'prod' header; 'staging' was not added
        assertThat(result.getFirst().headers)
                .filteredOn(h -> "x-env".equals(h.key()))
                .extracting(CassetteRecord.Header::value)
                .containsExactly("prod");
    }

    @Test
    void addHeader_ifAbsentTrue_addsWhenAbsent() {
        var step = new AddHeaderStep("x-new", "value", true);
        var result = pipeline(step).apply(new ReplayMessage(record(null)), "r1");

        assertThat(result.getFirst().headers)
                .anyMatch(h -> "x-new".equals(h.key()) && "value".equals(h.value()));
    }

    @Test
    void addHeader_templateResolvesTopicField() {
        var step = new AddHeaderStep("x-origin", "${$.topic}", false);
        var result = pipeline(step).apply(new ReplayMessage(record(null)), "r1");

        assertThat(result.getFirst().headers)
                .anyMatch(h -> "x-origin".equals(h.key()) && "orders".equals(h.value()));
    }

    @Test
    void addHeader_templateResolvesValueField() {
        String json   = "{\"order_id\":\"42\"}";
        var    step   = new AddHeaderStep("x-order", "${$.value.order_id}", false);
        var    result = pipeline(step).apply(new ReplayMessage(record(b64(json))), "r1");

        assertThat(result.getFirst().headers)
                .anyMatch(h -> "x-order".equals(h.key()) && "42".equals(h.value()));
    }

    @Test
    void addHeader_templateMissingPathResolvesToEmpty() {
        var step   = new AddHeaderStep("x-missing", "prefix-${$.value.no_such_field}-suffix", false);
        var result = pipeline(step).apply(new ReplayMessage(record(b64("{\"a\":1}"))), "r1");

        assertThat(result.getFirst().headers)
                .anyMatch(h -> "x-missing".equals(h.key()) && "prefix--suffix".equals(h.value()));
    }

    // =========================================================================
    // RemoveHeaderStep
    // =========================================================================

    @Test
    void removeHeader_removesAllMatchingHeaders() {
        var headers = List.of(
                new CassetteRecord.Header("x-trace", "aaa"),
                new CassetteRecord.Header("x-env",   "prod"),
                new CassetteRecord.Header("x-trace", "bbb"));
        var step   = new RemoveHeaderStep("x-trace");
        var result = pipeline(step).apply(new ReplayMessage(record(null, headers)), "r1");

        assertThat(result).hasSize(1);
        assertThat(result.getFirst().headers)
                .noneMatch(h -> "x-trace".equals(h.key()));
        assertThat(result.getFirst().headers)
                .anyMatch(h -> "x-env".equals(h.key()));
    }

    @Test
    void removeHeader_noOpWhenKeyAbsent() {
        var headers = List.of(new CassetteRecord.Header("x-env", "prod"));
        var step    = new RemoveHeaderStep("x-other");
        var result  = pipeline(step).apply(new ReplayMessage(record(null, headers)), "r1");

        assertThat(result.getFirst().headers).hasSize(1);
        assertThat(result.getFirst().headers.getFirst().key()).isEqualTo("x-env");
    }

    // =========================================================================
    // CopyToHeaderStep
    // =========================================================================

    @Test
    void copyToHeader_extractsValueFieldIntoHeader() {
        String json   = "{\"correlation_id\":\"corr-99\"}";
        var    step   = new CopyToHeaderStep("$.value.correlation_id", "x-correlation-id");
        var    result = pipeline(step).apply(new ReplayMessage(record(b64(json))), "r1");

        assertThat(result.getFirst().headers)
                .anyMatch(h -> "x-correlation-id".equals(h.key()) && "corr-99".equals(h.value()));
    }

    @Test
    void copyToHeader_extractsTopLevelField() {
        var step   = new CopyToHeaderStep("$.topic", "x-original-topic");
        var result = pipeline(step).apply(new ReplayMessage(record(null)), "r1");

        assertThat(result.getFirst().headers)
                .anyMatch(h -> "x-original-topic".equals(h.key()) && "orders".equals(h.value()));
    }

    @Test
    void copyToHeader_missingPathDoesNotAddHeader() {
        String json   = "{\"a\":1}";
        var    step   = new CopyToHeaderStep("$.value.no_such", "x-missing");
        var    result = pipeline(step).apply(new ReplayMessage(record(b64(json))), "r1");

        assertThat(result.getFirst().headers)
                .noneMatch(h -> "x-missing".equals(h.key()));
    }

    @Test
    void copyToHeader_nullValueDoesNotAddHeader() {
        var step   = new CopyToHeaderStep("$.value.field", "x-field");
        var result = pipeline(step).apply(new ReplayMessage(record(null)), "r1");

        assertThat(result.getFirst().headers)
                .noneMatch(h -> "x-field".equals(h.key()));
    }
}

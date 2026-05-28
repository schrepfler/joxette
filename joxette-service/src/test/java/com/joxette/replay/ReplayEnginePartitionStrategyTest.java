package com.joxette.replay;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReplayEnginePartitionStrategyTest {

    // =========================================================================
    // resolvePartition
    // =========================================================================

    @Test
    void defaultStrategy_returnsNull() {
        assertThat(ReplayEngine.resolvePartition(PartitionStrategy.DEFAULT, 3, 4)).isNull();
    }

    @Test
    void preserveStrategy_returnsSourcePartitionVerbatim() {
        assertThat(ReplayEngine.resolvePartition(PartitionStrategy.PRESERVE, 3, 4)).isEqualTo(3);
        assertThat(ReplayEngine.resolvePartition(PartitionStrategy.PRESERVE, 0, 12)).isEqualTo(0);
    }

    @ParameterizedTest(name = "source={0} targetCount={1} → expected={2}")
    @CsvSource({
        "7,  4, 3",
        "3,  4, 3",
        "0,  4, 0",
        "11, 4, 3",
        "4,  4, 0",
        "5,  3, 2",
    })
    void moduloStrategy_mapsSourceModuloTargetCount(int source, int targetCount, int expected) {
        assertThat(ReplayEngine.resolvePartition(PartitionStrategy.MODULO, source, targetCount))
                .isEqualTo(expected);
    }

    // =========================================================================
    // resolveTargetTopic
    // =========================================================================

    @Test
    void noMappings_usesTargetTopic() {
        ReplayToTopicRequest req = new ReplayToTopicRequest(
                "target", null, null, null, null, null, null, null, null);
        assertThat(ReplayEngine.resolveTargetTopic(req, "orders.events")).isEqualTo("target");
    }

    @Test
    void mappingPresent_returnsMappedTopic() {
        ReplayToTopicRequest req = new ReplayToTopicRequest(
                null, null, null, null, null, null, null,
                Map.of("orders.events", "orders.events.staging"), null);
        assertThat(ReplayEngine.resolveTargetTopic(req, "orders.events"))
                .isEqualTo("orders.events.staging");
    }

    @Test
    void mappingAbsent_fallsBackToTargetTopic() {
        ReplayToTopicRequest req = new ReplayToTopicRequest(
                "fallback", null, null, null, null, null, null,
                Map.of("payments.events", "payments.events.staging"), null);
        assertThat(ReplayEngine.resolveTargetTopic(req, "orders.events")).isEqualTo("fallback");
    }

    @Test
    void noMappingNoTarget_returnsSourceTopicAsIdentity() {
        ReplayToTopicRequest req = new ReplayToTopicRequest(
                null, null, null, null, null, null, null,
                Map.of("payments.events", "payments.events.staging"), null);
        // orders.events not in map, no targetTopic → identity
        assertThat(ReplayEngine.resolveTargetTopic(req, "orders.events"))
                .isEqualTo("orders.events");
    }

    // =========================================================================
    // ReplayToTopicRequest validation
    // =========================================================================

    @Test
    void requestWithNeitherTargetNorMappings_isValidIdentityRouting() {
        // All fields null/absent = identity routing: each source topic → itself
        ReplayToTopicRequest req = new ReplayToTopicRequest(
                null, null, null, null, null, null, null, null, null);
        assertThat(req.targetTopic()).isNull();
        assertThat(req.topicMappings()).isNull();
        assertThat(req.partitionStrategy()).isEqualTo(PartitionStrategy.DEFAULT);
    }

    @Test
    void requestWithTargetOnly_defaultsPartitionStrategyToDefault() {
        ReplayToTopicRequest req = new ReplayToTopicRequest(
                "target", null, null, null, null, null, null, null, null);
        assertThat(req.partitionStrategy()).isEqualTo(PartitionStrategy.DEFAULT);
    }

    @Test
    void requestWithMappingsOnly_defaultsPartitionStrategyToDefault() {
        ReplayToTopicRequest req = new ReplayToTopicRequest(
                null, null, null, null, null, null, null,
                Map.of("a", "b"), null);
        assertThat(req.partitionStrategy()).isEqualTo(PartitionStrategy.DEFAULT);
    }

    @Test
    void requestExplicitPartitionStrategy_preserved() {
        ReplayToTopicRequest req = new ReplayToTopicRequest(
                "target", null, null, null, null, null, null, null,
                PartitionStrategy.MODULO);
        assertThat(req.partitionStrategy()).isEqualTo(PartitionStrategy.MODULO);
    }
}

package com.joxette.operator.cluster;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WorkloadReadinessTest {

    @Test
    void accumulatesAcrossWorkloadsTreatingNullsAsZero() {
        WorkloadReadiness r = WorkloadReadiness.zero()
                .plus(3, 2)        // recorder: 3 desired, 2 ready
                .plus(2, 2)        // replay: all ready
                .plus(1, null);    // compaction: status not populated yet
        assertThat(r.desired()).isEqualTo(6);
        assertThat(r.ready()).isEqualTo(4);
    }

    @Test
    void progressingUntilAllReady() {
        WorkloadReadiness r = WorkloadReadiness.zero().plus(3, 2);
        assertThat(r.allReady()).isFalse();
        assertThat(r.phase()).isEqualTo("Progressing");
        assertThat(r.message()).isEqualTo("2/3 replicas ready");
    }

    @Test
    void readyWhenAllReplicasReady() {
        WorkloadReadiness r = WorkloadReadiness.zero().plus(1, 1).plus(2, 2);
        assertThat(r.allReady()).isTrue();
        assertThat(r.phase()).isEqualTo("Ready");
    }

    @Test
    void zeroDesiredIsNotReady() {
        // No workloads applied yet => not "Ready" (avoids a spurious green on first pass).
        assertThat(WorkloadReadiness.zero().allReady()).isFalse();
        assertThat(WorkloadReadiness.zero().phase()).isEqualTo("Progressing");
    }
}

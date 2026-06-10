package com.joxette.operator.rest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterRefResolverTest {

    @Test
    void buildsInClusterServiceDns() {
        assertThat(ClusterRefResolver.baseUrl("prod", "joxette"))
                .isEqualTo("http://prod.joxette.svc:8080");
    }

    @Test
    void rejectsBlankClusterName() {
        assertThatThrownBy(() -> ClusterRefResolver.baseUrl("  ", "joxette"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

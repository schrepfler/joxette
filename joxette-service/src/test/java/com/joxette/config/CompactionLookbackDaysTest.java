package com.joxette.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@code lookback-days} below zero must fail binding, not silently disable the lookback. */
class CompactionLookbackDaysTest {

    @ParameterizedTest
    @ValueSource(strings = {"entity", "general"})
    void negativeLookbackDays_failsBinding(String section) {
        assertThatThrownBy(() -> bind(section, "-1"))
                .isInstanceOf(BindException.class)
                .rootCause()
                .hasMessageContaining("lookback-days must be >= 0");
    }

    @ParameterizedTest
    @ValueSource(strings = {"entity", "general"})
    void zeroLookbackDays_isAccepted(String section) {
        JoxetteProperties.Compaction compaction = bind(section, "0").getCompaction();
        int lookback = section.equals("entity")
                ? compaction.getEntity().getLookbackDays()
                : compaction.getGeneral().getLookbackDays();
        assertThat(lookback).isZero();
    }

    @Test
    void defaultLookbackDays_isOne() {
        JoxetteProperties props = new JoxetteProperties();
        assertThat(props.getCompaction().getEntity().getLookbackDays()).isEqualTo(1);
        assertThat(props.getCompaction().getGeneral().getLookbackDays()).isEqualTo(1);
    }

    private static JoxetteProperties bind(String section, String value) {
        var source = new MapConfigurationPropertySource(
                Map.of("joxette.compaction." + section + ".lookback-days", value));
        return new Binder(source).bind("joxette", JoxetteProperties.class).get();
    }
}

package com.joxette.replay;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Parses-and-rejects coverage for {@link Order#parse(String)}. */
class OrderTest {

    @ParameterizedTest
    @CsvSource({
            "asc,  ASC",
            "ASC,  ASC",
            "Asc,  ASC",
            " asc, ASC",
            "desc, DESC",
            "DESC, DESC",
            "Desc, DESC",
    })
    void parse_acceptsValidValues_caseInsensitive(String raw, Order expected) {
        assertThat(Order.parse(raw)).isEqualTo(expected);
    }

    @org.junit.jupiter.api.Test
    void parse_nullDefaultsToAsc() {
        assertThat(Order.parse(null)).isEqualTo(Order.ASC);
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "ascending", "up", "down", "reverse", "1" })
    void parse_rejectsInvalidValues(String raw) {
        assertThatThrownBy(() -> Order.parse(raw))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid order");
    }
}

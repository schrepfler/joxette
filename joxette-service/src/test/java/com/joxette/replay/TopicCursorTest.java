package com.joxette.replay;

import com.joxette.api.error.InvalidCursorException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicCursorTest {

    private static final TopicCursor SAMPLE =
            new TopicCursor(Instant.parse("2025-06-01T10:00:00Z"), 3, 4567L);

    @Test
    void encode_thenDecode_roundTripsExactly() {
        TopicCursor decoded = TopicCursor.decode(SAMPLE.encode());
        assertThat(decoded).isEqualTo(SAMPLE);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidCursors")
    void decode_rejectsTamperedOrMalformedCursors(String label, Function<String, String> corrupt) {
        String valid = SAMPLE.encode();
        String corrupted = corrupt.apply(valid);
        assertThatThrownBy(() -> TopicCursor.decode(corrupted))
                .as(label)
                .isInstanceOf(InvalidCursorException.class);
    }

    static Stream<Arguments> invalidCursors() {
        return Stream.of(
                Arguments.of("tampered payload (flipped byte)", (Function<String, String>) valid -> {
                    int sep = valid.lastIndexOf('.');
                    String payload = valid.substring(0, sep);
                    String signature = valid.substring(sep + 1);
                    char[] chars = payload.toCharArray();
                    chars[0] = (char) (chars[0] ^ 1);
                    return new String(chars) + "." + signature;
                }),
                Arguments.of("wrong signature", (Function<String, String>) valid -> {
                    int sep = valid.lastIndexOf('.');
                    return valid.substring(0, sep) + ".wrong0000000000000000000000000";
                }),
                Arguments.of("malformed base64 payload (correctly signed, garbage payload)",
                        (Function<String, String>) valid -> CursorSignature.sign("not-valid-base64!!!"))
        );
    }
}

package com.joxette.replay;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies {@link CursorSignature} fails loudly instead of silently signing/verifying with a
 * known fallback key when no {@link CursorSigningKey} has installed a real one yet.
 *
 * <p>{@link #resetForTesting()} state is process-global (a static field), so every test here
 * restores a real key in {@code @AfterEach} — this test class is the only place in the suite
 * that ever puts {@link CursorSignature} into the "uninstalled" state, and it must not leak
 * that state to other test classes sharing this JVM fork.
 */
class CursorSignatureTest {

    private static final byte[] TEST_KEY =
            "cursor-signature-test-key-0123456789".getBytes(StandardCharsets.UTF_8);

    @AfterEach
    void restoreInstalledKey() {
        CursorSignature.install(TEST_KEY);
    }

    @Test
    void sign_beforeKeyInstalled_throwsIllegalStateException() {
        CursorSignature.resetForTesting();

        assertThatThrownBy(() -> CursorSignature.sign("payload"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cursor signing key not installed");
    }

    @Test
    void verify_beforeKeyInstalled_throwsIllegalStateException() {
        CursorSignature.resetForTesting();

        assertThatThrownBy(() -> CursorSignature.verify("payload.signature"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cursor signing key not installed");
    }

    @Test
    void sign_afterKeyInstalled_noLongerThrows() {
        CursorSignature.resetForTesting();
        CursorSignature.install(TEST_KEY);

        String signed = CursorSignature.sign("payload");

        assertThat(CursorSignature.verify(signed)).isEqualTo("payload");
    }
}

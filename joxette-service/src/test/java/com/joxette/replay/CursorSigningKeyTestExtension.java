package com.joxette.replay;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.nio.charset.StandardCharsets;

/**
 * Installs a deterministic {@link CursorSignature} key before test classes that exercise
 * {@link TopicCursor}/{@link EntityCursor} encode/decode (directly, or indirectly through
 * {@code TopicReplayService}/{@code EntityReplayService}) without a Spring context.
 *
 * <p>In production, {@link CursorSigningKey}'s constructor installs the real key during
 * Spring context refresh, before any HTTP request can reach a cursor encode/decode call.
 * Plain unit/integration tests that construct the replay services directly never trigger
 * that bean, so without this extension {@link CursorSignature} throws
 * {@code IllegalStateException} by design (see {@link CursorSignature}) rather than silently
 * falling back to a known key.
 */
final class CursorSigningKeyTestExtension implements BeforeAllCallback {

    static final byte[] TEST_KEY =
            "cursor-signing-key-test-extension-key-0".getBytes(StandardCharsets.UTF_8);

    @Override
    public void beforeAll(ExtensionContext context) {
        CursorSignature.install(TEST_KEY);
    }
}

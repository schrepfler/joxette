package com.joxette.replay;

import com.joxette.config.JoxetteProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

/**
 * Installs the effective HMAC signing key for {@link TopicCursor}/{@link EntityCursor} into
 * {@link CursorSignature} once at startup.
 *
 * <p>When {@code joxette.security.cursor-signing-key} is unset, a random 256-bit key is
 * generated for this process only. Cursors signed with a random key do not survive a
 * service restart — any client holding a {@code nextCursor} from before the restart gets
 * {@link com.joxette.api.error.InvalidCursorException} (HTTP 400) on the next page request.
 * Set the property explicitly for any deployment where cursors must survive a restart or
 * where multiple instances must accept each other's cursors.
 */
@Component
public class CursorSigningKey {

    private static final Logger log = LoggerFactory.getLogger(CursorSigningKey.class);

    public CursorSigningKey(JoxetteProperties properties) {
        String configured = properties.getSecurity().getCursorSigningKey();
        byte[] keyBytes;
        if (configured == null || configured.isBlank()) {
            keyBytes = new byte[32];
            new SecureRandom().nextBytes(keyBytes);
            log.warn("joxette.security.cursor-signing-key is not set — using a random " +
                    "per-process key. Cursors issued before a restart will fail with " +
                    "ERR_INVALID_CURSOR after restart. Set joxette.security.cursor-signing-key " +
                    "for stable cursors across restarts or multiple instances.");
        } else {
            keyBytes = configured.getBytes(StandardCharsets.UTF_8);
        }
        CursorSignature.install(keyBytes);
    }
}

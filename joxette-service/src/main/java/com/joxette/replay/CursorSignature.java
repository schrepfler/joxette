package com.joxette.replay;

import com.joxette.api.error.InvalidCursorException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HMAC-SHA256 signing/verification for {@link TopicCursor} and {@link EntityCursor}.
 *
 * <p>Both cursor records use static {@code encode}/{@code decode} methods called from many
 * sites in {@code TopicReplayService} and {@code EntityReplayService}; rather than threading
 * a signing key through every one of them, {@link CursorSigningKey} installs the effective
 * key here once at Spring context startup, exactly like {@code TopicCursor}'s own
 * {@code MAPPER} field is a static constant today. Package-private: only the two cursor
 * records use it.
 */
final class CursorSignature {

    private static final String ALGORITHM = "HmacSHA256";
    // Used only when no CursorSigningKey bean has installed a real key — e.g. a plain
    // unit test constructing TopicCursor/EntityCursor without the Spring context.
    private static final byte[] FALLBACK_KEY =
            "test-only-unconfigured-cursor-key".getBytes(StandardCharsets.UTF_8);
    private static final AtomicReference<byte[]> KEY = new AtomicReference<>();

    private CursorSignature() {}

    static void install(byte[] keyBytes) {
        KEY.set(keyBytes);
    }

    /** Appends {@code .<base64url-hmac>} to {@code payload}. */
    static String sign(String payload) {
        return payload + "." + hmac(effectiveKey(), payload);
    }

    /**
     * Splits {@code signed} into payload and signature at the last {@code '.'}, recomputes
     * the HMAC, and returns the payload if it matches in constant time.
     *
     * @throws InvalidCursorException if the input has no signature separator, or the
     *         recomputed HMAC does not match
     */
    static String verify(String signed) {
        int sep = signed.lastIndexOf('.');
        if (sep < 0) {
            throw InvalidCursorException.malformed(
                    new IllegalArgumentException("cursor is missing its signature"));
        }
        String payload   = signed.substring(0, sep);
        String signature = signed.substring(sep + 1);
        String expected = hmac(effectiveKey(), payload);
        if (!MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8),
                signature.getBytes(StandardCharsets.UTF_8))) {
            throw InvalidCursorException.malformed(
                    new IllegalArgumentException("cursor signature does not match"));
        }
        return payload;
    }

    private static byte[] effectiveKey() {
        byte[] key = KEY.get();
        return key != null ? key : FALLBACK_KEY;
    }

    private static String hmac(byte[] key, String payload) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(key, ALGORITHM));
            byte[] raw = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        }
    }
}

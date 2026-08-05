package com.joxette.replay;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.joxette.config.JoxetteProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises both branches of {@link CursorSigningKey}: an unset
 * {@code joxette.security.cursor-signing-key} property (random per-process key + WARN log) and
 * a configured one (deterministic key derived from the configured string).
 */
class CursorSigningKeyTest {

    private static final byte[] RESTORE_KEY =
            "cursor-signing-key-test-restore-key-0".getBytes(StandardCharsets.UTF_8);

    @AfterEach
    void restoreInstalledKey() {
        // CursorSignature's installed key is process-global; leave it usable for any other
        // test class sharing this JVM fork.
        CursorSignature.install(RESTORE_KEY);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    void unsetOrBlankProperty_installsRandomKey_andSigningWorks(String cursorSigningKey) {
        JoxetteProperties properties = new JoxetteProperties();
        properties.getSecurity().setCursorSigningKey(cursorSigningKey);

        new CursorSigningKey(properties);

        // The installed key is usable immediately — sign/verify round-trips.
        String signed = CursorSignature.sign("payload");
        assertThat(CursorSignature.verify(signed)).isEqualTo("payload");
    }

    @Test
    void unsetProperty_logsWarning() {
        JoxetteProperties properties = new JoxetteProperties();
        properties.getSecurity().setCursorSigningKey(null);

        Logger logger = (Logger) LoggerFactory.getLogger(CursorSigningKey.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level savedLevel = logger.getLevel();
        logger.setLevel(Level.WARN);
        logger.addAppender(appender);

        try {
            new CursorSigningKey(properties);
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(savedLevel);
        }

        boolean warned = appender.list.stream()
                .anyMatch(event -> event.getLevel() == Level.WARN
                        && event.getFormattedMessage().contains("cursor-signing-key is not set"));
        assertThat(warned)
                .as("expected a WARN log about the unset cursor-signing-key property")
                .isTrue();
    }

    @Test
    void configuredProperty_derivesDeterministicKey_reproducibleAcrossInstances() {
        JoxetteProperties first = new JoxetteProperties();
        first.getSecurity().setCursorSigningKey("shared-secret-value");
        new CursorSigningKey(first);
        String signed = CursorSignature.sign("payload");

        // A second CursorSigningKey built from the same configured string must derive the
        // same key — verifying a cursor signed under the first instance still succeeds.
        JoxetteProperties second = new JoxetteProperties();
        second.getSecurity().setCursorSigningKey("shared-secret-value");
        new CursorSigningKey(second);

        assertThat(CursorSignature.verify(signed)).isEqualTo("payload");
    }

    @Test
    void configuredProperty_doesNotLogWarning() {
        JoxetteProperties properties = new JoxetteProperties();
        properties.getSecurity().setCursorSigningKey("shared-secret-value");

        Logger logger = (Logger) LoggerFactory.getLogger(CursorSigningKey.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level savedLevel = logger.getLevel();
        logger.setLevel(Level.WARN);
        logger.addAppender(appender);

        try {
            new CursorSigningKey(properties);
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(savedLevel);
        }

        assertThat(appender.list).isEmpty();
    }
}

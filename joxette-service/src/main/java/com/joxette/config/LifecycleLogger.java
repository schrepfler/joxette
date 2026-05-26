package com.joxette.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Brackets startup and shutdown with wall-clock log lines so timing problems
 * are immediately visible in logs.
 */
@Component
public class LifecycleLogger {

    private static final Logger log = LoggerFactory.getLogger(LifecycleLogger.class);

    private final long startMs = System.currentTimeMillis();

    @EventListener
    public void onReady(ApplicationReadyEvent event) {
        log.info("=== Joxette ready — startup took {} ms ===",
                System.currentTimeMillis() - startMs);
    }

    @EventListener
    public void onContextClosing(ContextClosedEvent event) {
        log.info("=== Joxette context closing — initiating ordered shutdown ===");
    }
}

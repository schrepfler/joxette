package com.joxette.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

/**
 * Encapsulates the set of subsystem roles this Joxette instance is configured to run.
 *
 * <p>Configure via {@code joxette.roles} in {@code application.yml}. The special value
 * {@code "all"} (the default) activates every subsystem: {@code recorder},
 * {@code entity-router}, {@code compaction}, and {@code replay}.
 *
 * <h2>Valid roles</h2>
 * <dl>
 *   <dt>{@code recorder}</dt>
 *   <dd>Starts Kafka consumers and writes incoming messages to general cassettes.
 *       Controlled via {@link com.joxette.management.RecordingStartupRunner}.</dd>
 *
 *   <dt>{@code entity-router}</dt>
 *   <dd>Activates entity extraction and routing to entity cassettes within the recording
 *       pipeline.  Requires {@code recorder} to be active as well.</dd>
 *
 *   <dt>{@code compaction}</dt>
 *   <dd>Registers the cron-scheduled compaction job and the
 *       {@code POST /compaction/trigger} REST endpoint.
 *       Controlled via {@link com.joxette.compaction.CompactionScheduler}.</dd>
 *
 *   <dt>{@code replay}</dt>
 *   <dd>Registers the {@code /cassettes/**} REST endpoints. Can also be disabled
 *       independently via {@code joxette.replay.enabled=false}.</dd>
 *
 *   <dt>{@code all}</dt>
 *   <dd>Synthetic alias that expands to all four roles above (the default).</dd>
 * </dl>
 *
 * <h2>Configuration examples</h2>
 * <pre>
 * # All-in-one (default)
 * joxette:
 *   roles: [all]
 *
 * # Dedicated write node (recorder + entity routing, no compaction or replay API)
 * joxette:
 *   roles: [recorder, entity-router]
 *
 * # Compaction-only maintenance node
 * joxette:
 *   roles: [compaction]
 *
 * # Read-only replay node (disables Kafka consumers and compaction scheduler)
 * joxette:
 *   roles: [replay]
 * </pre>
 *
 * @see ConditionalOnRole
 * @see OnRoleCondition
 */
@Component
@ConfigurationProperties(prefix = "joxette")
public class InstanceRoles {

    private static final Logger log = LoggerFactory.getLogger(InstanceRoles.class);

    /** All concrete role names (excluding the synthetic {@code "all"} alias). */
    public static final Set<String> ALL_ROLES =
            Set.of("recorder", "entity-router", "compaction", "replay");

    private List<String> roles = List.of("all");

    // -------------------------------------------------------------------------
    // Startup log
    // -------------------------------------------------------------------------

    @PostConstruct
    public void init() {
        log.info("Starting Joxette with roles: {}", resolvedRoles().stream().sorted().toList());
    }

    // -------------------------------------------------------------------------
    // Role queries
    // -------------------------------------------------------------------------

    /**
     * Returns {@code true} if the {@code recorder} role is active on this instance.
     * When active, this node starts Kafka consumers and writes to general cassettes.
     */
    public boolean isRecorder() { return isActive("recorder"); }

    /**
     * Returns {@code true} if the {@code entity-router} role is active on this instance.
     * When active, the recording pipeline routes messages to entity cassettes in addition
     * to (or instead of) the general cassette.
     */
    public boolean isEntityRouter() { return isActive("entity-router"); }

    /**
     * Returns {@code true} if the {@code compaction} role is active on this instance.
     * When active, the scheduled compaction job and the {@code POST /compaction/trigger}
     * REST endpoint are registered.
     */
    public boolean isCompaction() { return isActive("compaction"); }

    /**
     * Returns {@code true} if the {@code replay} role is active on this instance.
     * When active, the {@code /cassettes/**} REST endpoints are registered.
     *
     * <p>Note: replay can also be disabled independently via
     * {@code joxette.replay.enabled=false}, regardless of this role.
     */
    public boolean isReplay() { return isActive("replay"); }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Returns the fully-expanded set of active roles, replacing the {@code "all"}
     * alias with all four concrete role names.
     */
    public Set<String> resolvedRoles() {
        if (roles.contains("all")) {
            return ALL_ROLES;
        }
        return Set.copyOf(roles);
    }

    private boolean isActive(String role) {
        return roles.contains("all") || roles.contains(role);
    }

    // -------------------------------------------------------------------------
    // ConfigurationProperties binding
    // -------------------------------------------------------------------------

    public List<String> getRoles() { return roles; }

    public void setRoles(List<String> roles) {
        if (roles == null || roles.isEmpty()) {
            this.roles = List.of("all");
        } else {
            this.roles = List.copyOf(roles);
        }
    }
}

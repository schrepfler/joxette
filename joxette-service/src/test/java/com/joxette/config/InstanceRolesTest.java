package com.joxette.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link InstanceRoles} role resolution and subsystem-activation logic.
 *
 * <p>Verifies:
 * <ul>
 *   <li>The {@code "all"} alias expands to all four concrete roles.</li>
 *   <li>Each single role activates exactly one subsystem.</li>
 *   <li>Multi-role combinations activate the expected subsystems.</li>
 *   <li>Edge cases: null/empty role lists default to {@code "all"}.</li>
 *   <li>{@link InstanceRoles#resolvedRoles()} never contains the synthetic {@code "all"} token.</li>
 * </ul>
 */
class InstanceRolesTest {

    // -------------------------------------------------------------------------
    // "all" alias expansion
    // -------------------------------------------------------------------------

    @Test
    void allExpandsToEveryConcreteRole() {
        var roles = rolesOf("all");
        assertThat(roles.resolvedRoles())
                .containsExactlyInAnyOrderElementsOf(InstanceRoles.ALL_ROLES);
    }

    @Test
    void allActivatesEverySubsystem() {
        var roles = rolesOf("all");
        assertThat(roles.isRecorder()).isTrue();
        assertThat(roles.isEntityRouter()).isTrue();
        assertThat(roles.isCompaction()).isTrue();
        assertThat(roles.isReplay()).isTrue();
    }

    @Test
    void defaultConstructorIsEquivalentToAll() {
        var roles = new InstanceRoles();
        assertThat(roles.isRecorder()).isTrue();
        assertThat(roles.isEntityRouter()).isTrue();
        assertThat(roles.isCompaction()).isTrue();
        assertThat(roles.isReplay()).isTrue();
    }

    // -------------------------------------------------------------------------
    // Single-role activation: only the named subsystem should be active
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "role={0} → recorder={1}, entityRouter={2}, compaction={3}, replay={4}")
    @CsvSource({
            "recorder,      true,  false, false, false",
            "entity-router, false, true,  false, false",
            "compaction,    false, false, true,  false",
            "replay,        false, false, false, true",
    })
    void singleRoleActivatesOnlyThatSubsystem(
            String role,
            boolean expectedRecorder,
            boolean expectedEntityRouter,
            boolean expectedCompaction,
            boolean expectedReplay) {

        var roles = rolesOf(role);
        assertThat(roles.isRecorder()).as("recorder").isEqualTo(expectedRecorder);
        assertThat(roles.isEntityRouter()).as("entityRouter").isEqualTo(expectedEntityRouter);
        assertThat(roles.isCompaction()).as("compaction").isEqualTo(expectedCompaction);
        assertThat(roles.isReplay()).as("replay").isEqualTo(expectedReplay);
    }

    // -------------------------------------------------------------------------
    // Multi-role combinations
    // -------------------------------------------------------------------------

    @Test
    void recorderAndEntityRouterWriteNode() {
        var roles = rolesOf("recorder", "entity-router");
        assertThat(roles.isRecorder()).isTrue();
        assertThat(roles.isEntityRouter()).isTrue();
        assertThat(roles.isCompaction()).isFalse();
        assertThat(roles.isReplay()).isFalse();
    }

    @Test
    void compactionAndReplayMaintenanceNode() {
        var roles = rolesOf("compaction", "replay");
        assertThat(roles.isRecorder()).isFalse();
        assertThat(roles.isEntityRouter()).isFalse();
        assertThat(roles.isCompaction()).isTrue();
        assertThat(roles.isReplay()).isTrue();
    }

    @Test
    void threeRolesExcludesMissingOne() {
        var roles = rolesOf("recorder", "entity-router", "compaction");
        assertThat(roles.isRecorder()).isTrue();
        assertThat(roles.isEntityRouter()).isTrue();
        assertThat(roles.isCompaction()).isTrue();
        assertThat(roles.isReplay()).isFalse();
    }

    // -------------------------------------------------------------------------
    // resolvedRoles() contents
    // -------------------------------------------------------------------------

    @Test
    void resolvedRolesNeverContainsAllToken() {
        assertThat(rolesOf("all").resolvedRoles()).doesNotContain("all");
    }

    @Test
    void resolvedRolesReflectsExplicitList() {
        assertThat(rolesOf("recorder", "compaction").resolvedRoles())
                .containsExactlyInAnyOrder("recorder", "compaction");
    }

    @Test
    void resolvedRolesIsImmutable() {
        var resolved = rolesOf("replay").resolvedRoles();
        // resolvedRoles() returns an unmodifiable set — mutation must throw
        assertThat(resolved).isUnmodifiable();
    }

    // -------------------------------------------------------------------------
    // setRoles() edge cases
    // -------------------------------------------------------------------------

    @Test
    void nullRoleListDefaultsToAll() {
        var roles = new InstanceRoles();
        roles.setRoles(null);
        assertThat(roles.isRecorder()).isTrue();
        assertThat(roles.isCompaction()).isTrue();
    }

    @Test
    void emptyRoleListDefaultsToAll() {
        var roles = new InstanceRoles();
        roles.setRoles(List.of());
        assertThat(roles.isRecorder()).isTrue();
        assertThat(roles.isCompaction()).isTrue();
    }

    // -------------------------------------------------------------------------
    // ALL_ROLES constant completeness
    // -------------------------------------------------------------------------

    @Test
    void allRolesConstantContainsAllFourRoles() {
        assertThat(InstanceRoles.ALL_ROLES)
                .containsExactlyInAnyOrder("recorder", "entity-router", "compaction", "replay");
    }

    // -------------------------------------------------------------------------
    // Helper
    // -------------------------------------------------------------------------

    private static InstanceRoles rolesOf(String... roleValues) {
        var ir = new InstanceRoles();
        ir.setRoles(List.of(roleValues));
        return ir;
    }
}

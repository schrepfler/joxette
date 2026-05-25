package com.joxette.config;

import org.springframework.context.annotation.Conditional;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a Spring bean as conditional on a specific Joxette instance role being active.
 *
 * <p>The active roles are read from the {@code joxette.roles} configuration property.
 * A bean annotated with {@code @ConditionalOnRole("compaction")} is only registered
 * when the active roles include {@code "compaction"} or the special {@code "all"} alias.
 *
 * <h2>Usage</h2>
 * <pre>
 * &#64;Component
 * &#64;ConditionalOnRole("compaction")
 * public class CompactionScheduler { … }
 * </pre>
 *
 * @see OnRoleCondition
 * @see InstanceRoles
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Conditional(OnRoleCondition.class)
public @interface ConditionalOnRole {

    /**
     * The role name that must be active for the annotated bean to be registered.
     * One of: {@code "recorder"}, {@code "entity-router"}, {@code "compaction"},
     * {@code "replay"}.
     */
    String value();
}

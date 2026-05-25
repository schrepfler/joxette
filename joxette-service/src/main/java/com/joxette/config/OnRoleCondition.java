package com.joxette.config;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.List;
import java.util.Map;

/**
 * Spring {@link Condition} that backs the {@link ConditionalOnRole} annotation.
 *
 * <p>Reads {@code joxette.roles} from the Spring {@link org.springframework.core.env.Environment}
 * using {@link Binder} so that all Spring property sources (YAML, environment variables,
 * system properties, test overrides) are evaluated consistently, even before the
 * application context is fully initialised.
 *
 * <p>A bean matches when the bound role list contains either the required role name or the
 * special alias {@code "all"}.  When {@code joxette.roles} is absent, the condition
 * defaults to {@code ["all"]} so every bean is registered out of the box.
 */
public class OnRoleCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        Map<String, Object> attrs =
                metadata.getAnnotationAttributes(ConditionalOnRole.class.getName());
        if (attrs == null) {
            return true; // defensive: no annotation attributes → do not gate
        }

        String requiredRole = (String) attrs.get("value");

        List<String> roles = Binder.get(context.getEnvironment())
                .bind("joxette.roles", Bindable.listOf(String.class))
                .orElse(List.of("all"));

        return roles.contains("all") || roles.contains(requiredRole);
    }
}

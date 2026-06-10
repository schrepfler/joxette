package com.joxette.operator.entity;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

/**
 * {@code EntityType} custom resource — declarative entity-type configuration,
 * reconciled into a running cluster's {@code /entities} REST API.
 *
 * <p>API group/version: {@code joxette.dev/v1alpha1}.
 */
@Group("joxette.dev")
@Version("v1alpha1")
@Kind("EntityType")
@ShortNames("jetype")
public class EntityType
        extends CustomResource<EntityTypeSpec, EntityTypeStatus>
        implements Namespaced {
}

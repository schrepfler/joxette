package com.joxette.operator.entity;

import com.joxette.operator.rest.ClusterRefResolver;
import com.joxette.operator.rest.JoxetteRestClient;
import com.joxette.operator.rest.RestClientFactory;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Reconciles an {@link EntityType} into a running cluster's {@code /entities} REST
 * API: resolve clusterRef → create/update the type → upsert source mappings, and
 * apply {@code deletionPolicy} on CR removal via the {@link Cleaner} hook.
 */
@Component
@ControllerConfiguration
public class EntityTypeReconciler implements Reconciler<EntityType>, Cleaner<EntityType> {

    private static final Logger log = LoggerFactory.getLogger(EntityTypeReconciler.class);

    private final RestClientFactory clientFactory;

    public EntityTypeReconciler(RestClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    private static final Duration CLUSTER_NOT_READY_BACKOFF = Duration.ofSeconds(15);

    @Override
    public UpdateControl<EntityType> reconcile(EntityType resource, Context<EntityType> context) {
        EntityTypeSpec spec = resource.getSpec();
        JoxetteRestClient client = clientFor(resource);

        if (!client.ready()) {
            log.info("EntityType '{}': cluster '{}' not ready, rescheduling",
                    resource.getMetadata().getName(), spec.getClusterRef().getName());
            EntityTypeStatus progressing = new EntityTypeStatus("Progressing",
                    "Waiting for cluster '" + spec.getClusterRef().getName() + "' to become ready");
            progressing.setRegistered(false);
            progressing.setObservedGeneration(resource.getMetadata().getGeneration());
            resource.setStatus(progressing);
            return UpdateControl.patchStatus(resource).rescheduleAfter(CLUSTER_NOT_READY_BACKOFF);
        }

        int sources = new EntityConverger(client).converge(spec);
        log.info("EntityType '{}' -> type '{}': {} source(s) applied",
                resource.getMetadata().getName(), spec.getType(), sources);

        EntityTypeStatus status = new EntityTypeStatus("Ready", "Configured");
        status.setRegistered(true);
        status.setSourceCount(sources);
        status.setObservedGeneration(resource.getMetadata().getGeneration());
        resource.setStatus(status);
        return UpdateControl.patchStatus(resource);
    }

    @Override
    public DeleteControl cleanup(EntityType resource, Context<EntityType> context) {
        JoxetteRestClient.ChangeResult result =
                new EntityConverger(clientFor(resource)).onDelete(resource.getSpec());
        log.info("EntityType '{}' deletion ({}): type '{}' -> {}",
                resource.getMetadata().getName(), resource.getSpec().getDeletionPolicy(),
                resource.getSpec().getType(), result);
        return DeleteControl.defaultDelete();
    }

    private JoxetteRestClient clientFor(EntityType resource) {
        String baseUrl = ClusterRefResolver.baseUrl(
                resource.getSpec().getClusterRef().getName(),
                resource.getMetadata().getNamespace());
        return clientFactory.forBaseUrl(baseUrl);
    }
}

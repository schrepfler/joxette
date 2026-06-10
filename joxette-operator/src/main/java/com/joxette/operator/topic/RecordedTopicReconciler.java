package com.joxette.operator.topic;

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
 * Reconciles a {@link RecordedTopic} into a running cluster's {@code /topics} REST
 * API (not Kubernetes objects).
 *
 * <ol>
 *   <li>Resolve {@code clusterRef} → Service DNS base URL.</li>
 *   <li>Diff desired vs current and POST/PUT to converge ({@link TopicConverger}).</li>
 *   <li>On CR delete, apply {@code deletionPolicy} via the {@link Cleaner} hook
 *       (JOSDK manages the finalizer automatically).</li>
 * </ol>
 *
 * <p>REST failures propagate so JOSDK retries with backoff; the status is set to
 * {@code Degraded} via {@link #updateErrorStatus}.
 */
@Component
@ControllerConfiguration
public class RecordedTopicReconciler implements Reconciler<RecordedTopic>, Cleaner<RecordedTopic> {

    private static final Logger log = LoggerFactory.getLogger(RecordedTopicReconciler.class);

    private final RestClientFactory clientFactory;

    public RecordedTopicReconciler(RestClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    /** How long to wait before re-checking a cluster that isn't ready yet. */
    private static final Duration CLUSTER_NOT_READY_BACKOFF = Duration.ofSeconds(15);

    @Override
    public UpdateControl<RecordedTopic> reconcile(RecordedTopic resource, Context<RecordedTopic> context) {
        RecordedTopicSpec spec = resource.getSpec();
        JoxetteRestClient client = clientFor(resource);

        // Don't hammer the REST API before the target cluster is serving — reschedule.
        if (!client.ready()) {
            log.info("RecordedTopic '{}': cluster '{}' not ready, rescheduling",
                    resource.getMetadata().getName(), spec.getClusterRef().getName());
            resource.setStatus(progressing(resource, "Waiting for cluster '"
                    + spec.getClusterRef().getName() + "' to become ready"));
            return UpdateControl.patchStatus(resource).rescheduleAfter(CLUSTER_NOT_READY_BACKOFF);
        }

        JoxetteRestClient.ChangeResult result = new TopicConverger(client).converge(spec);
        log.info("RecordedTopic '{}' -> topic '{}' on {}: {}",
                resource.getMetadata().getName(), spec.getTopic(), client.baseUrl(), result);

        RecordedTopicStatus status = new RecordedTopicStatus("Ready", "Registered (" + result + ")");
        status.setRegistered(true);
        status.setObservedGeneration(resource.getMetadata().getGeneration());
        resource.setStatus(status);
        return UpdateControl.patchStatus(resource);
    }

    @Override
    public DeleteControl cleanup(RecordedTopic resource, Context<RecordedTopic> context) {
        RecordedTopicSpec spec = resource.getSpec();
        JoxetteRestClient.ChangeResult result =
                new TopicConverger(clientFor(resource)).onDelete(spec);
        log.info("RecordedTopic '{}' deletion ({}): topic '{}' -> {}",
                resource.getMetadata().getName(), spec.getDeletionPolicy(), spec.getTopic(), result);
        return DeleteControl.defaultDelete();
    }

    private RecordedTopicStatus progressing(RecordedTopic resource, String message) {
        RecordedTopicStatus status = new RecordedTopicStatus("Progressing", message);
        status.setRegistered(false);
        status.setObservedGeneration(resource.getMetadata().getGeneration());
        return status;
    }

    private JoxetteRestClient clientFor(RecordedTopic resource) {
        String baseUrl = ClusterRefResolver.baseUrl(
                resource.getSpec().getClusterRef().getName(),
                resource.getMetadata().getNamespace());
        return clientFactory.forBaseUrl(baseUrl);
    }
}

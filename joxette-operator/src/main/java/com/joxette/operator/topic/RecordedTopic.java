package com.joxette.operator.topic;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

/**
 * {@code RecordedTopic} custom resource — declarative topic recording, reconciled
 * into a running cluster's {@code /topics} REST API (not Kubernetes objects).
 *
 * <p>API group/version: {@code joxette.dev/v1alpha1}.
 */
@Group("joxette.dev")
@Version("v1alpha1")
@Kind("RecordedTopic")
@ShortNames("rtopic")
public class RecordedTopic
        extends CustomResource<RecordedTopicSpec, RecordedTopicStatus>
        implements Namespaced {
}

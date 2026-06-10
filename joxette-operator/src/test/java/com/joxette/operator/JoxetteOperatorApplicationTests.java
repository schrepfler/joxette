package com.joxette.operator;

import com.joxette.operator.cluster.JoxetteClusterReconciler;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Context-load smoke test: proves the JOSDK Spring Boot starter (6.4.2) wires up
 * on Spring Boot 4 / JDK 25 and that {@link JoxetteClusterReconciler} is registered
 * as a bean. A mock KubernetesClient is provided so the auto-configured Operator
 * has a client without reaching a real cluster.
 */
@SpringBootTest(properties = {
        // Don't have the Operator open informers / contact the API server in tests.
        "javaoperatorsdk.client.namespace=test",
})
@EnableKubernetesMockClient(crud = true)
class JoxetteOperatorApplicationTests {

    static KubernetesMockServer server;
    static KubernetesClient mockClient;

    @TestConfiguration
    static class MockClientConfig {
        @Bean
        @Primary
        KubernetesClient kubernetesClient() {
            return mockClient;
        }
    }

    @Autowired
    JoxetteClusterReconciler reconciler;

    @Test
    void contextLoadsAndReconcilerIsRegistered() {
        assertThat(reconciler).isNotNull();
    }
}

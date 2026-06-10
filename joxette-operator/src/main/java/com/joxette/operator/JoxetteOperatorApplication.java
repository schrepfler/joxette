package com.joxette.operator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the Joxette Kubernetes operator.
 *
 * <p>The Java Operator SDK Spring Boot starter auto-configures the {@code Operator}
 * bean and registers every {@code Reconciler} bean on the context (e.g.
 * {@link com.joxette.operator.cluster.JoxetteClusterReconciler}). Health, metrics,
 * and config binding come from Spring Boot Actuator.
 */
@SpringBootApplication
public class JoxetteOperatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(JoxetteOperatorApplication.class, args);
    }
}

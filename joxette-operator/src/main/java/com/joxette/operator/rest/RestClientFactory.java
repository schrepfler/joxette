package com.joxette.operator.rest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Creates a {@link JoxetteRestClient} for a resolved base URL. A bean so the API
 * reconcilers can depend on it and tests can substitute a stub that returns a
 * client backed by a mock HTTP server.
 *
 * <p>{@code joxette.operator.api-key} (env {@code JOXETTE_OPERATOR_API-KEY}, typically
 * projected from a Secret — see {@code deploy/operator/deployment.yaml}) is a single
 * key sent to every target cluster. It only needs to match a given cluster's
 * {@code joxette.security.api-key} for mutating calls against that cluster to
 * succeed; clusters running without API-key auth ignore the header entirely.
 */
@Component
public class RestClientFactory {

    private final String apiKey;

    public RestClientFactory(@Value("${joxette.operator.api-key:}") String apiKey) {
        this.apiKey = apiKey;
    }

    public JoxetteRestClient forBaseUrl(String baseUrl) {
        return new JoxetteRestClient(baseUrl, apiKey);
    }
}

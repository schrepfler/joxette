package com.joxette.operator.rest;

import org.springframework.stereotype.Component;

/**
 * Creates a {@link JoxetteRestClient} for a resolved base URL. A bean so the API
 * reconcilers can depend on it and tests can substitute a stub that returns a
 * client backed by a mock HTTP server.
 */
@Component
public class RestClientFactory {

    public JoxetteRestClient forBaseUrl(String baseUrl) {
        return new JoxetteRestClient(baseUrl);
    }
}

package com.joxette.api.error;

import org.springframework.http.HttpStatus;

import java.net.URI;

public class CatalogQueryException extends JoxetteException {

    private static final URI TYPE = URI.create("https://joxette.dev/problems/catalog-query");

    public CatalogQueryException(String detail, Throwable cause) {
        super(HttpStatus.BAD_REQUEST, TYPE, "Catalog Query Error", detail, "ERR_CATALOG_QUERY", cause);
    }
}

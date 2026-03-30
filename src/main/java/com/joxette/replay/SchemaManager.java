package com.joxette.replay;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Initialises the DuckDB schema on application startup.
 *
 * <p>Runs once, after Spring has wired all beans, via {@link PostConstruct}.
 * Any {@link SQLException} propagates as an unchecked wrapper so that the
 * application context fails fast rather than silently starting with an
 * incomplete schema.
 */
@Component
public class SchemaManager {

    private final Connection duckDB;

    public SchemaManager(Connection duckDB) {
        this.duckDB = duckDB;
    }

    @PostConstruct
    public void initialize() throws SQLException {
        HeadersHelper.registerMacros(duckDB);
    }
}

package com.joxette.db;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Spring Boot {@link HealthIndicator} that verifies the DuckLake catalog is
 * accessible and reports the active backend.
 *
 * <p>Included automatically in:
 * <ul>
 *   <li>{@code GET /actuator/health} — top-level application health.</li>
 *   <li>{@code GET /actuator/health/readiness} — Kubernetes readiness probe
 *       (Spring Boot includes all {@link HealthIndicator} beans in the readiness
 *       group by default).</li>
 * </ul>
 *
 * <p>The check queries {@code ducklake_settings('<catalog>')} to confirm that the
 * named catalog is attached and responsive.  A result row signals {@code UP}; a
 * missing row or any {@link SQLException} signals {@code DOWN} with the exception
 * message as a detail.
 *
 * <h2>Startup behaviour</h2>
 * <p>{@link DuckLakeManager#initialize()} already throws and prevents Spring
 * context startup if the initial {@code ATTACH} fails — this indicator provides
 * the <em>ongoing</em> readiness signal for probes after the application has started.
 *
 * <h2>Sample response ({@code GET /actuator/health})</h2>
 * <pre>
 * {
 *   "status": "UP",
 *   "components": {
 *     "catalog": {
 *       "status": "UP",
 *       "details": {
 *         "backend":          "EMBEDDED_DUCKDB",
 *         "catalogType":      "duckdb",
 *         "extensionVersion": "1.0.0",
 *         "dataPath":         "s3://my-bucket/joxette/data"
 *       }
 *     }
 *   }
 * }
 * </pre>
 */
@Component("catalog")
public class CatalogHealthIndicator implements HealthIndicator {

    private final DuckLakeManager duckLakeManager;

    public CatalogHealthIndicator(DuckLakeManager duckLakeManager) {
        this.duckLakeManager = duckLakeManager;
    }

    /**
     * Probes the DuckLake catalog by querying {@code ducklake_settings()}.
     *
     * <p>Health detail keys:
     * <dl>
     *   <dt>{@code backend}</dt><dd>{@link CatalogBackend} name (e.g. {@code EMBEDDED_DUCKDB})</dd>
     *   <dt>{@code catalogType}</dt><dd>Value from {@code ducklake_settings().catalog_type}</dd>
     *   <dt>{@code extensionVersion}</dt><dd>DuckLake extension version string</dd>
     *   <dt>{@code dataPath}</dt><dd>Object-storage root path configured in DuckLake</dd>
     * </dl>
     */
    @Override
    public Health health() {
        CatalogBackend catalogBackend = duckLakeManager.getBackend();
        String backendName = catalogBackend != null ? catalogBackend.name() : "UNKNOWN";

        try (Statement stmt = duckLakeManager.getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT * FROM ducklake_settings('" + DuckLakeManager.CATALOG_NAME + "')")) {

            if (!rs.next()) {
                return Health.down()
                    .withDetail("backend", backendName)
                    .withDetail("reason", "ducklake_settings() returned no rows — catalog may not be attached")
                    .build();
            }

            return Health.up()
                .withDetail("backend", backendName)
                .withDetail("catalogType", rs.getString("catalog_type"))
                .withDetail("extensionVersion", rs.getString("extension_version"))
                .withDetail("dataPath", rs.getString("data_path"))
                .build();

        } catch (SQLException e) {
            return Health.down()
                .withDetail("backend", backendName)
                .withException(e)
                .build();
        }
    }
}

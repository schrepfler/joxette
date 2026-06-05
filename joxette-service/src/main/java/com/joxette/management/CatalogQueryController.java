package com.joxette.management;

import com.joxette.api.error.CatalogQueryException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

@Tag(name = "Catalog",
     description = "Ad-hoc SQL execution against the embedded DuckDB catalog. Intended for diagnostics and exploration.")
@RestController
@RequestMapping("/catalog")
public class CatalogQueryController {

    @Schema(description = "Metadata for a single column in the result set")
    public record ColumnMeta(
            @Schema(description = "Column name as reported by the JDBC driver", example = "topic")
            String name,
            @Schema(description = "SQL type name as reported by the JDBC driver", example = "VARCHAR")
            String typeName) {}

    @Schema(description = "Request body for POST /catalog/query")
    public record QueryRequest(
            @NotBlank
            @Schema(description = "SQL statement to execute", example = "SELECT topic, mode FROM topic_configs LIMIT 10")
            String sql,

            @Min(1) @Max(50_000)
            @Schema(description = "Maximum number of rows to return; defaults to 10 000", defaultValue = "10000",
                    minimum = "1", maximum = "50000")
            int maxRows) {

        public QueryRequest {
            if (maxRows == 0) maxRows = 10_000;
        }
    }

    @Schema(description = "Response from POST /catalog/query")
    public record QueryResponse(
            @Schema(description = "Column metadata in result-set order; empty for non-SELECT statements")
            List<ColumnMeta> columns,
            @Schema(description = "Rows as lists of values; empty for non-SELECT statements")
            List<List<Object>> rows,
            @Schema(description = "Number of rows returned", example = "42")
            int rowCount,
            @Schema(description = "Number of rows affected (INSERT/UPDATE/DELETE); -1 for SELECT statements", example = "-1")
            long affectedRows,
            @Schema(description = "True when the result set was capped at maxRows and more rows exist", example = "false")
            boolean truncated,
            @Schema(description = "Wall-clock execution time in milliseconds", example = "12")
            long durationMs,
            @Schema(description = "True for queries that return a result set (SELECT); false for DML/DDL", example = "true")
            boolean isQuery) {}

    private final Connection duckDB;

    public CatalogQueryController(Connection duckDB) {
        this.duckDB = duckDB;
    }

    @Operation(
            summary = "Execute an ad-hoc SQL query against the catalog",
            description = """
                    Executes an arbitrary SQL statement against the embedded DuckDB catalog.
                    For SELECT-like statements the result set is returned as a list of rows.
                    For DML/DDL statements `affectedRows` is populated and `rows` is empty.
                    Results are capped at `maxRows` (default 10 000); `truncated=true` when more rows exist.
                    Binary columns are base64-encoded. TIMESTAMP, VARIANT, JSON, LIST, and STRUCT columns
                    are serialised as strings.
                    """)
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Query executed successfully",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = QueryResponse.class),
                            examples = @ExampleObject(name = "select", value = """
                                    {
                                      "columns": [{"name": "topic", "typeName": "VARCHAR"}, {"name": "mode", "typeName": "VARCHAR"}],
                                      "rows": [["orders.events", "both"], ["audit.log", "general"]],
                                      "rowCount": 2,
                                      "affectedRows": -1,
                                      "truncated": false,
                                      "durationMs": 4,
                                      "isQuery": true
                                    }"""))),
            @ApiResponse(responseCode = "400", description = "SQL syntax error or execution failure",
                    content = @Content(mediaType = "application/problem+json"))
    })
    @PostMapping(value = "/query",
                 consumes = MediaType.APPLICATION_JSON_VALUE,
                 produces = MediaType.APPLICATION_JSON_VALUE)
    public QueryResponse query(@Valid @RequestBody QueryRequest request) {
        long start = System.currentTimeMillis();
        try {
            synchronized (duckDB) {
                try (Statement stmt = duckDB.createStatement()) {
                    boolean hasResultSet = stmt.execute(request.sql());
                    long durationMs = System.currentTimeMillis() - start;

                    if (hasResultSet) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            ResultSetMetaData meta = rs.getMetaData();
                            int colCount = meta.getColumnCount();

                            List<ColumnMeta> columns = new ArrayList<>(colCount);
                            for (int i = 1; i <= colCount; i++) {
                                columns.add(new ColumnMeta(meta.getColumnName(i), meta.getColumnTypeName(i)));
                            }

                            List<List<Object>> rows = new ArrayList<>();
                            boolean truncated = false;
                            while (rs.next()) {
                                if (rows.size() >= request.maxRows()) {
                                    truncated = true;
                                    break;
                                }
                                List<Object> row = new ArrayList<>(colCount);
                                for (int i = 1; i <= colCount; i++) {
                                    row.add(mapCell(rs, i, meta.getColumnType(i), meta.getColumnTypeName(i)));
                                }
                                rows.add(row);
                            }

                            return new QueryResponse(columns, rows, rows.size(), -1L, truncated, durationMs, true);
                        }
                    } else {
                        long affected = stmt.getUpdateCount();
                        return new QueryResponse(List.of(), List.of(), 0, affected, false, durationMs, false);
                    }
                }
            }
        } catch (SQLException e) {
            throw new CatalogQueryException(e.getMessage(), e);
        }
    }

    private Object mapCell(ResultSet rs, int col, int sqlType, String typeName) throws SQLException {
        if (sqlType == Types.BLOB || sqlType == Types.BINARY || sqlType == Types.VARBINARY
                || sqlType == Types.LONGVARBINARY) {
            byte[] bytes = rs.getBytes(col);
            return bytes == null ? null : Base64.getEncoder().encodeToString(bytes);
        }
        String upper = typeName.toUpperCase();
        if (sqlType == Types.TIMESTAMP || sqlType == Types.TIMESTAMP_WITH_TIMEZONE
                || upper.startsWith("VARIANT") || upper.startsWith("JSON")
                || upper.startsWith("LIST") || upper.startsWith("STRUCT")
                || upper.startsWith("MAP") || upper.startsWith("UNION")) {
            return rs.getString(col);
        }
        return rs.getObject(col);
    }
}

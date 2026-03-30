package com.joxette.replay;

/**
 * Row-count and size statistics for a topic's slice of the general cassette.
 *
 * <p>{@code estimatedSizeBytes} is the DuckDB-reported estimated size of the
 * whole {@code lake.cassette} table (not per-topic) and is intended as an
 * order-of-magnitude guide only.
 */
public record CassetteStats(String topic, String tableName, long rowCount, long estimatedSizeBytes) {}

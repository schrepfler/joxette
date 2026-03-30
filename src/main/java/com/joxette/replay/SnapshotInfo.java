package com.joxette.replay;

import java.time.Instant;

/** Metadata record for a DuckDB EXPORT DATABASE snapshot. */
public record SnapshotInfo(String name, Instant createdAt, long sizeBytes) {}

package com.joxette.replay;

import java.time.Instant;

/**
 * Metadata returned after a successful snapshot export to an S3-compatible object store.
 *
 * @param name            snapshot name
 * @param createdAt       when the snapshot was taken
 * @param sizeBytes       total size of the exported files in bytes
 * @param objectStoreUri  base S3 URI where the snapshot files were uploaded
 *                        (e.g. {@code s3://my-bucket/snapshots/snap-123/})
 */
public record ObjectStoreSnapshotInfo(
        String name,
        Instant createdAt,
        long sizeBytes,
        String objectStoreUri) {}

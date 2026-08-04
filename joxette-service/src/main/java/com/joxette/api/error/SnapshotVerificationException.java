package com.joxette.api.error;

import org.springframework.http.HttpStatus;

import java.util.Map;

/**
 * Thrown when {@code CassetteLifecycleService.restoreSnapshot()} completes
 * {@code IMPORT DATABASE} / lake-table reload but the restored row counts do
 * not match the counts recorded in {@code snapshots.row_counts} at snapshot
 * creation time — evidence that the snapshot's backing Parquet file(s) were
 * corrupted, truncated, or otherwise modified after the snapshot was taken.
 */
public class SnapshotVerificationException extends JoxetteException {

    public SnapshotVerificationException(String detail) {
        super(HttpStatus.CONFLICT, ErrorTypes.SNAPSHOT_VERIFICATION_FAILED,
                "Snapshot Verification Failed", detail, ErrorCodes.SNAPSHOT_VERIFICATION_FAILED);
    }

    public static SnapshotVerificationException rowCountMismatch(
            String snapshotName, Map<String, Long> expected, Map<String, Long> actual) {
        StringBuilder detail = new StringBuilder(
                "Snapshot '" + snapshotName + "' restored but row counts do not match stored metadata: ");
        expected.forEach((table, expectedCount) -> {
            if (actual.containsKey(table)) {
                detail.append(table).append(" expected=").append(expectedCount)
                      .append(" actual=").append(actual.get(table)).append("; ");
            }
        });
        return new SnapshotVerificationException(detail.toString());
    }
}

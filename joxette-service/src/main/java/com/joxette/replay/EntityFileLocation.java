package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

/** Physical file-location info for a single entity's cassette data. */
@Schema(description = "Physical file-location info for a single entity's cassette data. " +
                       "Split out of /stats since this can be slower to compute.",
        example = """
            {
              "fileCount": 3,
              "filesUnavailable": 0,
              "objectStoreDirectory": "s3://joxette-data/main/entity_customer/",
              "storageConsoleUrl": "http://localhost:9001/rustfs/console/browser/?bucket=joxette-data&key=main%2Fentity_customer%2F"
            }""")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EntityFileLocation(
        @Schema(description = "Exact number of physical Parquet files that contain at least one row " +
                               "for this entity, counted from the files that could actually be read. " +
                               "0 if the entity's data is still fully inlined in the catalog (not yet " +
                               "flushed), if object storage isn't configured, or if every candidate " +
                               "file was unreachable (see filesUnavailable).",
                example = "3")
        int fileCount,

        @Schema(description = "Number of candidate Parquet files that DuckLake's catalog says should " +
                               "contain this entity's rows but that could not be read when this count " +
                               "was computed (transient object-store I/O failure on that specific file). " +
                               "fileCount only reflects files that were actually readable — this field " +
                               "is what makes a non-zero fileCount possibly an undercount rather than " +
                               "the exact truth. 0 when every candidate file was reachable.",
                example = "0")
        int filesUnavailable,

        @Schema(description = "Object-store directory this entity type's Parquet files live in " +
                               "(shared with every other entity of the same type — storage is not " +
                               "physically partitioned per entity). Null if object storage isn't configured.",
                example = "s3://joxette-data/main/entity_customer/")
        String objectStoreDirectory,

        @Schema(description = "Deep link into a storage console's file browser for objectStoreDirectory, " +
                               "if joxette.storage-console.url-template is configured. Null otherwise.",
                example = "http://localhost:9001/rustfs/console/browser/?bucket=joxette-data&key=main%2Fentity_customer%2F")
        String storageConsoleUrl
) {}

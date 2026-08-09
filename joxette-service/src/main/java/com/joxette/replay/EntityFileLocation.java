package com.joxette.replay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;

/** Physical file-location info for a single entity's cassette data. */
@Schema(description = "Physical file-location info for a single entity's cassette data. " +
                       "Split out of /stats since this can be slower to compute.",
        example = """
            {
              "fileCount": 3,
              "objectStoreDirectory": "s3://joxette-data/main/entity_customer/",
              "storageConsoleUrl": "http://localhost:9001/rustfs/console/browser/?bucket=joxette-data&key=main%2Fentity_customer%2F"
            }""")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EntityFileLocation(
        @Schema(description = "Exact number of physical Parquet files that contain at least one row " +
                               "for this entity. 0 if the entity's data is still fully inlined in the " +
                               "catalog (not yet flushed) or if object storage isn't configured.",
                example = "3")
        int fileCount,

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

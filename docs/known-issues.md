# Known Issues

Historical record of a batch of pre-existing defects discovered while
hardening other parts of the system (surfaced when Failsafe was first wired
up for `mvn verify` — see git history around commit `547a224`). None were
introduced by the work that found them; each was independently verified
against a pre-change baseline before being logged here.

**Status: resolved.** All 10 originally-tracked test methods across the 5 IT
classes below were investigated and fixed. Nothing is currently excluded
from `mvn verify` — Failsafe runs the full integration-test suite with no
excludes and no `@Disabled` methods anywhere in the repo. This file is kept
as a historical stub because production code still points here (see
`CassetteLifecycleService.rebuildKnownEntities()`'s javadoc) for the
incident that motivated the current `recoverOrphanedFiles` design.

## Resolved

### `RebuildKnownEntitiesIT` — production bug (2 methods)

`rebuildKnownEntities_emptyEntityTables_returns0AndLeavesRegistryEmpty` and
`rebuildKnownEntities_idempotent_secondCallProducesSameResult` were
originally suspected to be a test-isolation issue. Investigation found a
real production data-integrity bug instead:
`resolveEntityDataSource()` fell back to an unconditional, bucket-wide
`**/*.parquet` glob whenever a table's live row count was zero — regardless
of *why* it was zero. That glob (a) matched every entity type's Parquet
files under the object-storage root, not just the one being rebuilt,
mislabeling rows with the wrong `entity_type`, and (b) could resurrect rows
that were already logically `DELETE`d (GDPR erase, truncate), since DuckLake
`DELETE` only marks rows deleted until compaction/vacuum removes the
underlying file. Fixed by scoping the fallback per-table
(`ducklake_list_files('lake', tableName)` plus a table-scoped glob) and
making it opt-in via `recoverOrphanedFiles` (default `false`). See
`resolveEntityDataSource()`'s javadoc for the full contract and residual
risk, and `RebuildKnownEntitiesIT` for the regression tests that guard
against both failure modes reappearing.

### `RecordReplayRoundTripIT.kafkaRecording_messagesAppearInPerTopicCassetteTable` — production bug

Timed out waiting for a dynamically-registered topic's cassette table to
receive rows. Root cause: `SchemaManager.createLakeTables()` only created
`lake.main.general_{topic}` tables for topics present in
`joxette.bootstrap.topics` at startup — a topic registered later via
`POST /topics` never got its table created, so recording crash-looped for
it. Fixed by having topic registration create its general cassette table
on demand (`SchemaManager.createGeneralTable()`), with a follow-up fix for
case-insensitive `TopicMode` resolution.

### `EntityReplayRoundTripIT.entityRecording_fullRoundTrip_recordsAppearInAllReplayEndpoints` — production bug

Timed out waiting for entity-routed records to appear via replay, sharing
its root cause with `RecordReplayRoundTripIT` above: dynamically-registered
topics never got their cassette table created. Resolved by the same fix.

### `InstanceRegistryIT` — test-only bug (2 methods)

`getInstancesIncludesRolesAndCatalogBackend` and
`staleInstancesAreReapedWhenReapIsCalled` asserted against a `roles` column
that no longer existed on `joxette_instances` — it was migrated away to
`recording_enabled`/`compaction_enabled` boolean columns
(`SchemaManager.migrateJoxetteInstances`). The tests predated that migration
and were never updated. Fixed by rewriting both against the current schema.

### `HeadersRoundTripIT` — test-only bug (4 methods)

All four header round-trip scenarios failed with an NPE in `writeRecord`,
caused by the test passing `List.of((String) null)` as the `messageTypes`
batch parameter (`List.of` rejects `null` elements). Fixing the NPE
surfaced a second, previously-masked assertion mismatch: replay responses
always carry provenance headers (`x-replay-id`, `x-original-*`,
`x-replayed-at`) injected by the metadata-only transform pipeline, which the
tests weren't accounting for. Fixed by switching to
`Collections.singletonList(null)` and stripping the well-known provenance
headers before asserting on test-written headers.

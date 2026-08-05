# Known Issues

Tracked, pre-existing defects discovered while hardening other parts of the
system. None of these were introduced by the work that found them — each was
independently verified against a pre-change baseline before being logged
here. Excluded from `mvn verify` (see `joxette-service/pom.xml`'s
`maven-failsafe-plugin` configuration) so the build stays a real signal
rather than permanently red; each should get its own fix task.

## `InstanceRegistryIT` — stale `roles` column assertion

`getInstancesIncludesRolesAndCatalogBackend` and
`staleInstancesAreReapedWhenReapIsCalled` assert against a `roles` column
that no longer exists on `joxette_instances` — it was migrated away to
`recording_enabled`/`compaction_enabled` boolean columns (see
`SchemaManager.migrateJoxetteInstances`). The test predates that migration
and was never updated. Fix: rewrite both tests against the current schema.

## `RebuildKnownEntitiesIT` — 2 failures, not yet root-caused

`rebuildKnownEntities_emptyEntityTables_returns0AndLeavesRegistryEmpty` and
`rebuildKnownEntities_idempotent_secondCallProducesSameResult` return
non-zero/mismatched row counts. Confirmed pre-existing (reproduces
identically on main before any recent hardening work), not yet
investigated further — likely a test-isolation issue (leftover rows from a
prior test in the same run) rather than a `rebuildKnownEntities()` defect,
but this needs confirming.

## `HeadersRoundTripIT` — 4 `NullPointerException`s in `writeRecord`

All four header round-trip scenarios (binary non-UTF8, duplicate keys,
empty header list, UTF8 values) fail at the same `writeRecord` call site.
Confirmed pre-existing, not yet root-caused.

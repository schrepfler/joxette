# Known Issues

Tracked, pre-existing defects discovered while hardening other parts of the
system. None of these were introduced by the work that found them — each was
independently verified against a pre-change baseline before being logged
here. Excluded from `mvn verify` (see `joxette-service/pom.xml`'s
`maven-failsafe-plugin` configuration) so the build stays a real signal
rather than permanently red; each should get its own fix task.

No known issues currently tracked. All previously-listed items
(`InstanceRegistryIT`'s stale `roles`-column assertions,
`HeadersRoundTripIT`'s `List.of(null)` NPE) have been fixed and their
tests re-enabled — see git history.

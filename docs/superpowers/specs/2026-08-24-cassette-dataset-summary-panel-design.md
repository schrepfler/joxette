# Cassette Dataset Summary Panel

## Motivation

Inspired by [Hamilton Ulmer's customer-dashboards-on-R2 post](https://www.hamiltonulmer.com/customer-dashboards-r2-hyparquet/):
a leaderboard-style breakdown of a dataset's dominant dimensions, sitting
next to the main view, that you can click to narrow what you're looking at.

Today, `topics/$topic.tsx` and `entities/$entityType/$entityId.tsx` give you
records (table), a timeline (canvas), a barcode view, and SOL/sequence
tooling — but no way to see, at a glance, "what's actually in this cassette
right now" (which partitions dominate, which message types are present,
which source topics contributed). You only discover that by scrolling.

The goal: a persistent side panel, visible across every tab, that answers
that question for the currently-filtered time window, and lets a click on
one of its rows narrow the view.

## Scope

**In scope:**
- New backend summary endpoints for topic and entity cassettes.
- New `DatasetSummaryPanel` component, wired into both cassette route pages.
- Click-to-select/clear interaction on leaderboard rows, wired into
  existing filters where they exist, client-side highlight where they don't.

**Out of scope:**
- Header-key/value cardinality (headers is a `LIST(STRUCT)` column with
  unbounded keys — a different aggregation problem; not needed for v1).
- New server-side filters for topic `message_type` or entity `source_topic`
  (no such filter exists today on the replay endpoints — see Interaction
  section; adding one is explicitly deferred, not part of this design).
- Any change to the existing `/stats` / `/storage` endpoints — those answer
  a different question (file counts, byte sizes) and are left untouched.

## Backend

### New endpoints

```
GET /cassettes/topics/{topic}/summary?from=&to=
GET /cassettes/entities/{entityType}/{entityId}/summary?from=&to=
```

Response shape (topic):
```json
{
  "totalRecords": 128456,
  "from": "2026-08-24T00:00:00Z",
  "to": "2026-08-24T12:00:00Z",
  "dimensions": {
    "partition": [
      {"value": "3", "count": 40213},
      {"value": "1", "count": 38010},
      {"value": null, "count": 0}
    ],
    "messageType": [
      {"value": "OrderCreated", "count": 88012},
      {"value": null, "count": 1200}
    ]
  }
}
```
Entity variant uses `sourceTopic` in place of `partition`; `messageType` is
the same shape. `from`/`to` are optional, same semantics as the existing
replay endpoints (open-ended when omitted).

New DTOs: `CassetteSummary`, `DimensionBreakdown` (`List<ValueCount>`),
`ValueCount(String value, long count)` — plain records alongside the
existing `CassetteStats`/`EntityStats` DTOs in `com.joxette.replay`.

### Query strategy

**Corrected during implementation research** (`GROUPING SETS` has zero
precedent in this codebase — `grep -rn "groupingSets\|GROUPING SETS"` across
`src/main` returns nothing, and jOOQ 3.21.7's DuckDB-dialect support for it
is unverified): two independent, simple `GROUP BY` queries per cassette
(one per dimension), each scoped by the same `from`/`to` `Condition` the
existing replay queries already build inline, run back-to-back inside one
`synchronized(duckDB)` block. This directly mirrors the existing aggregate
query in `EntityReplayService.getEntityStats()` (`GROUP BY topic ORDER BY
topic`) — same fluent `.select(...).from(...).where(...).groupBy(...)
.orderBy(...).limit(...)` shape used throughout `query()`. Same response
shape and architecture as originally sketched; safer query construction.

```java
var rows = dsl.select(F_PARTITION, DSL.count())
    .from(table)
    .where(cond)
    .groupBy(F_PARTITION)
    .orderBy(DSL.count().desc())
    .limit(TOP_N + 1)   // +1 lets Java detect "more than TOP_N remain" for the "other" bucket
    .fetch();
```

Each dimension list is capped at the top 20 values by count; the 21st+ rows
(if `limit(21)` returns a 21st row) are folded into a single
`{"value": "__other__", "count": remainder}` entry, computed as
`totalRecords - sum(top 20 counts)` so a 485-distinct-value field (e.g.
`message_type`) doesn't blow up the response.

Executed on the shared connection under `synchronized(duckDB)`, exactly
like every other read path (CLAUDE.md threading model) — no new locking
pattern.

### Service + controller

**Corrected during implementation research:** the summary method belongs
on `TopicReplayService` (topic) and `EntityReplayService` (entity) — *not*
a new shared class, and *not* `CassetteLifecycleService` (which owns the
existing `/stats`/`/storage` endpoints, but those answer a storage-metadata
question; this is a content `GROUP BY` query, which is what
`TopicReplayService`/`EntityReplayService` already do for every other read).
Each service gets its own summary method next to its existing `F_*` static
`Field` constants and table helper (`tableFor(topic)` /
`entityTable(entityType)`) — no cross-service sharing needed, the two
dimension sets differ (`partition`+`messageType` vs `sourceTopic`+
`messageType`) and the query shape is identical either way.

`CassetteController` gains two `@GetMapping` handlers
(`/topics/{topic}/summary`, `/entities/{entityType}/{entityId}/summary`),
thin delegating bodies (`throws SQLException`) following the existing
`getTopicStats`/`getEntityStats` OpenAPI-annotation style (operationId,
`@ApiResponse` with example JSON) — `from`/`to` as `@RequestParam(required
= false) Instant`, same as the topic replay GET handler (Spring parses
ISO-8601 directly, no manual parsing).

**Corrected error handling:** neither `/stats` endpoint today performs a
not-found check on an unknown topic/entity — a missing DuckDB table simply
raises `SQLException`, which `GlobalExceptionHandler` already maps to a
**503 "Database error"** (`UPSTREAM_UNAVAILABLE`), not 404. The new
endpoints follow this exact existing precedent: no explicit existence
check, `@ApiResponses` documents `200`/`500` only (matching
`getTopicStats`), no new exception type. The entity summary endpoint does
call the existing `validateEntityType(entityType)` first (same as
`getEntityStats`), which throws `ValidationException.field(...)` → 400 for
a malformed (not merely unknown) entity type — that check is about input
shape, not existence, and is unchanged from today's behavior.

## Frontend

### `DatasetSummaryPanel`

New file `ui/src/components/DatasetSummaryPanel.tsx`. Props:

```ts
interface Props {
  kind: 'topic' | 'entity'
  topic?: string                    // kind === 'topic'
  entityType?: string; entityId?: string  // kind === 'entity'
  from?: string; to?: string         // shared with the route's existing filter state
  selected: { dimension: string; value: string | null } | null
  onSelect: (dimension: string, value: string | null) => void
}
```

Fetches via TanStack Query, key `['cassettes', kind, id, 'summary', {from, to}]`,
`staleTime` similar to existing stats queries. Refetches only when `from`/`to`
change — not on tab switches, not on record-table pagination.

**Corrected during implementation research:** `DESIGN.md`'s described
palette (warm paper, oxblood accent, Fraunces/Figtree/JetBrains Mono) does
not match what `ui/src/design/tokens.css` actually defines today — the live
tokens are a blue-accent, Montserrat/DM Sans/DM Mono system (`--accent:
#166DF8` light / `#60a5fa` dark, `--surface-raised`, `--ink-secondary`,
`--rule`/`--rule-strong` are the real, currently-defined custom
properties). `DESIGN.md` is stale; the panel styles against the real
tokens above, not the doc.

**Corrected during implementation research:** the "Timeline" tab in both
`topics/$topic.tsx` and `entities/$entityType/$entityId.tsx` is a link-out
card to a *separate* route (`topics/$topic_.timeline.tsx`,
`entities/$entityType/$entityId_.timeline.tsx`) — `CassetteTimeline` is
never mounted on the detail page itself. So "persistent across every tab"
requires the panel on **four** route files, not two: the two detail routes
(covering Records / SOL / Barcode / Sequence) and the two dedicated
timeline routes (covering Timeline). Each of the four renders its own
`<DatasetSummaryPanel>` instance with the same `kind`/id/`from`/`to` props;
TanStack Query's cache (same query key) means only one network request
fires even though the component mounts separately per route.

Each dimension renders as a small ranked list: label + `<Tabular>` count
(`ui/src/design/primitives/Tabular.tsx`), `var(--accent)` on the selected
row, a quiet skeleton (not `LoadingSpinner`, which is sized for full-page
loading, not an inline panel) while loading. No bars, no gradients; this is
a ranked list of numbers, not a bar chart.

### Interaction wiring

Deliberately asymmetric, matching what each replay endpoint actually
supports today:

| Dimension | Cassette | Wiring |
|---|---|---|
| partition | topic | `setPartitionRaw(value)` in `topics/$topic.tsx` → real server refetch (existing filter, `topics/$topic.tsx:159`) |
| messageType | entity | new `messageTypes` field on `EntityRecordsParams` (see below) → real server refetch |
| messageType | topic | client-side highlight/dim of already-loaded records (no server filter exists) |
| sourceTopic | entity | client-side highlight/dim of already-loaded records (no server filter exists) |

**Corrected during implementation research:** the entity `message_types`
filter is backend-only today — `EntityRecordsParams` in `ui/src/api/client.ts`
(the entity records query params type) has no `messageTypes` field, and
`getEntityRecords` doesn't send one. Wiring the entity message-type
leaderboard to a "real" filter means *adding* `messageTypes?: string[]` to
`EntityRecordsParams`, passing it through `getEntityRecords`'s query-string
building, and adding the corresponding state in
`entities/$entityType/$entityId.tsx` (mirroring how `partitionRaw` works in
the topic route) — not just reusing something that already exists.

Client-side highlight means: `CassetteTimeline` markers and records-table
rows that don't match the selected value are dimmed (lower opacity / muted
ink), using data already in memory — no network request. This only affects
what's currently loaded/paged in, which is a real limitation worth a small
inline note in the UI ("highlighting loaded records only") when the
selection is a client-side one, so it doesn't read as a full-dataset filter
it isn't.

Clicking a selected row again clears the selection (toggle). Only one
`{dimension, value}` selection is active at a time — selecting a new row
replaces the old selection rather than stacking, mirroring the blog's
click-to-filter/click-to-clear behavior.

### File organization

- `ui/src/components/DatasetSummaryPanel.tsx` — new.
- `ui/src/api/client.ts` — new `cassettesApi.getTopicSummary(topic, {from, to})`
  and `cassettesApi.getEntitySummary(entityType, entityId, {from, to})`,
  alongside the existing `getTopicStats`/`getEntityStats` methods; new
  `CassetteSummary`/`DimensionBreakdown`/`ValueCount` types; new
  `messageTypes?: string[]` field on `EntityRecordsParams`, threaded into
  `getEntityRecords`'s query-string building.
- `ui/src/routes/topics/$topic.tsx` — render `<DatasetSummaryPanel kind="topic" .../>`
  alongside the tab content; thread `selected`/`onSelect` into the existing
  `partitionRaw` state and into `CassetteTimeline`'s highlight prop (new,
  see below) / the records table's row styling.
- `ui/src/routes/topics/$topic_.timeline.tsx` — same `<DatasetSummaryPanel
  kind="topic" .../>`, alongside the page's `CassetteTimeline`, wired to
  its `highlightPredicate` prop.
- `ui/src/routes/entities/$entityType/$entityId.tsx` — same, `kind="entity"`;
  new `messageTypesRaw`/`setMessageTypesRaw`-style state (mirroring
  `partitionRaw`) feeding the new `EntityRecordsParams.messageTypes`.
- `ui/src/routes/entities/$entityType/$entityId_.timeline.tsx` — same
  `<DatasetSummaryPanel kind="entity" .../>`, wired to `CassetteTimeline`'s
  `highlightPredicate`.
- `CassetteTimeline.tsx` — gains an optional `highlightPredicate?: (record: TimelineRecord) => boolean`
  prop; when set, non-matching markers render at reduced opacity via the
  existing hex-alpha-suffix trick already used for marker fill color
  (`color + '99'` today for the default state; a lower-alpha suffix for
  dimmed markers). Additive, doesn't change existing behavior when the prop
  is omitted.

## Testing / verification

**Backend** (corrected during implementation research — service-level
tests against the existing lightweight embedded-DuckDB fixture, no
Testcontainers and no new controller test, matching how `/stats` itself is
tested today):
- New tests in `TopicReplayServiceTest.java`/`EntityReplayServiceTest.java`
  using the existing `DuckDBTestSupport` helpers (`newConnection()`,
  `createGeneralCassetteTable()`/`createEntityTable()`,
  `insertCassetteRow()`/`insertEntityRow()` — all already present, no new
  fixtures needed) — assert the two dimension lists come back correctly
  split and counted for known inserted rows.
- Top-N + `__other__` bucketing edge case (a dimension with >20 distinct
  values).
- `from`/`to` scoping — records outside the window excluded from counts.
- No dedicated controller-level test: the generic `SQLException` → 503
  mapping is already covered by existing `GlobalExceptionHandler` tests,
  and the new handlers are thin delegating bodies with no new branching
  logic to test at that layer.

**Frontend:**
- `DatasetSummaryPanel.test.tsx` (vitest + testing-library; `vi.mock('../api/client', ...)`
  + an inline `QueryClientProvider` wrap — the established per-file pattern,
  e.g. `topics/-AddMatcherModal.a11y.test.tsx`; no shared test-utils file
  exists in this codebase and one file doesn't justify adding one): renders
  dimension lists from a mocked query response; click selects, click again
  clears.
- Manual verification in the browser: partition/message-type/source-topic
  breakdowns look correct against real recorded data; panel stays visible
  and doesn't refetch when switching tabs; light and dark mode (existing
  CSS custom properties, no new theming expected).

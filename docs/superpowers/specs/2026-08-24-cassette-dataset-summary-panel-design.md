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

One jOOQ query per cassette, computing both dimension breakdowns in a
single table scan via `GROUP BY GROUPING SETS`, mirroring the technique
DuckDB documents (linked from the blog post) rather than issuing two
separate `GROUP BY` round trips:

```sql
SELECT kafka_partition, message_type, COUNT(*) AS cnt
FROM lake.main.general_{topic}
WHERE kafka_timestamp >= ? AND kafka_timestamp < ?   -- same from/to predicate
                                                       -- TopicReplayService
                                                       -- already builds
GROUP BY GROUPING SETS ((kafka_partition), (message_type))
```

jOOQ supports this via `DSL.groupingSets(...)` — the query is built with
jOOQ `DSLContext`, consistent with the existing pattern in
`TopicReplayService`/`EntityReplayService` (both already build `Condition`s
for `from`/`to` with jOOQ; this reuses that same condition-building code
rather than duplicating WHERE-clause logic).

The result set has `GROUPING SETS` marker rows (partition set: message_type
NULL and vice versa) — split into the two dimension lists in Java by
checking which grouping key is non-null, matching the two grouping sets
requested. Each dimension list is capped at the top 20 values by count
(`ORDER BY cnt DESC LIMIT 20` per grouping set, or truncated in Java after
one query — either is fine at implementation time); anything beyond that is
folded into a single `{"value": "__other__", "count": remainder}` entry so
a 485-distinct-value field doesn't blow up the response.

Executed on the shared connection under `synchronized(duckDB)`, exactly
like every other read path (`docs/write-resilience.md` / CLAUDE.md
threading model) — no new locking pattern.

### Service + controller

New method on `TopicReplayService`/`EntityReplayService` (or a small new
`CassetteSummaryService` if the query-building logic is substantial enough
to warrant its own class — decide at implementation time based on how much
shared logic exists between the topic and entity variants).

`CassetteController` gains two `@GetMapping` handlers
(`/topics/{topic}/summary`, `/entities/{entityType}/{entityId}/summary`),
following the existing OpenAPI-annotation style already used for
`/topics/{topic}/stats` (operationId, `@ApiResponse` with example JSON).
Errors: `ResourceNotFoundException` if the topic/entity is unknown, same as
existing endpoints — no new exception types needed.

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

Rendered once per route (`topics/$topic.tsx`, `entities/$entityType/$entityId.tsx`),
as a persistent right-hand rail alongside the existing tab content — sibling
to the tab panel, not inside it, so it stays mounted and visible across
Records / Timeline / Barcode / SOL / Sequence tabs.

Each dimension renders as a small ranked list: label + `<Tabular>` count
(design tokens from `ui/src/design/primitives`), oxblood `--accent` on the
selected row, quiet skeleton (not a spinner overlay) while loading —
consistent with `DESIGN.md`'s "instrument, not dashboard" tone. No bars,
no gradients; this is a ranked list of numbers, not a bar chart.

### Interaction wiring

Deliberately asymmetric, matching what each replay endpoint actually
supports today:

| Dimension | Cassette | Wiring |
|---|---|---|
| partition | topic | `setPartitionRaw(value)` in `topics/$topic.tsx` → real server refetch (existing filter, `topics/$topic.tsx:159`) |
| messageType | entity | sets existing `message_types` query param → real server refetch |
| messageType | topic | client-side highlight/dim of already-loaded records (no server filter exists) |
| sourceTopic | entity | client-side highlight/dim of already-loaded records (no server filter exists) |

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
  alongside the existing `getTopicStats`/`getEntityStats` methods.
- `ui/src/routes/topics/$topic.tsx` — render `<DatasetSummaryPanel kind="topic" .../>`
  alongside the tab content; thread `selected`/`onSelect` into the existing
  `partitionRaw` state and into `CassetteTimeline`'s highlight prop (new,
  see below) / the records table's row styling.
- `ui/src/routes/entities/$entityType/$entityId.tsx` — same, `kind="entity"`.
- `CassetteTimeline.tsx` — gains an optional `highlightPredicate?: (record: TimelineRecord) => boolean`
  prop; when set, non-matching markers render at reduced opacity. Additive,
  doesn't change existing behavior when the prop is omitted.

## Testing / verification

**Backend:**
- `CassetteSummaryServiceTest` (or wherever the query lands) — GROUPING SETS
  query construction against a Testcontainers DuckDB fixture; assert the
  two dimension lists come back correctly split and counted.
- Top-N + `__other__` bucketing edge case (a dimension with >20 distinct
  values).
- `from`/`to` scoping — records outside the window excluded from counts.
- Controller-level: `ResourceNotFoundException` → RFC7807 problem-detail
  for an unknown topic/entity, matching `CassetteControllerProblemDetailTest`
  patterns.

**Frontend:**
- `DatasetSummaryPanel` component test (vitest + testing-library, pattern
  matching `CrudModals.a11y.test.tsx`): renders dimension lists from a
  mocked query response; click selects/highlights; click again clears.
- `topics/$topic.tsx`: clicking a partition row updates `partitionRaw` and
  triggers the existing records refetch (can assert via the existing query
  mocking setup already in that route's tests, if any — otherwise a new
  light test).
- Manual verification in the browser: partition/message-type/source-topic
  breakdowns look correct against real recorded data; panel stays visible
  and doesn't refetch when switching tabs; light and dark mode (existing
  CSS custom properties, no new theming expected).

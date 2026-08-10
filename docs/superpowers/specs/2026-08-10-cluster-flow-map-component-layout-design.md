# Cluster Flow Map: Component-Driven Instance Panel Layout

## Motivation

`ui/src/components/ClusterFlowMap.tsx` renders the Cluster page's "Flow Map"
tab using React Flow (`@xyflow/react`). The instance panel (the box
containing recorder jobs, the DuckDB/DuckLake engine, and replay jobs) lays
out its internals via hand-computed pixel math in `buildGraph()`:

```ts
const ROW_H        = 110
const JOB_W        = 240
const CONT_PAD_X   = 20
const CONT_PAD_TOP = 64
const CONT_PAD_BOT = 16
const LAKE_W       = 180
const COL_GAP      = 24
```

Column x-offsets (`recColX`, `lakeColX`, `rplColX`) and row y-offsets
(`childY = CONT_PAD_TOP + i * ROW_H`) are computed in JS, and the
container's own `width`/`height` are derived from those same constants
(`CONT_W`, `CONT_H`) and passed to the node as fixed CSS. This behaves like
laying out shapes on a picture: sizes and distances are guessed constants
baked into JS, not values that fall out of real component/content sizing.
If a row's content changes height (e.g. an error message wraps), the layout
doesn't adapt — the guess just becomes wrong.

The goal: make the instance panel's internals genuinely component/object-like
— sized and spaced by real CSS layout (flexbox, `gap`) responding to actual
content, not precomputed pixel offsets.

## Scope

**In scope:** the instance container's internal layout (recorder rows, the
DuckDB/DuckLake engine box, replay rows) — columns, rows, dividers, sizing.

**Out of scope:** the freestanding diagram nodes (Kafka topic nodes, the
remote-DuckLake node, replay-target nodes, the object-storage node) and their
positioning relative to the instance panel. Those remain independently
x/y-positioned React Flow nodes — that's normal, expected diagram-node
behavior, not the problem being fixed here.

**Explicit non-goal:** pixel-perfect vertical alignment between a Kafka
topic node and "its" recorder row. Today this alignment is achieved because
both use the same `childY` formula. Once recorder rows free-flow inside the
container, Kafka nodes will use simple constant-gap stacking, and edges will
bend (via the existing bezier path) to meet wherever the row actually lands.
This is a deliberate, accepted trade-off, not an oversight — true DOM-measured
alignment can be a future enhancement if it turns out to matter visually.

## Architecture

The instance panel becomes a single, self-sizing React Flow node
(`InstanceContainerNode`) whose internals are ordinary React children in a
CSS flex layout:

- Three columns (Recording | Catalog | Replay) laid out as a CSS flex row.
  Only present columns render (same `hasRecorders`/`hasCatalog`/`hasReplays`
  conditionals as today).
- Each column is a flex column of row cards with CSS `gap` for spacing —
  no `ROW_H` stepping.
- Column dividers become plain `borderLeft` on the adjacent column; column
  labels become normal child text — no computed-X absolute-positioned divs.
- The node itself gets **no fixed `width`/`height`** in its React Flow
  `style`. React Flow already auto-measures nodes that don't declare
  explicit dimensions (via its internal `ResizeObserver`, populated into
  `node.measured.width/height`) — the container just grows to fit its
  content, and re-measures automatically if content changes.

## Components

New row components (plain functions returning JSX — **not** registered
React Flow node types):

- `RecorderRow({ topic, rec, rates })` — one card per recorder, with a
  target `<Handle id={"recorder-in-" + topic}>` on its left and a source
  `<Handle id={"recorder-out-" + topic}>` on its right. Handles are nested
  inside the row's own DOM, so their visual position tracks the row
  automatically — this is the concrete mechanism that replaces `childY`.
- `DuckLakeRow({ totalWritten })` — one card, target `<Handle id="lake-in">`
  (shared — multiple recorder edges may target the same handle id, which
  React Flow supports natively), source `<Handle id="lake-replay-out">`
  (right), source `<Handle id="lake-tier-out">` (bottom, for the object
  storage tier edge).
- `ReplayRow({ replay })` — one card per active replay, target
  `<Handle id={"replay-in-" + replay.id}>` (left), source
  `<Handle id={"replay-out-" + replay.id}>` (right).

`InstanceContainerNode` renders the three columns, each populated with the
row components above, reading recorder/replay/duckdb data straight from
`data` (same shape of data `buildGraph` already assembles today — recorder
entries, active replays, total written).

`RecordJobNode`, `ReplayJobNode`, `DuckDbEngineNode` as standalone React
Flow node types are **not carried over** to the new implementation — their
rendering logic is absorbed into the row components above.
`KafkaTopicNode`, `DuckLakeNode` (remote-catalog case), `ObjectStorageNode`,
`ReplayTargetNode` are reused as-is (imported, unmodified).

## Data flow / edges

Same edge-id scheme as today (`e-kafka-rec-{topic}`, `e-rec-lake-{topic}`,
`e-lake-objstore`, `e-lake-replay-{id}`, `e-replay-target-{id}`,
`e-replay-target-topic-{topic}`) so `useParticleSpawner` and `ParticleEdge`
keep working completely unmodified — they only care about edge ids, not
which node/handle an edge is attached to.

What changes is `source`/`target`/`sourceHandle`/`targetHandle` on each
edge:

| Edge | Before | After |
|---|---|---|
| kafka → recorder | `kafka-{topic}` → `recorder-{topic}` | `kafka-{topic}` → `instance` (handle `recorder-in-{topic}`) |
| recorder → lake | `recorder-{topic}` → `sink-ducklake` | `instance` (handle `recorder-out-{topic}`) → `instance` (handle `lake-in`) |
| lake → object store | `sink-ducklake` (handle `bottom`) → `obj-store` | `instance` (handle `lake-tier-out`) → `obj-store` |
| lake → replay | `sink-ducklake` (handle `right`) → `replay-job-{id}` | `instance` (handle `lake-replay-out`) → `instance` (handle `replay-in-{id}`) |
| replay → target topic | `replay-job-{id}` → `replay-target-topic-{topic}` | `instance` (handle `replay-out-{id}`) → `replay-target-topic-{topic}` |

Same-node edges (source and target both `'instance'`, distinct handles) are
a standard, supported React Flow pattern (used for e.g. self-referencing
schema diagrams) — no special-casing needed in `ParticleEdge` itself, since
it already just resolves `sourceX/sourceY/targetX/targetY` from whatever
handles React Flow reports.

The remote-catalog case (`!hasCatalog`) is unchanged: `DuckLakeNode` remains
an external node, and recorder rows' `lake-in`-equivalent edges continue to
target it directly (no `DuckLakeRow` is rendered inside the container in
that case, matching today's `if (hasCatalog) { ... } else { ... }` branch).

## File organization

Per "don't delete the old implementation, just switch":

- `ui/src/components/ClusterFlowMap.tsx` — **untouched**, stays fully
  intact and would still work if directly imported. No longer referenced
  by any route once this ships.
- `ui/src/components/ClusterFlowMapObjects.tsx` — **new file**, contains
  the new `buildGraph`, `InstanceContainerNode`, `RecorderRow`,
  `DuckLakeRow`, `ReplayRow`, and the new public `ClusterFlowMapObjects`
  export. Imports and reuses the layout-agnostic infrastructure from
  `ClusterFlowMap.tsx` (`makeParticleStore`, `ParticleContext`/
  `useParticleStore`, `useLiveMetrics`, `useParticleSpawner`,
  `useAnimationLoop`, `ParticleEdge`, `useRates`, `KafkaTopicNode`,
  `DuckLakeNode`, `ObjectStorageNode`, `ReplayTargetNode`, `Stat`, `fmt`,
  and the shared style constants) rather than duplicating them — those
  exports need to become exported from `ClusterFlowMap.tsx` where they
  aren't already.
- `ui/src/routes/cluster/index.tsx` — import switched from
  `ClusterFlowMap` to `ClusterFlowMapObjects`.

## Technical assumptions verified

Checked against the React Flow (`@xyflow/react` v12 / xyflow.com) docs before
committing to this design:

- A node without an explicit `width`/`height` (no fixed size in `style` or
  on the node object itself) is measured after render and its real
  rendered size is stored in `node.measured.width`/`node.measured.height`
  — confirmed via the v12 migration notes ("these attributes now directly
  set inline styles... not dynamic based on content" implies the inverse:
  omitting them keeps sizing dynamic/measured) and `useNodesInitialized()`'s
  documented behavior of waiting for nodes to be "measured and given a
  width and height."
- Multiple handles per node, each with a unique `id`, with edges targeting
  a specific handle via `sourceHandle`/`targetHandle`, is an explicitly
  documented, first-class pattern (`learn/customization/handles`). Nothing
  in the docs restricts an edge's `source` and `target` from being the same
  node id as long as the handles differ — this is standard graph-edge
  modeling, not an xyflow-specific limitation.

## Testing / verification

No backend or business-logic changes — this is a visual/layout rewrite of
an existing live view. No new unit tests. Verification is by running the
dev server and checking the Flow Map tab in browser:

- Container auto-sizes correctly with recorders present.
- Particle animation still tracks edges correctly (kafka→recorder,
  recorder→lake, lake→object-store tier, lake→replay, replay→target).
- Conditional column rendering: zero recorders, zero replays, remote
  catalog (`hasCatalog === false`) — each hides its column/uses the
  external-node fallback as today.
- Light and dark mode (existing CSS custom properties, should need no
  new theming work).
- Visual comparison against the current `ClusterFlowMap` (reachable by
  temporarily swapping the import back, or by eye against a
  before/after screenshot) to confirm no functional regression.

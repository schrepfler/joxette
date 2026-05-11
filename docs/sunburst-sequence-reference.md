# Sunburst Sequence Chart — Reference

> Source: https://observablehq.com/@mikpanko/nfl-plays-sunburst-chart
> and its dependency https://observablehq.com/@mikpanko/sequence-pattern-matching
> Extracted May 2026. Purpose: inform a Joxette sunburst chart for visualising
> entity cassette sequences — e.g. all drives / sessions / entity histories
> aggregated into a single radial frequency chart.

---

## 1. What the Sunburst Chart Shows

Each entity sequence (NFL drive = Joxette entity history) contributes to a
**radial tree** where:

- The **centre** is the start of every sequence.
- Each **concentric ring** is one step further into the sequence.
- Each **arc** in a ring represents one event type at that position.
- The **angle span** of an arc is proportional to the number of sequences
  that have that exact prefix path to reach this arc.
- Traversing from centre outward reads the sequence left-to-right.

```
                         ┌──────────────────────────────┐
                         │   watch_heartbeat (wide)      │
              ┌──────────┤──────────────────────────────┤
              │ watch_st │   watch_start (medium)        │
    ┌─────────┤──────────┤──────────────────────────────┤──────────────┐
    │ movie_p │ search_p │   search_results (narrow)    │ movie_page   │
────┤─────────┤──────────┼──────────────────────────────┤──────────────┤
    │ home_p  │ home_p   │   home_page (medium)         │ home_page    │
    └─────────┤──────────┤──────────────────────────────┤──────────────┘
              │          │   ...                         │
              └──────────┴──────────────────────────────┘
                ring 1       ring 2                        ring N
               (step 1)     (step 2)                    (step N)
                         centre = START
```

In polar coordinates:

```
                  ╭───── watch_heartbeat ──────╮
              ╭── watch_start ──╮    ╭── logout ──╮
         ╭─── movie_page ───╮  │    │  ╭── search ──╮
    ╭─── home_page ───╮    │  │    │  │  ╭─── ...
    │       │         │    │  │    │  │  │
    │   ring 1    ring 2  ring 3  ring 4 ring 5
    │                          │
    │         ◉ START           │
    │     (centre circle)       │
    ╰───────────────────────────╯
```

---

## 2. Data Model

Input: an array of sequences. Each sequence is an object with:

```javascript
{
  // sequence-level properties (any key/value)
  pos_team: "NE",
  quarter:  4,
  // required: ordered list of events
  events: [
    { _eventName: "run",  yards_gained: 3, down: 1 },
    { _eventName: "pass", yards_gained: 12, down: 2 },
    { _eventName: "touchdown" }
  ]
}
```

### Joxette mapping

| Sequence data model | Joxette equivalent |
|---|---|
| Sequence | All `EntityRecord`s for one `entity_id`, ordered by `(timestamp, recorded_at)` |
| `events[]` | The `EntityRecord` list |
| `_eventName` | `messageType` (or `topic` as fallback) |
| Sequence properties | `entity_id`, `entity_type`, any top-level fields from the first event |
| Event properties | All fields in the `value` JSON, `partition`, `offset`, `timestamp` |

---

## 3. Hierarchy Construction

The `dataHierarchy` function builds a tree from the flat sequence array by
walking each sequence's events left-to-right:

```
rootNode {name:"start", node_id:1, node_count:N, children:[]}
  └─ {name:"home_page", node_count:K, children:[]}
       ├─ {name:"search_page", node_count:J, children:[...]}
       │    └─ {name:"movie_page", node_count:I, children:[...]}
       └─ {name:"movie_page", node_count:M, children:[...]}
            └─ {name:"watch_start", ...}
```

Each node tracks:
- `node_id` — stable integer, used to match SVG elements on re-render
- `node_count` — number of sequences passing through this node
- `target_count` — sequences on "happy" (target) path (A/B marking)
- `prop_values[]` — sampled property values for the selected colour dimension
- `seq_ids[]` (leaf nodes only) — which sequences end here

D3 `partition` layout then assigns:
- `x0`, `x1` — angular span in radians `[0, 2π]`
- `y0`, `y1` — radial depth band `[0, numSteps+1]`

---

## 4. Arc Geometry

```javascript
// innerRadius and outerRadius use sqrt to make area proportional to count
// (equal-area rings, not equal-width rings)
d3.arc()
  .startAngle(d => d.x0)
  .endAngle(d => d.x1)
  .padAngle(2 / diameter)
  .padRadius(diameter / 2)
  .innerRadius(d => diameter / 2 * Math.sqrt(d.y0 / (numSteps + 1)))
  .outerRadius(d => diameter / 2 * Math.sqrt(d.y1 / (numSteps + 1)) - 1)
```

An arc is **visible** when:

```javascript
function arcVisible(d) {
  return (d.y1 >= 1)                                   // not the root
      && (d.y1 <= numSteps + 1)                        // within depth limit
      && ((d.x1 - d.x0) >= minPlotAngle / 360 * 2π);  // above min angle threshold
}
```

`minPlotAngle` defaults to ~0.5°, configurable by the user. This hides arcs
representing very rare sequences.

---

## 5. Colour Modes

### 5.1 By event name (default)
Each distinct `_eventName` maps to a colour from `d3.schemeTableau10`. The
mapping is stable (same event = same colour always). Named events can have
overrides via a `defaultColors` map.

### 5.2 By target sequence probability
After dragging arcs to mark "happy" sequences, arcs are coloured by:
```
P(happy | this prefix path) = node.target_count / node.node_count
```
Uses `d3.scaleSqrt([0,1], [Blues(0.2), Blues(1)])` — light→dark blue.

### 5.3 By probability change
`P(happy at step N) - P(happy at step N-1)` — red=decreases, green=increases.
Uses a diverging scale centred at 0.

### 5.4 By sequence/event property (numeric)
Median property value across all sequences through each arc.
`d3.scaleSequential(range, d3.interpolateOranges)` — light→dark orange.

### 5.5 By sequence/event property (categorical)
Frequency of the most common property value.
`d3.scaleSqrt(range, d3.interpolatePurples)` — light→dark purple.

---

## 6. Interactions

### 6.1 Hover — breadcrumb path
Hovering an arc lights up the path from root to that arc and shows a
**breadcrumb trail** to the right of the chart:

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────────────┐
│  START   │ ►  │ home_page│ ►  │search_pa │ ►  │  movie_page          │
│          │    │          │    │          │    │  60.6k   100%        │
└──────────┘    └──────────┘    └──────────┘    └──────────────────────┘
```

Each breadcrumb is a pentagon (arrow shape pointing right). The final crumb
shows count and percentage of total. The breadcrumb arrows are also clickable
to zoom out to that level.

Centre circle shows:
```
  60.6k         ← count of sequences through hovered arc
   22%           ← percentage of all sequences
```

### 6.2 Click — select path
Single-clicking an arc **selects** it (highlights it), keeping the path lit
even without hover. Click the centre or an empty area to deselect.

### 6.3 Double-click — zoom in
Double-clicking an arc **zooms** the chart so that arc spans the full
360° — equivalent to saying "show only sequences that went through this step".

```
Before zoom:                    After zoom on "search_page":
     ╭──────────╮                    ╭──────────────────────────╮
  ╭─ home_page ─╮                ╭── search_results ────────────╮
  │  search_page│                │   search_click               │
  │  favorites  │                │   movie_page                 │
  ╰─────────────╯                │   search_page (again)        │
                                 ╰──────────────────────────────╯
                                  (only paths through search_page shown)
```

The zoomed-in chart re-normalises arc angles to fill 360°.
Double-click the centre to zoom out one level.

### 6.4 Right-click / long-click — property distribution
Triggers a **property distribution panel** below the chart, showing:
- For numeric properties: a histogram of values across all sequences through
  that arc
- For categorical properties: a bar chart of value frequencies
- The arc gets a striped fill to indicate it's the selected one

```
Distribution of "yards_gained" for arc [search_page → movie_page]:
  ─────────────────────────────────────────────────────
  0–5    ████████████████████  42%
  6–10   ██████████            21%
  11–20  ████████              17%
  21+    █████████████         20%
```

### 6.5 Drag to "target sequences" — A/B probability
Dragging an arc to the **target sequences** drop zone on the left marks
sequences passing through that arc as "happy". Once happy sequences are
marked, the colour mode can switch to probability view to see which paths
lead to the target outcome and which diverge from it.

### 6.6 Drag to "start" in path view — rebasing percentages
Dragging an arc to the **start** box in the path view on the right sets the
denominator for all `%` calculations to that arc's count — useful for
conditional probability analysis ("of people who searched, what % watched?").

---

## 7. Sequence Query Widget (`rulesWidget`)

A form-based UI widget that builds a list of sequence filter/transform rules:

```
Rules to filter & modify sequences:      [Add Rule]  [Submit Rules]
────────────────────────────────────────────────────────────────────

Rule 1:
  Match pattern:  [  -(:pass)-  ______________________ ]
  [not]  [or]  [replace]                         [✕ remove]

Rule 2:
  Match pattern:  [  -(:touchdown)-  _________________ ]  ← AND (sequential)
  Replace with:   [  (:first_down_seq {play_attempt=@ev}) ]
  [not]  [or]     [remove replace]               [✕ remove]
```

Each **rule** has:
- A match pattern (string, see query language below)
- Optional `not` toggle — inverts the filter
- Optional `or` button — creates an OR branch below this rule
- Optional `replace` toggle — adds a replace pattern field

Rules are applied sequentially (AND semantics between rules).

---

## 8. Sequence Query Language

A custom DSL inspired by Cypher graph patterns and regex. Spaces are ignored.

### 8.1 Matching single events

| Pattern | Meaning |
|---|---|
| `()` | Any one event |
| `(:pass)` | Event named "pass" |
| `(:pass\|:run)` | Event named "pass" OR "run" |
| `(!:pass)` | Any event NOT named "pass" |
| `({yards_gained>=3})` | Event where `yards_gained ≥ 3` |
| `({play_result!=first_down\|second_down})` | Property not equal to either value |

Supported property operators: `==`, `=`, `:` (equal); `!=`; `>`; `<`; `>=`; `<=`

### 8.2 Quantifiers

| Pattern | Meaning |
|---|---|
| `[3](:pass)` | Exactly 3 "pass" events in a row |
| `[0..3](:pass)` | 0 to 3 "pass" events |
| `[2..](:pass)` | 2 or more "pass" events |
| `[..4](:pass)` | 0 to 4 "pass" events |
| `[..](:pass)` | Any number of "pass" events |
| `[..]()` | Any number of any events (wildcard) |

### 8.3 Sequencing events

| Pattern | Meaning |
|---|---|
| `(:pass)-(:run)` | "pass" immediately followed by "run" |
| `(:pass)-[..3]-(:td)` | "pass" then up to 3 events then "touchdown" |
| `\|-(:pass)` | "pass" at start of sequence |
| `(:touchdown)-\|` | "touchdown" at end of sequence |

### 8.4 Partial match controls

| Pattern | Meaning |
|---|---|
| `-(:pass)` | Sequence contains "pass" anywhere; match ends at it |
| `(:pass)-` | Match starts at "pass" but sequence may have events before |
| `-(:pass)-(^:run)-$` | Sequence has "pass" before the main match starts |
| `^-($:pass)-(:run)-` | Sequence has "run" immediately after match ends |

### 8.5 Sequence-level properties

```
-()- {{pos_team!=NE|SF, home_score>=10}}
```

Double curly braces at the end filter by sequence properties.

### 8.6 Matching parameters

```
(:pass) {{_whereToMatch=_FIRST}}   // only first match per sequence (default: _ALL)
(:pass) {{_allowOverlaps=true}}    // allow overlapping matches (default: false)
```

### 8.7 Cross-event references (tagging)

```
(@ev :pass)                        // tag matched event as "@ev"
(@ev)-(:@ev)                       // second event must have same name as first
(@ev)-({yards_gained=@ev|3})       // second event's yards = first event's yards OR 3
```

### 8.8 Replace patterns

In the `rulesWidget`, clicking "replace" adds a replace pattern:

| Replace pattern | Meaning |
|---|---|
| `(:new_name {prop=value})` | Replace with a single new event |
| `(ALL@ev)` | Copy all events tagged `@ev` |
| `(REVERSE@ev)` | Copy all `@ev` events in reverse order |
| `({@ev})` | Copy all properties from `@ev` |
| `-` | Remove matched sub-sequence |

Example — collapse first-down sequences into one event:
```
Match:   (@ev {down=1})-[..]({down!=1})
Replace: (:first_down_seq {play_attempt=@ev})
```

### 8.9 Logical combining

| Operator | Mechanism |
|---|---|
| AND | Add another rule — output of rule N feeds into rule N+1 |
| NOT | Toggle "not" button on a rule |
| OR | Click "or" to add a parallel branch; results are unioned |

### 8.10 Arbitrary JavaScript

Inject JS code between `#` symbols for complex conditions:

```
|-(@a)-[..]-(@b)-| {{#(@b[0].timestamp - @a[0].timestamp >= 600) && (seq.events.length >= 20)#}}
```

In replace patterns:
```
Match:   (@ev:pass)
Replace: ({@ev, #nEv._eventName = (@ev[0].pass_location) ? ('pass_'+@ev[0].pass_location) : 'pass'#})
```

### 8.11 Example queries (NFL)

```
// 4th quarter drives where NE was sacked
-({quarter=4, play_result=sack})- {{pos_team=NE}}

// three-and-out drives
|-[3]-(:punt)-|

// touchdown drives without a single run
|-[..](!:run)-(:touchdown)-|

// split drives into first-down sub-sequences
({down=1})-[..]({down!=1})

// drives lasting 10+ minutes with 20+ plays
|-(@a)-[..]-(@b)-| {{#(@b[0].timestamp - @a[0].timestamp >= 600) && (seq.events.length >= 20)#}}
```

---

## 9. SVG Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│  ┌──────────────────────────┐                   ┌───────────────────────┐  │
│  │   Target sequences       │    ╭──────────╮   │  Breadcrumb trail     │  │
│  │   drop zone              │  ╭─────────────╮  │  ─────────────────    │  │
│  │                          │  │  ╭───────╮  │  │  ► step 1             │  │
│  │   (drag arcs here to     │  │  │       │  │  │  ► step 2  76%        │  │
│  │    mark happy paths)     │  │  │  ◉    │  │  │  ► step 3  60.6k      │  │
│  │                          │  │  │       │  │  │                       │  │
│  └──────────────────────────┘  │  ╰───────╯  │  │  Percentage display   │  │
│                                │             │  │  ─────────────────    │  │
│  Total: N happy (%)            │  Sunburst   │  │  base: ┌─────────┐   │  │
│  Total: M sad   (%)            │  chart      │  │  start:│  drag   │   │  │
│                                ╰─────────────╯  │        │  here   │   │  │
│                                                  │        └─────────┘   │  │
│                                                  └───────────────────────┘  │
│                                                                             │
│  ─────────────────── Property distribution (right-click) ───────────────── │
│  [Vega-Lite bar/histogram chart appears here after right-clicking an arc]   │
└─────────────────────────────────────────────────────────────────────────────┘
```

SVG viewBox: `[0, 0, diameter + 100 + breadcrumbWidth + 100, diameter]`
- `diameter` = 600px
- `breadcrumbHeight` = 30px  
- `breadcrumbWidth` = 200px
- Sunburst `g` centred at `(diameter/2, diameter/2)`
- Breadcrumb `g` at `(diameter + 100, 20)`

---

## 10. Joxette Implementation Plan

### 10.1 Data preparation

```java
// SolEngine already produces a Sequence per entity.
// For the sunburst we need ALL entities of a type as sequences:
//   EntityReplayService.streamEntityEvents(type, id, ...) → per entity
// Aggregate: for each entity, load events, build Sequence,
// then pass all sequences to the sunburst builder.

// Event name: messageType ?? topic
// Sequence: ordered events for one entity_id
```

### 10.2 Hierarchy building (TypeScript)

```typescript
interface SunburstNode {
  name: string
  nodeId: number
  nodeCount: number
  seqIds: string[]          // leaf nodes only
  children: SunburstNode[]
}

function buildHierarchy(sequences: Sequence[]): SunburstNode {
  const root: SunburstNode = { name: 'start', nodeId: 1, nodeCount: 0,
                               seqIds: [], children: [] }
  let nextId = 2
  for (const seq of sequences) {
    root.nodeCount++
    let node = root
    for (const event of seq.events) {
      const name = event.messageType ?? event.topic
      let child = node.children.find(c => c.name === name)
      if (!child) {
        child = { name, nodeId: nextId++, nodeCount: 0, seqIds: [], children: [] }
        node.children.push(child)
      }
      child.nodeCount++
      node = child
    }
    node.seqIds.push(seq.entityId)
  }
  return root
}
```

### 10.3 Rendering (D3 + React)

Use `d3-hierarchy` partition layout + `d3-shape` arc generator:

```typescript
// Arc generator — sqrt radii for equal-area rings
const arc = d3.arc<d3.HierarchyRectangularNode<SunburstNode>>()
  .startAngle(d => d.x0)
  .endAngle(d => d.x1)
  .padAngle(2 / diameter)
  .padRadius(diameter / 2)
  .innerRadius(d => (diameter / 2) * Math.sqrt(d.y0 / (numSteps + 1)))
  .outerRadius(d => (diameter / 2) * Math.sqrt(d.y1 / (numSteps + 1)) - 1)

// Visibility filter
const arcVisible = (d: d3.HierarchyRectangularNode<SunburstNode>) =>
  d.y1 >= 1
  && d.y1 <= numSteps + 1
  && (d.x1 - d.x0) >= (minAngleDeg / 360) * 2 * Math.PI

// Colour — stable hash of event name → hue
const eventColour = (name: string) =>
  `hsl(${(djb2hash(name) * 137.5) % 360}, 60%, 65%)`
```

### 10.4 Zoom animation (D3 transition)

```typescript
// On double-click: tween arc coordinates from current to target
selection.transition().duration(750).attrTween('d', d => {
  const interpolate = d3.interpolate(d.current, d.target)
  return t => { d.current = interpolate(t); return arc(d.current) }
})
```

### 10.5 Breadcrumb trail

```typescript
// Pentagon chevron SVG polygon for each ancestor node
function breadcrumbPoints(i: number): string {
  const tip = 10
  return [
    `0,0`, `0,${bh}`, `${bw/2},${bh+tip}`, `${bw},${bh}`, `${bw},0`,
    ...(i > 0 ? [`${bw/2-tip},${bh/2}`] : [])  // arrow notch on left for non-first
  ].join(' ')
}
```

### 10.6 Backend API

New endpoint to serve aggregated hierarchy for a given entity type:

```
POST /cassettes/entities/{type}/sunburst
Body: { "from": "...", "to": "...", "maxSteps": 8, "minAngleDeg": 0.5, "solQuery": "..." }
Response: { "root": SunburstNode, "eventNames": string[], "totalSequences": number }
```

The `solQuery` field is optional — if supplied, the sequences are filtered/
transformed through the SOL engine before building the hierarchy.

### 10.7 SOL integration

The sequence query language from this notebook is closely related to SOL.
The key parallels:

| Observable query language | SOL equivalent |
|---|---|
| `(:pass)` | event name in match pattern |
| `(!:pass)` | `^pass` exclusion |
| `[..]()` | `*` wildcard |
| `\|-...-\|` | `start >> ... >> end` anchors |
| `(@ev)-(:@ev)` | cross-event reference (future SOL extension) |
| `{{_whereToMatch=_FIRST}}` | `match` (first only, default) vs `match split` (all) |
| `{{seq.pos_team=NE}}` | `if` condition on sequence dim |
| Match → Replace → Combine | `match split ... replace ... combine` |

The main difference: Observable's language uses `()` and `-` where SOL uses
`>>` and explicit tag naming. The semantics are equivalent.

---

## 11. Complete Example: Entity Cassette Sunburst

For an `order` entity type in Joxette:

```
Events: checkout_started → payment_submitted → payment_failed →
        payment_submitted → payment_succeeded → order_confirmed

Sunburst shows:
  Centre: all order sequences
  Ring 1: checkout_started (100%), ...
  Ring 2: payment_submitted (95%), abandoned (5%)
  Ring 3: payment_failed (30%), payment_succeeded (65%)
  Ring 4 (after failed): payment_submitted (80%), abandoned (20%)
  Ring 5 (retry path): payment_succeeded (70%), ...

Hover "payment_failed" at ring 3:
  START → checkout_started → payment_submitted → payment_failed
  30% of all orders · 1,240 sequences

Double-click "payment_failed":
  Zoom in: show only retry paths
  Ring 1 becomes payment_submitted (retry)
  Ring 2: payment_succeeded (70%) vs abandoned (30%)

Right-click "payment_failed" → property distribution:
  Shows histogram of "error_code" values for all failed sequences
```

SOL query to scope to high-value orders before building the sunburst:
```sol
match A(checkout_started)
if A.order_value > 100
filter MATCHED
```

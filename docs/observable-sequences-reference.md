# Observable Sequences Collection — Reference

> Source: https://observablehq.com/collection/@mikpanko/sequences
> Notebooks analysed: barcode chart, tennis sunburst, NFL data, tennis data.
> Extracted May 2026.

---

## 1. NFL Plays — Barcode Chart

### What it shows

Every play from an entire NFL season (up to ~45k plays) displayed as a
**scrollable horizontal grid** — one row per game, one rectangle per play.

```
                    game clock time →  (or total yards, or play number)
      ─────────────────────────────────────────────────────────────────────►
GB@CHI│[run][pass][pass][sack][punt] │[run][pass][pass][sack][punt]│  ...  │
 -19  │ Q1 drive 1 (GB)              │ Q1 drive 2 (CHI)            │       │
      ├──────────────────────────────┼─────────────────────────────┼───────┤
KC@NE │[kickoff][pass][run][TD][XP]  │[run][run][field_goal]       │  ...  │
 -19  │                              │                             │       │
      ├──────────────────────────────┼─────────────────────────────┼───────┤
...
```

- **X axis**: time elapsed (seconds), or cumulative yards gained, or play number
- **Y axis**: one band per game (`d3.scaleBand`, `bandwidth` = `cell_height=15px`)
- **Width of each rect**: proportional to play duration, yards gained, or fixed
- **Colour**: play type, EPA (expected points added), or WPA (win probability added)
- **Stroke**: black outline when a selected player was involved; red on hover

### Layout

```
┌─── SVG overlay (y-axis labels, fixed position) ────────────────────────┐
│  GB@CHI-19 │                                                            │
│  KC@NE-19  │                                                            │
│  DAL@NYG-19│                                                            │
└────────────┘
┌─── Scrollable div ─────────────────────────────────────────────────────►
│  ─── x-axis (game clock / yards / play number) ──────────────────────  │
│  [rect][rect][rect][rect][rect][rect][rect][rect][rect][rect][rect]... │  game 1
│  [rect][rect][rect][rect][rect][rect][rect][rect][rect][rect][rect]... │  game 2
│  ...                                                                   │
└────────────────────────────────────────────────────────────────────────►
```

Two SVG elements: a fixed-position one for the y-axis labels, and a
horizontally scrollable one for the data. `fullwidth` = up to ~4500px for a
full 2019 season by game clock.

### Controls

| Control | Values |
|---|---|
| Year | 2009–2019 |
| Team | all / any NFL team (filter to games involving that team) |
| Player | none / any player (highlights plays involving that player) |
| Plot plays by | `play_duration` (seconds), `play_yards` (yards gained), `play_number` |
| Color plays by | `play_type`, `epa` (diverging red-green), `wpa` (diverging red-green) |

### Colour scheme — play type

```
run              → #fed9a6  (light orange)
pass             → #fddaec  (light pink)
qb_scramble      → #fbb4ae  (salmon)
kneel/spike      → #e5d8bd  (tan)
touchdown        → #238b45  (dark green)
field_goal       → #41ab5d  (medium green)
two_point_conv   → #74c476  (light green)
extra_point      → #a1d99b  (pale green)
return_touchdown → #a50f15  (dark red)
safety           → #cb181d  (red)
interception     → #ef3b2c  (bright red)
fumble_lost      → #fb6a4a  (orange-red)
turnover_on_downs→ #fc9272  (light orange-red)
sack             → #fcbba1  (very light red)
kickoff          → #377eb8  (blue)
punt             → #984ea3  (purple)
penalty          → #999999  (grey)
non_play         → black
```

### Hover tooltip

```
description: (15:00) A.Jones left tackle to GB 25 for no gain (R.Smith).
play_type:   normal
play_formation: regular
play_attempt:   run
play_result:    second_down
pos_team:    GB
quarter:     1
score:       0:0
down:        1
yard_line:   75
yards_to_go: 10
play_yards:  0
timestamp:   0
play_duration: 27
play_epa:    -0.76436
play_wpa:    -0.02066
```

### Play data schema

Each play rectangle in the chart has these fields (from the real 2019 CSV):

```
game_id           — "2019090500" (date + game number)
home_team         — "CHI"
away_team         — "GB"
quarter           — 1–4
timestamp         — seconds elapsed in game
quarter_seconds_remaining
pos_team          — possessing team
drive             — drive number within game
home_score / away_score
play_num_game     — play number within game
play_num_drive    — play number within drive
yard_line         — yards from own end zone (0-100)
down              — 1-4
yards_to_go       — yards needed for first down
no_huddle         — boolean
timeout_team
play_type         — "normal", "kickoff", "punt", "field_goal", etc.
play_formation    — "regular", "shotgun", "under_center", etc.
play_attempt      — "run", "pass", "qb_scramble", "kickoff", "punt", etc.
play_result       — "first_down", "second_down", "sack", "touchdown", etc.
play_duration     — seconds
play_yards        — net yards on the play
total_yards       — cumulative yards in the game (computed)
play_group        — derived colour group (collapses play_attempt + play_result)
epa               — expected points added
wpa               — win probability added
passer_player_name, receiver_player_name, rusher_player_name, kicker_player_name
tackle_player_names, interception_player_name, forced_fumble_player_name
kick_returner_player_name, blocked_player_name, qb_hit_player_names
pass_defense_player_names, penalty_player_name
highlighted_play  — boolean (true if selected player was involved)
```

### Joxette mapping

| Barcode chart concept | Joxette equivalent |
|---|---|
| One row per game | One row per `entity_id` |
| One rect per play | One `EntityRecord` event |
| X axis by time | `timestamp` column (Kafka producer timestamp) |
| X axis by play number | event index within the sequence |
| Width = play_duration | `timestamp[n+1] - timestamp[n]` (gap to next event) |
| Colour by play_type | `messageType` → colour hash |
| Colour by EPA/WPA | any numeric field from the event `value` JSON |
| Player highlight | SOL match result — highlight matched events |
| Scrollable | horizontal scroll already in `CassetteTimeline.tsx` |

This is essentially what `CassetteTimeline.tsx` already does, but the barcode
chart adds: **proportional-width rectangles** (event duration), **numeric
colour modes** (diverging scale), and **player/field-based filtering**.

---

## 2. Tennis Rallies — Sunburst Chart

A second instance of the sunburst pattern, this time for tennis shot sequences.
Key differences from the NFL sunburst:

### Data model

Each **rally** is one sequence. Each **shot** is one event:

```javascript
{
  _eventName: "forehand_topspin",  // shot type
  player: "Djokovic",
  side: "deuce",                   // court side
  depth: "deep",                   // ball depth
  direction: "cross-court",
  outcome: "in",                   // in / out / net / winner / forced_error / unforced_error
  // ... other shot properties
}
```

Sequence properties:
```javascript
{
  match_id: "...",
  tournament: "Wimbledon",
  round: "Final",
  set: 1, game: 3, point: 2,
  server: "Djokovic",
  point_outcome: "winner",         // who won the point
  events: [ ...shots... ]
}
```

### Additional controls vs NFL sunburst

- `playerGenderVisible` — filter by player gender
- `playerVisible` — filter to rallies involving a specific player
- `playerRalliesVisible` — only rallies where player hit ≥ N shots
- `modifyEventsRules` — pre-process events before building hierarchy
  (uses the `rulesWidget` to transform shot types, merge sequences, etc.)
- `sequencePattern` — a single match pattern string applied before building
  the hierarchy (e.g. `"-(:forehand_topspin)-"` to find all rallies containing
  a forehand topspin)
- `whereCondition` — JS code string for arbitrary sequence-level filtering

### Shot type colour scheme

Shot types grouped into families, each family gets a hue:

```
Serve family:    serve, serve_ace, double_fault → blues
Forehand family: forehand_topspin, forehand_slice, forehand_volley → oranges
Backhand family: backhand_topspin, backhand_slice, backhand_volley → greens
Special:         overhead, drop_shot, lob → purples
```

---

## 3. NFL Play-by-Play Data

### Data pipeline

Raw `nflscrapR` data → cleaned & normalised → `nfl_reg_pbp_{year}_small.csv`.

Transformation logic (drives → sequences):
1. Group plays by `(game_id, drive)` — each drive is one sequence
2. Compute `timestamp` as running total of `play_duration` within game
3. Compute `total_yards` as running total of `|play_yards|` within game
4. Derive `play_group` — collapses `play_attempt` + `play_result` into a
   colour category (e.g. if `play_result == "touchdown"`, group = "touchdown")
5. Mark `highlighted_play` if a selected player appears in any player name field

Key computed fields:
```javascript
play_group = play_attempt;                    // default = play attempt type
if (play_result == null)    play_group = 'non_play';
if (play_attempt == 'qb_kneel' || 'qb_spike') play_group = 'kneel/spike';
if (play_result in ['touchdown', 'return_touchdown', 'fumble_lost',
    'safety', 'interception', 'sack', 'turnover_on_downs'])
    play_group = play_result;                 // outcome overrides attempt type
if (play_result in ['*_penalty', 'offsetting_penalties', 'declined_penalty'])
    play_group = 'penalty';
```

---

## 4. Tennis Shot-by-Shot Data

Source: professional tennis match shot sequences from tracking data.

Each row is one shot in a rally. Grouped into rallies (sequences) by
`(match_id, set, game, point)`.

Shot properties include: `player`, `side`, `depth`, `direction`, `spin`,
`shot_type`, `outcome`, `x_position`, `y_position`.

Rally (sequence) properties include: `match_id`, `tournament`, `round`,
`surface`, `server`, `score_before`, `point_outcome`.

---

## 5. Barcode Chart as a Cassette Viewer — Design Implications

The barcode chart is the most direct analogue to a Joxette cassette view:

```
Cassette (entity type: "order", entity_id: "order-42"):

  timestamp (ms) →
  ──────────────────────────────────────────────────────────────────────►
  [checkout_started][payment_submitted][    payment_failed   ][payment_submitted][payment_succeeded][order_confirmed]
  └───── 1.2s ──────┘└─── 0.4s ────────┘└────── 8.3s ────────┘└─── 0.3s ──────┘└──── 2.1s ────────┘└─── 0.1s ──────┘
```

Key design ideas from the barcode chart applicable to `CassetteTimeline.tsx`:

**Proportional width by real time gap:**
```typescript
// width of rect = proportional to (next_event.timestamp - this_event.timestamp)
const width = xScale(nextEvent.timestamp) - xScale(event.timestamp)
```
Currently `CassetteTimeline` uses proportional spacing already — this validates
the approach.

**Dual X-axis modes:**
- By wall-clock time (absolute `kafka_timestamp`)
- By event index (play number equivalent)
- By cumulative "metric" — e.g. offset delta, or a numeric field from `value`

**Multiple entity rows:**
The barcode chart's biggest power: showing **all** entity IDs of a type on
one scrollable chart. This is the natural complement to the sunburst — the
sunburst aggregates all sequences into one radial view, the barcode shows each
sequence as its own row.

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────►
│  order-001  │[checkout][pay_submit][pay_ok][confirmed]                                                   │
│  order-002  │[checkout][pay_submit][pay_fail][pay_submit][pay_ok][confirmed]                             │
│  order-003  │[checkout][pay_submit][pay_fail][pay_submit][pay_fail][abandoned]                           │
│  order-004  │[checkout][abandoned]                                                                       │
│  order-005  │[checkout][pay_submit][pay_ok][confirmed]                                                   │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────►
```

**After SOL match:** highlighted events show in accent colour; non-matched in grey:

```
│  order-002  │[checkout][pay_submit][PAY_FAIL][pay_submit][PAY_FAIL][abandoned]  ← matched tag = red
│              │ ░░░░░░░░  ░░░░░░░░░░  ████████  ░░░░░░░░░░  ████████  ░░░░░░░░  │
```

**Numeric colour mode** — colour by a numeric field (e.g. `order_value`,
`response_time_ms`). Use `d3.scaleDivergingSqrt` or `d3.scaleSequential`.

---

## 6. Updated Joxette Roadmap Items

These notebooks add the following to the implementation plan:

### Barcode multi-entity view
```
POST /cassettes/entities/{type}/barcode
Body: { from, to, limit, solQuery, colorField }
Response: ordered list of { entityId, events: [{messageType, timestamp, fields}] }
```

- New `EntityBarcodeView` component — horizontally scrollable, one row per entity
- X axis: timestamp (real time) or event index
- Rect width: proportional to `timestamp[n+1] - timestamp[n]`
- Colour: `messageType` (categorical) or any numeric field (diverging scale)
- After SOL match: matched events highlighted, non-matched greyed
- Y axis label: `entity_id` (clickable → entity detail page)

### Tennis → Joxette equivalence
The tennis sunburst confirms the pattern generalises to any domain. In Joxette
terms, a "shot" = one `EntityRecord`, a "rally" = one entity's full sequence.
The `modifyEventsRules` pre-processing step maps directly to running a SOL
`replace` or `set` operation before building the sunburst hierarchy.

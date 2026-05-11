# Motif Analytics — UI Reference

> Reverse-engineered from Motif Analytics video recordings (April 2026).
> Purpose: inform the design of Joxette's SOL query UI, sequence viewer, and
> analytics visualisations. Every component below is drawn from direct visual
> observation of the running product, supplemented by audio transcripts.

---

## 1. Application Shell

Motif is a single-page web app. The top-level layout is a **fixed header bar**
above a **two-panel body** that fills the remaining viewport height.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  [M] Motiflix (extended) ▼   [◫ Share]  [⊞]  [◂]                            │
│  ──────── Homepage Examples                                                   │
├────────────────────────────────────────────────────────────────────────────── │
│                      TAB BAR (view switcher)                                  │
│  Examples │ Barcode │ Outcome │ Table │ Plot │ Graph ᵝ      [>> View options] │
├───────────────────────┬───────────────────────────────────────────────────────┤
│  LEFT PANEL           │  MAIN VIEW AREA                                       │
│  (fixed 290px)        │  (fills remaining width)                              │
│                       │                                                       │
│                       │                                                       │
│                       │                                                       │
│  ─────────────────    │                                                       │
│  SEQUENCE MODEL       │                                                       │
│  (collapsible, ~150px)│                                                       │
├───────────────────────┴───────────────────────────────────────────────────────┤
│  STATUS BAR                                                                    │
│  273k sequences  ·  Sampled to 2%  ·  Split by 14.1 per actor  ·              │
│  Displaying 3.85M upsampled sequences                                         │
└────────────────────────────────────────────────────────────────────────────── ┘
```

**Header bar** elements (left to right):
- Logo mark (small `M` icon)
- Dataset name + workspace label — dropdown to switch datasets
- `Share` button (blue/primary)
- Two icon buttons: layout toggle (split/collapsed), sidebar collapse

**Tab bar** sits between header and body. Active tab is underlined, not
background-highlighted. Tabs: `Examples`, `Barcode`, `Outcome`, `Table`,
`Plot`, `Graph ᵝ` (beta badge).

**`>> View options`** button on the right of the tab bar opens a right-side
panel of controls that changes per tab. These are the visualisation controls —
never the query controls.

---

## 2. Left Panel

```
┌─────────────────────────────────────────────┐
│  [>> Query more]  [</> Help]                │  ← context actions row
├──────────────────┬────────────┬─────────────┤
│  Query           │  Events    │  Dimensions │  ← sub-tab bar
├──────────────────┴────────────┴─────────────┤
│                                             │
│  [+ ✦ Query Copilot]   (magenta button)     │  ← copilot launcher
│   Try \ ...                                 │
│   • "Define funnel from search to watch"    │  ← example prompts
│   • "Compute duration between events"       │
│   • "Sessionize on 1 hour gap"              │
│                                             │
│  [☰ Add recipe ▼]    [?]                    │  ← recipe picker
│                                             │
│   1 │ // sessionize                         │
│   2 │ match split Session()+                │
│   3 │ if duration(Session[-1],              │
│     │    SUFFIX[0]) > 30min                 │
│   4 │                                       │
│   5 │ match watch_start                     │  ← query editor
│     │                                       │
│     │                                       │
│  ─────────────────────────────────────────  │
│  Shuffle sample ⌥⇧S                        │  ← keyboard hint
├─────────────────────────────────────────────┤
│  Sequence model                         [^] │  ← collapsible section
│                                             │
│  variant                                    │  ← sequence-level dim
│  ─────────────────────────────────────────  │
│  Unnamed Tag          72%  [⊞]             │
│  ████████████████░░░░░░░░░░░░░             │  ← % coverage bar
│  search_page                                │
│  ─────────────────────────────────────────  │
│  Unnamed Tag 2         0%  [⊞]             │
│  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░             │
│  watch_start                                │
│  ─────────────────────────────────────────  │
│  PREFIX                                     │
│  SUFFIX                                     │
└─────────────────────────────────────────────┘
```

### 2.1 Sub-tabs

| Tab | Content |
|-----|---------|
| `Query` | SOL code editor (default) |
| `Events` | Schema browser listing all event names in the dataset |
| `Dimensions` | Schema browser listing all dimension names per event type |

### 2.2 Query Copilot

Invoked via `\` keystroke anywhere in the editor, or the magenta
`+ ✦ Query Copilot` button. Opens an inline prompt overlay:

```
┌──────────────────────────────────────────────────────────┐
│  ✦  Ask the Query Copilot                                │
│  ─────────────────────────────────────────────────────── │
│  > Describe a common query recipe...                      │
│                                                          │
│  Suggested:                                              │
│  • Define funnel from search to movie page to watch      │
│  • Compute duration between search and watch             │
│  • Remove trailer events                                 │
│  • Sessionize on 1 hour gap                              │
└──────────────────────────────────────────────────────────┘
```

The copilot generates multi-line SOL code directly into the editor. The user
then edits the result (e.g. removing event names from a generated OR list).

### 2.3 Add Recipe Dropdown

```
┌────────────────────────────────────────────────┐
│  ☰ Add recipe ▼                                │
├────────────────────────────────────────────────┤
│  Start with a recipe                           │
│  ────────────────────────────────────────────  │
│  Define a funnel                               │
│  Filter to matched sequences                   │
│  Compute duration between events               │
│  Filter to specific events                     │
│  Find when something didn't happen             │
│  View sequence for a specific user             │
│  Explore before or after a specific event      │
│  Truncate sequences                            │
│  Split sequences on time gap between events    │
│  Remove specific events                        │
│  Remove consecutive repeated events            │
│  Change event names or other dimensions        │
│  Compute attribution                           │
│  Measure retention                             │
│  Label moments of churn                        │
└────────────────────────────────────────────────┘
```

Each item inserts a template SOL snippet into the editor.

### 2.4 Query Editor

```
┌──────────────────────────────────────────────────────┐
│  1 │ // sessionize                                    │  ← comment (greyed)
│  2 │ match split Session()+                           │  ← keyword blue
│  3 │ if duration(Session[-1], SUFFIX[0]) > 30min      │
│  4 │                                                  │
│  5 │ match watch_start█                               │  ← cursor
│    │         ┌───────────────────────────────────┐    │
│    │         │  ⊞ watch_start          Event    │    │  ← autocomplete
│    │         └───────────────────────────────────┘    │
└──────────────────────────────────────────────────────┘
```

Syntax colouring observed:
- `match`, `split`, `if`, `filter`, `set`, `replace` → blue / keyword colour
- `duration()`, function names → same blue
- String literals (e.g. `"top_movies_xp"`) → orange/amber
- Comments `//` → grey
- Tag names (e.g. `Session`, `LastTouch`) → bold, same colour as keywords
- Duration literals (`30min`, `1h`) → accent colour

**Autocomplete dropdown** fires:
1. After `>>` — shows all event names labelled `Event`
2. While typing an event/tag name — filters the list
3. After `.` on a tag — shows dimension names for that tag

```
┌─────────────────────────────────────────────────┐
│  ⊞ favorites_add          Event                │
│  ⊞ favorites_page         Event                │
│  ⊞ favorites_page_click   Event                │
│  ⊞ home_page              Event                │
│  ⊞ home_page_click        Event                │
└─────────────────────────────────────────────────┘
```

Each entry has a small grid icon prefix and the type label right-aligned.

### 2.5 Sequence Model Inspector

Shown at the bottom of the left panel. Collapsible with `[^]` / `[v]`.
Shows the current sequence's tag decomposition as a horizontal proportion bar:

```
Sequence model                                    [^]

variant                         ← sequence-level dimension value

Prefix                   66%   [⊞]
████████████████░░░░░░░         ← blue fill = % of sequence spanned

LastTouch                       ← matched tag name
░░░░░░░░░░░░░░░░████████        ← tag's position in sequence

Unnamed Tag              18%   [⊞]
░░░░░░░░░░░░░░░░░░░░░░░░

PREFIX                          ← implicit tag
SUFFIX
```

The `[⊞]` expand button shows example events in that tag slice.

---

## 3. Examples View (Sequence Grid)

The default view. Shows 25 sampled sequences.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  Actor     │  Sequence  (scrolls horizontally →)                             │
│  user_id   │                                                                  │
├────────────┼──────────────────────────────────────────────────────────────── │
│  6948      │ [home_page][home_page_click][movie_page][home_page][home_page_cl]│
│  21508     │ [search_page][search_results...][search_click][movie_page][searc]│
│  19080     │ [home_page][home_page_click][movie_page][home_page][favorites_pa]│
│  7964      │ [home_page][home_page_click][movie_page][home_page][home_page_cl]│
│  21077     │ [home_page][home_page_click][movie_page][watch_start][watch_hea] │
├────────────┴──────────────────────────────────────────────────────────────── │
│  23k sequences  >  Displaying 25 example sequences                            │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Event pill colours** (Events colour mode):
- Each distinct event name gets a unique pastel background colour
- The same event always gets the same colour within a session
- `home_page` → pink/salmon
- `search_page` → light green
- `movie_page` → light blue
- `watch_start` → teal/darker
- `watch_heartbeat` → lighter variant of watch_start colour

**After `match split` — stacked rows per session:**

```
│  Actor  │ split_index │ split_count │  Sequence                              │
├─────────┼─────────────┼─────────────┼─────────────────────────────────────── │
│         │      1      │     13      │ [signup][home_page][search_p][search_r] │
│         │      2      │     13      │ [home_page][search_page][search_re]...  │
│         │      3      │     13      │ [home_page][search_page][search_re]...  │
│ 63003a  │      7      │     13      │ (empty session - gap boundary)          │
│         │      8      │     13      │ (empty)                                 │
│  ...    │     ...     │     ...     │  ...                                    │
│         │      1      │     31      │ [signup][home_page][search_p]...        │
```

When a `match` tag is active, each row shows two lines: the minimap of
the full sequence (collapsed, dense dots) on the top line, and the matched
portion with tag label badge on the bottom line:

```
│         │      1 │ 13 │ [Session] signup home_page search_page search_re ... │
│         │      2 │ 13 │ [Session] home_page search_page search_results...    │
│         │      3 │ 13 │ [Session] home_page search_page search_results...    │
```

The `[Session]` badge is a blue rounded rectangle preceding the events.

---

## 4. Barcode View

A **density heatmap** — each row is a population of sequences, stacked
vertically. The y-axis is a count scale (500k, 1M, 1.5M...). Each column
position represents an event step. Width of a column block = population
proportion. Hovering shows a tooltip.

```
         1       2       3       4       5       6       7       8       9  10
         ┌───────┬───────┬───────┬───────┬──────┬────────┬────────┬───────┐
  START  │       │       │       │       │      │        │        │       │ CONTINUE
         │       │       │       │       │      │        │        │       │
500k─    │ home  │search │search │movie_ │watch │watch_h │watch_h │ ...   │
         │ _page │ _page │ _re.. │ page  │ _sta │ eart   │ eart   │       │
         ├───────┴───────┤       │       │      │        │        │       │
  1M─    │  home_page_cl │       │       │      │        │        │       │
         │               │       │       │      │        │        │       │
         ├───────┬───────┴───────┤       │      │        │        │       │
1.5M─    │ home  │    movie_page │       │      │        │        │       │
         │       │               │       │      │        │        │       │
         ├───────┼───────┬───────┤       │      │        │        │       │
  2M─    │ home  │favor..│favor..│       │      │        │        │       │
```

**Hover tooltip** on any block:

```
┌─────────────────────────────┐
│  movie_page                 │
│  60.6k    Count             │
│  100%     Percent previous  │
└─────────────────────────────┘
```

### 4.1 Barcode: Tags colour mode

When a `match` produces tags, the matched events are highlighted in the same
blue as the tag label; everything else is greyed out.

```
   [Session] ████████████████░░░░░░░░░░░░░░   ← blue = session events
   [Session] ████████░░░░░░░░░░░░░░░░░░░░░░
   [Session] ██████████████████████░░░░░░░░
```

### 4.2 Barcode: Outcome colour mode

Colours sequences by whether they reached the outcome event. The right panel
shows a `Path` view — ranked list of events with outcome rate and count:

```
View options
─────────────────────────────────────
Color
Color by:  [Events] [Tags] [Outcome*] [Compare]

Outcome tag:
  ⊞ watch_start  [▼]

Y-axis
Aggregation type:  [Aggregate*]  [Sampled]

X-axis
Truncation:  ┃┃┃  10  [▼]
Alignment:   [⊞] [⊟]  START  [▼]
Text labels: [On*]  [Off]
─────────────────────────────────────

Path                        "View example sequence"
START                                        3.85M

  Prefix               65.6%
  home_page             2.52M
                        50.4%
  search_page           1.27M
                        99.6%
  search_results_page   1.27M
                        51.3%
  search_click           650k
                       100.0%
  movie_page             650k
                        16.8%
  search_results_page    109k
                        55.4%
  search_click          60.6k
                       100.0%
  movie_page            60.6k
```

The barcode itself renders in a warm/orange-red gradient — warmer = higher
outcome rate, cooler/blue = lower. A legend bar appears bottom-right:
`Change in outcome rate  ←100%  ░░░░███████░░░  +100%`.

### 4.3 Barcode: Compare mode (A/B experiments)

Two series rendered side-by-side in each column block:
- **Blue** = more common in group A (control)
- **Orange** = more common in group B (treatment)

Hovering shows the split tooltip:

```
┌──────────────────────────────────────────────────────────┐
│  search_page                                             │
│  182k (58.7%)  control                                   │
│   94.8k (24.9%) treatment                               │
│  -33.8%  Difference                                     │
└──────────────────────────────────────────────────────────┘
```

Right panel in Compare mode:

```
Color by:  [Events] [Tags] [Outcome] [Compare*]

Compare dimension:  ⊞ variant  [▼]

Compare groups:
  A  ◉ control   [▼]
  B  ◉ treatment [▼]

Y-axis  ...
X-axis  ...

                        → control    → treatment
START
  home_page      60.5%  +12.8%  73.3%   ██░░  ████████
  search_page    58.7%  -33.8%  24.9%   ████████  ██░░
```

The right-side path list shows: `group A %`, `delta`, `group B %`, plus
proportional bars for each.

Legend: `Transition rate    A larger ←──────────► B larger`

---

## 5. Plot View

A configurable chart builder. The chart area is empty until you click
**`Run query`** (green button that appears in the left panel when on the Plot
tab). The right panel is fully expanded.

```
View options
─────────────────────────────────────────────────────
Plot title:  [                              ]  [Reset plot]

Plot type:
  [Bar ▼]  (other options: Line, Scatter, Histogram)

X axis                                              [^]
  Dimension:  [⊞ search_page exists  ×  ▼]  Type: [ab ▼]
  + sort    + bin

Y axis                                              [^]
  Dimension:  [─ duration from start       ▼]  Type: [# ▼]
  + scale
  Aggregate:  [mean                        ▼]  [⊖]

Group by                                            [^]
  Dimension:  [⊞ variant               ×  ▼]  Type: [ab ▼]
  + sort   + bin   + stack

Styling                                             [^]
  X axis label:  [                              ]
  ☐ x-axis grid
  Y axis label:  [                              ]
  ☑ y-axis grid

Normalize:  [Percent of all (including null values) ▼]  [⊖]
─────────────────────────────────────────────────────
```

**Dimension picker dropdown** (opens when clicking a Dimension field):

```
┌─────────────────────────────────────────────────────────┐
│  Select dimension                               [▲]     │
│  ─────────────────────────────────────────────────────  │
│  ⊞  Sequence start timestamp                            │
│  ⊞  Sequence duration                                   │
│  ⊞  Sequence length                                     │
│  ── (tag-derived computed dimensions) ──                │
│  ⊞  movie_page  exists                                  │
│  ⊞  movie_page  position                                │
│  ⊞  movie_page  duration from start                     │
│  ⊞  split_index                                         │
│  ⊞  split_count                                         │
│  ── (event-level dimensions grouped by tag) ──          │
│  ⊞  movie...  >  actor                                  │
│  ⊞  movie...  >  app                                    │
│  ⊞  movie...  >  browser                                │
│  ⊞  movie...  >  category                               │
│  ⊞  movie...  >  country                                │
│  ⊞  movie...  >  experiment                             │
│  ⊞  movie...  >  id                                     │
│  ⊞  movie...  >  language                               │
│  ⊞  movie...  >  movie_duration                         │
│  ⊞  movie...  >  movies_number                          │
│  ⊞  movie...  >  name                                   │
│  ⊞  movie...  >  position                               │
└─────────────────────────────────────────────────────────┘
```

Dimensions are categorised into: computed (from the query), tag-derived,
sequence-level, and raw event properties per tag.

**Rendered bar chart** (last-touch attribution, grouped by variant):

```
  60%─  ██ (search_page)
  55%─
  50%─       ▒▒ (top_movies_page, treatment=orange)
  45%─
  40%─
  35%─
  30%─
  25%─  ░░                              ░░ (home_page, control=blue)
  20%─       ██ (search_page)
  15%─
  10%─                 ░░ (home_page)   ▒▒
   5%─  ▒▒
   0%
        search_page  top_movies_page  home_page  favorites_page
                          LastTouch.name

        variant:  ■ control  ■ treatment
```

Chart tooltip on hover:
```
┌──────────────────────────┐
│  x  top_movies_page      │
│  y  47%                  │
└──────────────────────────┘
```

---

## 6. Status Bar

Persistent bar at the bottom of the main view area. Updates live with query
execution statistics:

```
273k sequences  ·  Sampled to 2%  ·  Split by 14.1 per actor  ·  Displaying 3.85M upsampled sequences
```

| Field | Meaning |
|---|---|
| `273k sequences` | Total unique actors in dataset |
| `Sampled to 2%` | Display samples this % of the population |
| `Split by 14.1 per actor` | Average sessions per user after `match split` |
| `Displaying 3.85M upsampled sequences` | Extrapolated count for visualisation |

Also seen:
- `Filtered actors to 40.8%` — after `filter MATCHED`
- `Filtered actors to 46.2%` — progressive filtering
- `9.07k sequences · Sampled to 42% · Filtered to 98.5%`

---

## 7. Complete Example: Experiment Analysis Query

The full SOL pipeline visible in the last-touch attribution demo:

```
 1  match experiment_exposure
 2  if experiment_exposure.experiment =
 3     "top_movies_xp"
 4  set variant = experiment_exposure.variant
 5  filter MATCHED
 6  replace PREFIX with null
 7
 8  // sessionize
 9  match split Session()+
10  if duration(Session[-1], SUFFIX[0]) > 1h
11
12  // match LastTouch as last page event before watching
13  match LastTouch(home_page | search_page
14     | favorites_page | top_movies_page) >>
15     (^home_page, search_page, movie_page,
16     search_results_page, favorites_page,
17     top_movies_page)* >> watch_start
```

Lines 1-6 scope to experiment participants and strip pre-experiment history.
Lines 8-10 sessionise. Lines 12-17 find the last page visit before watching.

Sequence model inspector at this point shows:
- `variant` — sequence dim set on line 4
- `Prefix 66%` — pre-last-touch events
- `LastTouch` — the matched tag
- `SUFFIX`

---

## 8. Joxette Component Mapping

### 8.1 SOL Query Panel

Maps to: a new `SolQueryPanel` component in the entity detail page,
replacing / augmenting the existing `SequenceQueryPanel`.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  SOL Query                                                        [Run ▶]   │
│  ─────────────────────────────────────────────────────────────────────────  │
│  [Query] [Events] [Dimensions]                                              │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                             │
│   1 │ match Login(login) >> * >> Buy(purchase)                             │
│   2 │ if duration(Login, Buy) < 5min                                       │
│     │                                                                      │
│  ─────────────────────────────────────────────────────────────────────────  │
│  [☰ Add recipe ▼]    (recipes: funnel, attribution, sessionize, retention)  │
│                                                                             │
│  POST  /cassettes/entities/{type}/{id}/sol-match                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

- Editor: CodeMirror with SOL syntax extension (keywords, autocomplete from `messageType` vocab)
- Autocomplete vocabulary: fetch distinct `messageType` values from entity stats endpoint
- Recipes: pre-built SOL snippets for common Joxette patterns (funnel, attribution, retention, dedup)

### 8.2 Sequence Grid (Examples equivalent)

Maps to: the existing `CassetteTimeline.tsx` canvas viewer + a new table view.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  [Paged] [SSE] [NDJSON]        [↑ oldest first]  [↓ latest first]           │
│  ─────────────────────────────────────────────────────────────────────────  │
│  Timestamp           │ Type        │ Topic    │ Key    │ Value (expand)      │
│  ─────────────────── │ ─────────── │ ──────── │ ────── │ ──────────────────  │
│  2025-01-15 10:30:00 │ [login    ] │ orders   │ u-42   │ {…}                 │
│  2025-01-15 10:31:00 │ [browse   ] │ orders   │ u-42   │ {…}                 │
│  2025-01-15 10:33:00 │ [purchase ] │ orders   │ u-42   │ {…}  ← SOL matched  │
│  2025-01-15 10:45:00 │ [logout   ] │ orders   │ u-42   │ {…}                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

After a SOL match, events in the matched tag range get a coloured left border
or background tint. Tag name shown as a badge inline:

```
│  [MATCHED: Buy] 2025-01-15 10:33:00 │ [purchase] │ orders │ …  │
```

Use `SolResultMapper.tagsForIndex(i, tags)` to compute tag badges per row.

### 8.3 Barcode / Timeline

Maps to: `CassetteTimeline.tsx` (already built). Enhancements from Motif:

- **Colour by tag** — when SOL result has tags, colour matched events in accent,
  non-matched in grey
- **Outcome mode** — colour event blocks green/red based on whether they precede
  a specified outcome event in the same sequence
- The canvas already supports zoom/pan/keyboard nav — keep that

### 8.4 Plot / Aggregation View

Not yet built. Maps to a new `SolPlotPanel` (future work):
- X axis: any computed dimension (e.g. `split_count`, `tag.exists`)
- Y axis: count / mean / sum of a numeric dimension
- Group by: any categorical dimension
- Renders a bar chart using the existing Recharts dependency in the UI

### 8.5 Sequence Model Inspector

Maps to: new `SolSequenceInspector` collapsible panel in the entity detail page.

```
┌───────────────────────────────────┐
│  Sequence model               [^] │
│                                   │
│  entity_id: order-42              │
│  ─────────────────────────────    │
│  MATCHED (Buy)           1 event  │
│  ███░░░░░░░░░░░░░░░░░░░░          │
│                                   │
│  PREFIX                  2 events │
│  ██████░░░░░░░░░░░░░░░░░          │
│                                   │
│  SUFFIX                  1 event  │
│  ░░░░░░░░░░░░░░░░░░░░░░░█         │
└───────────────────────────────────┘
```

Data source: `SolResult.tags()` mapped back via `SolResultMapper`.

### 8.6 Status Bar

Add to the entity detail page below the records table:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  4 events  ·  SOL matched  ·  1 unexpected null                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

Fields: total events in sequence, match status (matched / not matched),
unexpected null count (links to expandable error list).

---

## 9. Key Interaction Patterns to Replicate

| Motif pattern | Joxette implementation |
|---|---|
| Live preview as you type | Debounce 500ms → POST sol-match → stream results via SSE |
| Autocomplete from dataset vocab | GET distinct `messageType` from entity stats; inject into CodeMirror completions |
| Recipe library | Dropdown of SOL snippets targeting Joxette use-cases (funnel, dedup, sessionize, retention) |
| Colour-coded event pills | `messageType` → stable hash → CSS hue in `CassetteTimeline` and grid |
| Tag highlighting in grid | `tagsForIndex()` → coloured left border + tag badge on matched rows |
| Barcode tag mode | `CassetteTimeline` canvas: render tag spans as coloured overlays on the timeline bar |
| Outcome mode | Post-process result: for each event, check if a specified outcome event follows within N seconds; colour accordingly |
| Compare mode | Two SOL queries (different entity IDs or different filters) → overlay two timelines in different colours |
| Sequence model inspector | `SolResult.tags()` → horizontal % bars showing coverage of each tag |
| Status bar | Event count · match status · unexpected null count |

---

## 10. Colour System Observed

Motif uses a consistent colour vocabulary for event types and states:

| Element | Colour |
|---|---|
| Unmatched / PREFIX events | Salmon / pink (`#FED7D7` range) |
| Matched / tagged events | Blue (`#3182CE` / `#BEE3F8` range) |
| Outcome achieved | Teal / green |
| Outcome not achieved | Light grey |
| A/B control group | Blue (`#3182CE`) |
| A/B treatment group | Orange (`#DD6B20`) |
| Running / active state | Accent blue |
| Query Copilot button | Magenta / purple (`#805AD5` range) |
| Session tag badge | Dark blue, white text, rounded rectangle |
| Suffix indicator | Grey, lighter weight |
| Event pills (Events mode) | Pastel, unique per event name, consistent hashing |

For Joxette, a reasonable mapping:
- Matched events → `var(--accent)` tint
- Unmatched PREFIX/SUFFIX → `var(--surface-raised)`
- Tag badges → `var(--accent)` background, white text
- Event pills → `messageType` → `hsl(hash(name) * 137.5, 60%, 85%)` (golden angle distribution)

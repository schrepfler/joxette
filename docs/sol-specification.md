# SOL — Sequence Operations Language Specification

> Extracted from the Motif Analytics documentation (wayback archive, April 2026).
> This spec is the basis for implementing SOL-based sequence search over Joxette cassettes.

---

## Overview

SOL is a domain-specific language for sequence analytics. It enables:

- **Matching** — finding complex event patterns based on order and labelling them (regex-style but for event sequences)
- **Replacing** — transforming sequences by replacing tagged sub-sequences with new events
- **Filtering** — selecting or discarding sequences based on conditions
- **Setting** — computing new dimensions on events or sequences
- **Splitting / Combining** — sessionizing or re-merging sequences

SOL operates on **each sequence independently and in parallel**. There is no cross-sequence aggregation within SOL — that is handled outside (post-query, e.g., in a visualization layer or in SQL after the fact).

---

## Data Model

### Sequences

A **sequence** is an ordered list of events belonging to the same entity (e.g., all Kafka messages for a given `entity_id`). In Joxette, a sequence maps naturally to the messages in an entity cassette for one entity ID, ordered by `(timestamp, recorded_at)`.

### Events

Each event has:
- A **name** (the Kafka topic event type or a field extracted from the message)
- A **timestamp** (`ts`) — the Kafka producer timestamp
- Zero or more **event dimensions** — arbitrary key/value properties extracted from the message

### Sequence Dimensions

Properties that apply to the entire sequence (e.g., `entity_id`, `entity_type`).

### Tags

Labels assigned to one or more consecutive events within a sequence by a MATCH or MATCH SPLIT operation. Tags are the primary mechanism for referencing sub-sequences in subsequent operations.

**Implicit tags** (always available after MATCH):
| Tag | Meaning |
|-----|---------|
| `PREFIX` | Events before the first matched event |
| `SUFFIX` | Events after the last matched event |
| `MATCHED` | All matched events |
| `SEQ` | All events in the sequence |
| `start` | Sequence boundary anchor (before first event) |
| `end` | Sequence boundary anchor (after last event) |

---

## Operations

Operations are chained in order. Each operation transforms the sequence (or its tags/dimensions) and passes it to the next.

### MATCH

Evaluates a match pattern on each sequence and assigns tags to matching sub-sequences.

```
match <match pattern>
  {if <match condition>}
```

Only the **first** occurrence is matched. For all occurrences, use `match split`.

#### Pattern Elements

| Syntax | Meaning |
|--------|---------|
| `event_name` | Match a single event with that name |
| `Tag(event_name)` | Match an event and label it `Tag` |
| `Tag(event1 \| event2)` | Match any of the listed events |
| `Tag(^event1, event2)` | Match any event NOT in the exclusion list |
| `p1 >> p2` | `p1` followed immediately (consecutively) by `p2` |
| `p1 >> * >> p2` | `p1` followed by any events then `p2` (wildcard) |
| `start` | Sequence start anchor |
| `end` | Sequence end anchor |

#### Quantifiers

Quantifiers follow the tag or event name:

| Quantifier | Meaning |
|-----------|---------|
| `+` | One or more |
| `*` | Zero or more (optional) |
| `?` | Zero or one (optional) |
| `{n}` | Exactly n |
| `{n,}` | At least n |
| `{,m}` | At most m |
| `{n,m}` | Between n and m |

#### Matching Algorithm

- Left-to-right, event-by-event.
- Priority at each event: start next tag > extend current tag > finalize without consuming event.
- Backtracking on mismatch (worst-case O(n²) for pathological patterns).
- **Middle tags**: lazy (prefer jumping to next tag early).
- **Edge tags**: greedy (prefer extending).
- Optional tags (`?`, `*`, `{,m}`): preferred over zero-match when ambiguous.

#### IF Condition

Same expression syntax as FILTER. Commonly used for time constraints:

```
match event1 >> * >> event2
  if duration(event1, event2) < 5min
```

"Until" conditions are evaluated efficiently at tag boundaries.

#### Tag Indexing (Array Slicing)

Tags act as arrays of matched events. Python-style slicing:

| Syntax | Meaning |
|--------|---------|
| `Tag[0]` | First event |
| `Tag[-1]` | Last event |
| `Tag[2:-2]` | Slice, excluding first 2 and last 2 |
| `Tag.dim_name` | Broadcast: list of `dim_name` values for all events in Tag |
| `position(Tag)` | Index of Tag's first event within the full sequence |

#### Examples

```
// Simple event match
match purchase

// Consecutive events
match product_page >> purchase

// With wildcard (any events between)
match search >> * >> purchase

// Named tags
match ViewProducts(product_page)+ >> Buy(purchase)

// Exclude events (any page except home_page before conversion)
match search_page >> (^home_page)* >> Conversion(purchase_movie | rent_movie)

// Anchored
match start >> Login(login) >> * >> Logout(logout) >> end

// Time-constrained
match Before()* >> event1
  if duration(Before, event1) <= 1h
```

---

### MATCH SPLIT

Identical to MATCH but finds **all non-overlapping occurrences** and splits the sequence before each match.

```
match split <match pattern>
  {if <match condition>}
```

After splitting:
- Each sub-sequence is processed independently.
- Auto-added sequence dimensions: `split_index` (0-based), `split_count` (total splits).
- `PREFIX` gets `split_index = 0`.
- `SUFFIX` covers events between matches and after the last match.
- Workflow: find first match → continue from event after match → repeat.

#### Examples

```
// Split before every occurrence of event1
match split event1

// Sessionization: split when gap between session events exceeds 1 hour
match split Session()+
  if duration(Session[-1], SUFFIX[0]) > 1h

// Split on every event (one sub-sequence per event)
match split Event()

// Split by name pattern
match split Event()
  if Event.name like '%checkout%'
```

---

### FILTER

Retains only sequences (or sub-sequences after split) matching the condition.

```
filter <boolean expression>
filter MATCHED            // keep only sequences where MATCH succeeded
filter not MATCHED        // keep sequences where MATCH did not succeed
```

#### Examples

```
filter MATCHED
filter SEQ[0].ts >= '2024-01-01'
filter length(SEQ) > 3
filter duration(event1, event2) < 10min
```

---

### SET

Creates or updates event dimensions or sequence dimensions.

```
set <sequence_dim> = <expression>
set <tag>.event_dim = <expression>
set <tag>[n:m].event_dim = <expression>
```

#### Examples

```
// Sequence-level computed dimension
set duration_1_to_2 = duration(event1, event2)

// Copy dimension from one tag to another
set event2.user_id = event1.user_id

// Compute time-since-last for all events except the first
set SEQ[1:].time_since_prev = SEQ[1:].ts - SEQ[:-1].ts

// Aggregation into a sequence dimension
set total_amount = sum(SEQ.amount)

// String manipulation
set SEQ.label = concat(SEQ.category, '_', SEQ.action)
```

---

### REPLACE

Replaces a single tag with a new subsequence of events.

```
replace <old_tag_name> with <replace pattern>
  {dims <dimension assignments>}
```

#### Replace Pattern Elements

| Syntax | Meaning |
|--------|---------|
| `NewTag(new_event_name)` | Insert a new event |
| `NewTag(@OldTag)` | Copy events from OldTag |
| `OldTag` | Shorthand for `OldTag(@OldTag)` (keep in place) |
| `null` | Remove the tag's events entirely |

New events get `ts` set to the first event's `ts` unless explicitly overridden in `dims`.

#### Examples

```
// Remove matched events
replace Tag with null

// Insert a new event after Tag
replace Tag with Tag >> (new_event)

// Merge multiple matched events into one
match A+
replace A with (single_event) dims single_event.value = sum(A.value)

// Reorder two events
match event1 >> event2
replace MATCHED with event2 >> event1

// Keep only the last of consecutive duplicates
match split A(event){2,}
replace A with A[-1]
combine
```

---

### COMBINE

Merges sub-sequences created by `match split` back into a single sequence.

```
combine
  {<merged_dim> = <aggregation_expression>, ...}
```

- Only merges sub-sequences from the **last** split operation.
- Multiple `combine` calls unwind prior splits in reverse order.
- Sequence dimensions that are identical across all sub-sequences are automatically preserved.
- All existing tags are removed after combine.

#### Examples

```
// Count how many times event occurred
match split event
combine count_event = max(split_index)

// Rename events then re-merge
match split Event(event1)
set Event.name = 'renamed_event'
combine

// Deduplicate: keep first occurrence of each event name
match split Events()
  if Events.name not in PREFIX.name
replace SEQ with Events
combine

// Deduplicate: keep last occurrence of each event name
match split Events()
  if Events.name not in SUFFIX.name
replace SEQ with Events
combine
```

---

## Functions & Operators Reference

### Logical

| Operator | Description |
|----------|-------------|
| `a and b` | Boolean AND |
| `a or b` | Boolean OR |
| `not a` | Boolean NOT |

### Relational

| Operator | Description |
|----------|-------------|
| `=, !=, <, >, <=, >=` | Standard comparisons |
| `v between a and b` | Range test (inclusive) |
| `v in array` | Array membership |

### Tag / Array

| Syntax | Description |
|--------|-------------|
| `Tag[n]` | Single element at index n (0-based, negative from end) |
| `Tag[n:m]` | Slice |
| `Tag.dim` | Broadcast dimension across all events in tag |
| `position(Tag)` | Index of Tag's first event within SEQ |

### String

| Function | Description |
|----------|-------------|
| `s like 'pat'` | SQL LIKE (`%` = any chars, `_` = single char) |
| `s similar to 'regex'` | Regex match (ECMA/JavaScript rules) |
| `concat(s1, s2, ...)` | Concatenation |
| `strlen(s)` | String length |
| `edit_distance(s1, s2)` | Levenshtein distance |
| `regex_count(s, pat)` | Count regex matches |
| `regex_substr(s, pat)` | Extract first match |
| `regex_replace(s, pat, repl)` | Replace all matches |
| `lower(s)` | Lowercase |
| `upper(s)` | Uppercase |

### Mathematical

| Function/Op | Description |
|-------------|-------------|
| `+, -, *, /, ^` | Arithmetic |
| `abs(v)` | Absolute value (scalar or array) |
| `ceiling(v)` | Ceiling |
| `floor(v)` | Floor |
| `round(v)` | Round |
| `log(v)` | Log base 10 |
| `rand(max?)` | Random float 0.0–1.0 |

### Aggregations (operate on arrays / tags)

| Function | Description |
|----------|-------------|
| `all(arr)` | True if all elements non-null |
| `any(arr)` | True if any element non-null |
| `min(arr)` | Minimum |
| `max(arr)` | Maximum |
| `avg(arr)` | Average (nulls included as 0) |
| `sum(arr)` | Sum |
| `length(arr)` | Count of elements |
| `unique(arr)` | Deduplicated array |

### Conditional

| Function | Description |
|----------|-------------|
| `if(cond, true_val, false_val)` | Ternary / conditional expression |
| `coalesce(v1, v2, ...)` | First non-null value |

### Array Utilities

| Function | Description |
|----------|-------------|
| `array_extract(arr, i)` | Element at 0-based index i |
| `array_position(arr, item)` | 0-based index of item in arr |
| `array_remove(arr, item)` | Remove all occurrences of item |
| `array_to_string(arr, delim)` | Join to delimited string |
| `flatten(arr)` | Flatten nested array |

### Time & Duration

Duration literals: `5.5s`, `10min`, `1h`, `7d`, `52w`, `1y`

| Function | Description |
|----------|-------------|
| `duration(tag1, tag2)` | Shorthand: `tag2[-1].ts - tag1[0].ts` |
| `now()` | Current timestamp |
| `date(ts)` | Returns `YYYY-MM-DD` string |
| `time(ts)` | Returns `HH:MM:SS.XXX` string |
| `datetime(ts)` | Returns `YYYY-MM-DD HH:MM:SS.XXX` string |
| `datepart(part, ts)` | Extract part: `year`, `quarter`, `month`, `month_name`, `day`, `dayofyear`, `week`, `weekday`, `weekday_name`, `hour`, `minute`, `second`, `millisecond` |
| `time_bucket(granularity, ts)` | Truncate to granularity: `ms`, `s`, `m`, `h`, `d`, `w`, `month`, `q`, `y` |

### Geospatial

Points: `[longitude, latitude]` (WGS84). Bounding boxes: `[sw_lon, sw_lat, ne_lon, ne_lat]`.

| Function | Description |
|----------|-------------|
| `geo_area(bbox)` | Area in km² |
| `geo_centroid(bbox)` | Centroid point |
| `geo_contains(point, bbox)` | Is point within bbox? |
| `geo_distance(p1, p2)` | Distance in km |
| `geo_intersection(bbox1, bbox2)` | Intersection bounding box |
| `geo_overlap(bbox1, bbox2)` | Overlap proportion (0–1) |

### Type Casting

```
cast(<value> AS <type>)
```

Types: `number`, `string`, `boolean`, `timestamp`, `duration`

---

## Error Handling — "Show Must Go On"

Rather than throwing runtime errors, SOL casts uncomputable expressions to `null` and records them as **unexpected nulls**. Queries always complete.

Four sources of unexpected nulls:
1. Referencing a non-existent tag or dimension name
2. Operations on incompatible types
3. Operations on tagged event arrays of different sizes
4. Referencing a non-existent tag index

---

## Mapping to Joxette

| SOL concept | Joxette equivalent |
|-------------|-------------------|
| Sequence | Entity cassette for one `entity_id`, ordered by `(timestamp, recorded_at)` |
| Event | A Kafka message in the entity cassette |
| Event name | Typically the Kafka topic name, or a field extracted from the message value |
| `ts` dimension | `timestamp` column (Kafka producer timestamp) |
| `recorded_at` dimension | `recorded_at` column (ingestion time) |
| Sequence dimension | `entity_id`, `entity_type`, `entity_bucket` |
| Event dimension | Any field from the message `value` JSON |
| General cassette | No natural sequence grouping; SOL applies after grouping by `(partition, offset)` run |

### Implementation Approach for Joxette

1. **Query layer**: Translate a SOL query into a plan that:
   - Fetches the ordered event list for one or more entity IDs from DuckLake
   - Executes MATCH / FILTER / SET / REPLACE / COMBINE operations in memory (per sequence)
   - Returns the transformed sequences (with tags and computed dimensions) to the caller

2. **DuckDB integration**: Where possible, push time-range and partition filters down to DuckLake SQL before loading events into the SOL engine. SOL itself operates in-memory over the loaded sequence.

3. **Parallelism**: Each entity sequence is independent — run SOL over multiple entity IDs concurrently using Jox structured concurrency.

4. **Streaming output**: After SOL evaluation, stream matched/transformed events back over SSE or NDJSON using the existing `CassetteController` streaming infrastructure.

---

## Quick Reference Examples

```sol
// How many times did checkout happen per user?
match split checkout
combine checkout_count = max(split_index)

// Find sessions (gap > 30 min), label each session
match split Session()+
  if duration(Session[-1], SUFFIX[0]) > 30min

// All sequences where a purchase followed a search within 5 minutes
match Search(search) >> * >> Buy(purchase)
  if duration(Search, Buy) < 5min
filter MATCHED

// Compute time between first login and first purchase
match Login(login) >> * >> FirstPurchase(purchase)
set time_to_purchase = duration(Login, FirstPurchase)
filter MATCHED

// Remove duplicate consecutive events (keep last of each run)
match split A(event){2,}
replace A with A[-1]
combine

// Keep only sequences that contain event X but NOT event Y
match start >> (^event_y)* >> event_x >> * >> end
filter MATCHED

// Tag events with time since previous event
set SEQ[1:].gap = SEQ[1:].ts - SEQ[:-1].ts
```

# Entity Cassette Ordering and Clock Skew

## How events are ordered

Entity cassettes aggregate messages from multiple Kafka topics. The replay query
orders events by:

1. **`kafka_timestamp` (primary)** — the timestamp assigned by the Kafka producer
   at the time of publishing. This is the logical event time and is the "natural"
   order a consumer expects to see events in.
2. **`recorded_at` (tiebreaker)** — the wall-clock time at which Joxette ingested
   the message. Used only when two events from different topics carry the same
   `kafka_timestamp` millisecond.
3. **`topic`, `partition`, `offset`** — deterministic tiebreakers to make the
   cursor stable when timestamp equality persists across a page boundary.

This ordering is applied after deduplication (which keeps the most-recently
recorded copy per `(topic, partition, offset)` tuple).

## The clock-skew caveat

`kafka_timestamp` is set by the **producer**, not the broker or Joxette. When an
entity receives events from multiple services — for example `orders.events` and
`payments.events` — each service runs on its own host with its own clock. Even
with NTP, producer clocks can diverge by tens or hundreds of milliseconds.

Practical consequence: an `OrderPaid` event produced at 10:00:00.050 on the
payments service may carry an earlier `kafka_timestamp` than an `OrderCreated`
event produced at 10:00:00.010 on the orders service if the payments clock is
running behind. The replay will surface `OrderPaid` before `OrderCreated`, even
though causally the creation happened first.

### When this matters

| Situation | Impact |
|---|---|
| All producers share the same clock source (e.g. same NTP server, same datacenter) | Skew typically < 5 ms; ordering is reliable in practice |
| Producers span multiple regions or cloud providers | Skew can reach 50–500 ms; visible misordering possible |
| Producers set `kafka_timestamp` explicitly from an upstream event time | Ordering reflects that upstream time, which may be minutes or hours off from wall clock |

## What to do when producer clocks are unreliable

**Use `recorded_at` as the authoritative timeline.**

`recorded_at` is set by Joxette at ingestion time. Because all topics flow
through a single Joxette process backed by a single DuckDB instance, `recorded_at`
values are monotonically non-decreasing within a single ingestion run. They reflect
the order in which Joxette *saw* the messages — a consistent, single-clock view
even when producer clocks vary.

To reconstruct the ingestion-time order:

```sql
SELECT *
FROM lake.main.entity_order
WHERE entity_id = 'order-789'
ORDER BY recorded_at ASC, kafka_timestamp ASC,
         topic ASC, kafka_partition ASC, kafka_offset ASC;
```

Or, via the replay API, supply the `order_by=recorded_at` query parameter
(where supported) to switch the primary sort key.

### Tradeoffs

| Sort key | Strength | Weakness |
|---|---|---|
| `kafka_timestamp` (default) | Closest to "event happened at" semantics; useful for business analysis | Vulnerable to producer clock skew across topics |
| `recorded_at` | Consistent single-clock view; immune to cross-producer skew | Does not reflect event time; affected by Joxette restarts or catch-up replays where recorded_at is far later than event time |

There is no universally correct ordering for cross-topic entity events. Choose
the sort key that matches your query intent:

- **"What order did these events happen in from the business perspective?"** →
  `kafka_timestamp`, accept best-effort skew.
- **"What order did Joxette see these events?"** → `recorded_at`.
- **"I need exact causal ordering."** → Introduce a monotonic sequence number in
  your producers or use a single Kafka topic per entity type to eliminate
  cross-topic ambiguity.

## Replay cursor stability

The replay cursor encodes
`(kafka_timestamp, recorded_at, topic, partition, offset)`. Changing the
primary sort key between pages would invalidate cursors. If you switch from the
default `kafka_timestamp`-primary order to a `recorded_at`-primary order, start
a fresh replay from the beginning; do not resume an existing cursor from the
opposite ordering.

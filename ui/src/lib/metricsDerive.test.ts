import { describe, it, expect, vi } from 'vitest'
import { computeLagSeries, computeRateSeries, computeNetPts, type PtRecord } from './metricsDerive'

// Deliberately partial: these fixtures only populate the fields the three
// functions under test actually read. PtRecord (Omit<DataPoint, 'ts'>) is a
// wide production type — the assertion below is a test-fixture-only escape
// hatch, not a claim that this object satisfies the full DataPoint shape.
const pts: PtRecord[] = [
  { ts: '1000', partitionLag: { orders: { '0': 5, '1': 2 } }, partitionConsumedRate: { orders: { '0': 10, '1': -1 } }, consumedRate: { t0: 9 }, pollDurationP50: { t0: 1.5 }, pollDurationP99: { t0: 4 }, fetchLatencyMax: { t0: 12 } },
  { ts: '2000', partitionLag: { orders: { '0': 3 } }, partitionConsumedRate: { orders: { '0': 8 } }, consumedRate: { t0: 8 }, pollDurationP50: { t0: 2 }, pollDurationP99: { t0: 5 }, fetchLatencyMax: { t0: 15 } },
] as unknown as PtRecord[]

describe('computeLagSeries', () => {
  it('builds one row per point, keyed by partition, defaulting missing partitions to 0', () => {
    expect(computeLagSeries(pts, ['0', '1'], 'orders')).toEqual([
      { ts: '1000', p0: 5, p1: 2 },
      { ts: '2000', p0: 3, p1: 0 },
    ])
  })
})

describe('computeRateSeries', () => {
  it('clamps negative partition rates to 0 and includes the topic-level total', () => {
    expect(computeRateSeries(pts, ['0', '1'], 'orders', 't0')).toEqual([
      { ts: '1000', p0: 10, p1: 0, total: 9 },
      { ts: '2000', p0: 8, p1: 0, total: 8 },
    ])
  })
})

describe('computeNetPts', () => {
  it('extracts poll p50/p99 and fetch-latency-max per point for the given topic key', () => {
    expect(computeNetPts(pts, 't0')).toEqual([
      { ts: '1000', p50: 1.5, p99: 4, flmax: 12 },
      { ts: '2000', p50: 2, p99: 5, flmax: 15 },
    ])
  })
})

describe('memoization contract (documentation, not enforcement — see metrics/index.tsx TopicRow)', () => {
  it('computeLagSeries is a pure function of its three arguments — same inputs, same output reference-independent result', () => {
    const spy = vi.fn(computeLagSeries)
    const a = spy(pts, ['0', '1'], 'orders')
    const b = spy(pts, ['0', '1'], 'orders')
    expect(spy).toHaveBeenCalledTimes(2)
    expect(a).toEqual(b) // same result even though called twice — safe to useMemo
  })
})

// Pure, DOM-free derivations for the per-topic metric charts in
// src/routes/metrics/index.tsx. Extracted out of TopicRow so they can be:
//   1. Unit tested without rendering React or touching the DOM.
//   2. Wrapped in useMemo with an honest, minimal dependency list — the
//      function signature *is* the dependency list.

export type MetricSample = { labels: Record<string, string>; value: number }
export type MetricFamily = { help: string; type: string; samples: MetricSample[] }

export interface DataPoint {
  ts: number
  topicKeys: string[]
  topicLabels: Record<string, string>
  lag:          Record<string, number>
  committedLag: Record<string, number>
  consumed:     Record<string, number>
  written:      Record<string, number>
  consumedRate: Record<string, number>
  bytesRate:    Record<string, number>
  fetchLatency: Record<string, number>
  partitionLag:          Record<string, Record<string, number>>
  partitionConsumedRate: Record<string, Record<string, number>>
  pollDurationP50:    Record<string, number>
  pollDurationP99:    Record<string, number>
  fetchLatencyMax:    Record<string, number>
  fetchThrottle:      Record<string, number>
  networkIoRate:      Record<string, number>
  writeDepth:    number
  writeDuration: number
  compactionFiles: number
  retentionRows:   number
  catalogBytes:    number
  inlinedBytes:    number
  duckdbMemoryTotal: number
  duckdbMemoryByTag: Record<string, number>
  activeReplays:   number
  heapUsed:        number
  heapMax:         number
  processRss:      number
}

export type PtRecord = Omit<DataPoint, 'ts'> & { ts: string }

export function computeLagSeries(pts: PtRecord[], partitions: string[], label: string): Array<Record<string, string | number>> {
  return pts.map(pt => {
    const row: Record<string, string | number> = { ts: pt.ts }
    for (const p of partitions) row[`p${p}`] = pt.partitionLag?.[label]?.[p] ?? 0
    return row
  })
}

export function computeRateSeries(pts: PtRecord[], partitions: string[], label: string, tk: string): Array<Record<string, string | number>> {
  return pts.map(pt => {
    const row: Record<string, string | number> = { ts: pt.ts }
    for (const p of partitions) {
      row[`p${p}`] = Math.max(0, pt.partitionConsumedRate?.[label]?.[p] ?? 0)
    }
    row['total'] = pt.consumedRate[tk] ?? 0
    return row
  })
}

export interface NetPt { ts: string; p50: number; p99: number; flmax: number }

export function computeNetPts(pts: PtRecord[], tk: string): NetPt[] {
  return pts.map(pt => ({
    ts: pt.ts,
    p50:  pt.pollDurationP50[tk] ?? 0,
    p99:  pt.pollDurationP99[tk] ?? 0,
    flmax: pt.fetchLatencyMax[tk] ?? 0,
  }))
}

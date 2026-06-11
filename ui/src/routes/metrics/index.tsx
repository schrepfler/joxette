import { createFileRoute } from '@tanstack/react-router'
import { useEffect, useRef, useState } from 'react'
import {
  AreaChart, Area, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Legend,
} from 'recharts'
import { ChartContainer, ChartTooltip, ChartTooltipContent, type ChartConfig } from '@/components/ui/chart'
import { healthApi } from '../../api/client'
import { Layout } from '../../components/Layout'
import { pageTitle, cardStyle } from '../../styles/shared'

export const Route = createFileRoute('/metrics/')({ component: MetricsPage })

// ---------------------------------------------------------------------------
// Prometheus text-format parser
// ---------------------------------------------------------------------------

type MetricSample = { labels: Record<string, string>; value: number }
type MetricFamily = { help: string; type: string; samples: MetricSample[] }

function parsePrometheus(text: string): Record<string, MetricFamily> {
  const result: Record<string, MetricFamily> = {}
  let currentHelp = ''
  let currentType = ''

  for (const raw of text.split('\n')) {
    const line = raw.trim()
    if (!line) continue
    if (line.startsWith('# HELP ')) {
      const rest = line.slice(7); const sp = rest.indexOf(' ')
      currentHelp = rest.slice(sp + 1); continue
    }
    if (line.startsWith('# TYPE ')) {
      const rest = line.slice(7); const sp = rest.indexOf(' ')
      currentType = rest.slice(sp + 1); continue
    }
    if (line.startsWith('#')) continue

    const bo = line.indexOf('{'), bc = line.indexOf('}')
    let name: string, labels: Record<string, string> = {}, valueStr: string
    if (bo >= 0 && bc > bo) {
      name = line.slice(0, bo)
      const labelStr = line.slice(bo + 1, bc)
      valueStr = line.slice(bc + 1).trim().split(' ')[0]
      for (const pair of labelStr.split(',')) {
        const eq = pair.indexOf('='); if (eq < 0) continue
        labels[pair.slice(0, eq).trim()] = pair.slice(eq + 1).trim().replace(/^"|"$/g, '')
      }
    } else {
      const sp = line.indexOf(' '); if (sp < 0) continue
      name = line.slice(0, sp); valueStr = line.slice(sp + 1).split(' ')[0]
    }
    const value = parseFloat(valueStr); if (isNaN(value)) continue
    const family = name.replace(/_bucket$|_count$|_sum$|_max$/, '')
    if (!result[family]) result[family] = { help: currentHelp, type: currentType, samples: [] }
    result[family].samples.push({ labels: { ...labels, __name__: name }, value })
  }
  return result
}

// The Prometheus parser groups _sum/_count/_bucket/_max samples under the stripped family
// key (e.g. joxette_write_duration_seconds_sum → family key joxette_write_duration_seconds).
// Try the stripped key first so callers can pass the full suffixed name.
function familyFor(family: Record<string, MetricFamily>, name: string) {
  const stripped = name.replace(/_bucket$|_count$|_sum$|_max$/, '')
  return family[stripped] ?? family[name]
}

function getSample(family: Record<string, MetricFamily>, name: string, labelFilter: Record<string, string> = {}): number {
  const f = familyFor(family, name); if (!f) return NaN
  for (const s of f.samples) {
    if (s.labels.__name__ !== name) continue
    if (Object.entries(labelFilter).every(([k, v]) => s.labels[k] === v)) return s.value
  }
  return NaN
}

// Sum all samples matching a full metric name across every label combination.
// Used for per-topic timers where we want the aggregate across all topics.
function sumSamples(family: Record<string, MetricFamily>, name: string): number {
  const f = familyFor(family, name); if (!f) return NaN
  let total = 0, found = false
  for (const s of f.samples) {
    if (s.labels.__name__ !== name) continue
    total += s.value; found = true
  }
  return found ? total : NaN
}

function getSamplesBy(family: Record<string, MetricFamily>, name: string, groupBy: string): Record<string, number> {
  const f = family[name]; if (!f) return {}
  const result: Record<string, number> = {}
  for (const s of f.samples) {
    if (s.labels.__name__ !== name) continue
    const key = s.labels[groupBy] ?? 'unknown'
    result[key] = (result[key] ?? 0) + s.value
  }
  return result
}

// Returns { topic → { partition → value } } for metrics that have both labels
function getSamplesByTopicAndPartition(family: Record<string, MetricFamily>, name: string): Record<string, Record<string, number>> {
  const f = family[name]; if (!f) return {}
  const result: Record<string, Record<string, number>> = {}
  for (const s of f.samples) {
    if (s.labels.__name__ !== name) continue
    const topic = s.labels['topic'] ?? 'unknown'
    const partition = s.labels['partition']
    if (!partition) continue  // skip topic-level (no partition tag)
    if (!result[topic]) result[topic] = {}
    result[topic][partition] = (result[topic][partition] ?? 0) + s.value
  }
  return result
}

// ---------------------------------------------------------------------------
// Rolling history
// ---------------------------------------------------------------------------

const MAX_POINTS = 60
const POLL_MS    = 3_000

// Topic names may contain dots which break Recharts dataKey (object traversal).
// We map topics to safe short keys like t0, t1, t2…
interface DataPoint {
  ts: number
  topicKeys: string[]           // safe keys (t0, t1…) in order
  topicLabels: Record<string, string>  // t0 → "feed.betgenius.fixture.v1"
  // per-topic (indexed by tN) — aggregated across partitions
  lag:          Record<string, number>   // fetch-position lag (cheap, updated every poll)
  committedLag: Record<string, number>   // committed-offset lag (AdminClient, 15 s cache)
  consumed:     Record<string, number>
  written:      Record<string, number>
  consumedRate: Record<string, number>
  bytesRate:    Record<string, number>
  fetchLatency: Record<string, number>
  // per-partition metrics: { "feed.betgenius.fixture.v1": { "0": 123, "1": 456 } }
  partitionLag:          Record<string, Record<string, number>>
  partitionConsumedRate: Record<string, Record<string, number>>
  // network diagnostics
  pollDurationP50:    Record<string, number>  // kc.poll() p50 ms per topic
  pollDurationP99:    Record<string, number>  // kc.poll() p99 ms per topic
  fetchLatencyMax:    Record<string, number>  // Kafka fetch-latency-max ms
  fetchThrottle:      Record<string, number>  // broker throttle ms (>0 → quota hit)
  networkIoRate:      Record<string, number>  // I/O ops/s
  // aggregates
  writeDepth:    number
  writeDuration: number
  compactionFiles: number
  retentionRows:   number
  catalogBytes:    number
  inlinedBytes:    number
  activeReplays:   number
  heapUsed:        number
  heapMax:         number
}

// Stable topic-to-key mapping across scrapes
const topicKeyMap = new Map<string, string>()
function safeKey(topic: string): string {
  if (!topicKeyMap.has(topic)) topicKeyMap.set(topic, `t${topicKeyMap.size}`)
  return topicKeyMap.get(topic)!
}

function scrapeToPoint(family: Record<string, MetricFamily>): DataPoint {
  const topicNames = Object.keys(getSamplesBy(family, 'joxette_messages_consumed_total', 'topic'))
  const topicKeys: string[] = []
  const topicLabels: Record<string, string> = {}
  const lag: Record<string, number> = {}
  const committedLag: Record<string, number> = {}
  const consumed: Record<string, number> = {}
  const written: Record<string, number> = {}
  const consumedRate: Record<string, number> = {}
  const bytesRate: Record<string, number> = {}
  const fetchLatency: Record<string, number> = {}
  const pollDurationP50: Record<string, number> = {}
  const pollDurationP99: Record<string, number> = {}
  const fetchLatencyMax: Record<string, number> = {}
  const fetchThrottle:   Record<string, number> = {}
  const networkIoRate:   Record<string, number> = {}

  // Compute per-partition breakdowns first so we can derive correct per-topic totals.
  // getSample() returns only the first matching sample (partition 0), not the topic total.
  const partitionLag          = getSamplesByTopicAndPartition(family, 'joxette_consumer_lag')
  const partitionConsumedRate = getSamplesByTopicAndPartition(family, 'joxette_kafka_consumer_records_consumed_rate')

  for (const topic of topicNames) {
    const k = safeKey(topic)
    topicKeys.push(k); topicLabels[k] = topic
    // Sum all partition values so lag[k] is the true total for this topic,
    // not just partition 0 (which getSample() would have returned).
    lag[k]          = Object.values(partitionLag[topic] ?? {}).reduce((a, b) => a + b, 0)
                      || getSample(family, 'joxette_consumer_lag', { topic }) || 0
    committedLag[k] = getSample(family, 'joxette_consumer_committed_lag', { topic }) || 0
    consumed[k]     = getSample(family, 'joxette_messages_consumed_total',             { topic }) || 0
    written[k]      = getSample(family, 'joxette_messages_written_total',               { topic }) || 0
    consumedRate[k] = Object.values(partitionConsumedRate[topic] ?? {}).reduce((a, b) => a + b, 0)
                      || getSample(family, 'joxette_kafka_consumer_records_consumed_rate', { topic }) || 0
    bytesRate[k]    = getSample(family, 'joxette_kafka_consumer_bytes_consumed_rate',   { topic }) || 0
    fetchLatency[k] = getSample(family, 'joxette_kafka_consumer_fetch_latency_avg',     { topic }) || 0
    // poll() instrumentation: Micrometer timer → _seconds_sum/_count + percentile gauges
    const pollP50Raw = getSample(family, 'joxette_poll_duration_seconds', { topic, quantile: '0.5' })
    const pollP99Raw = getSample(family, 'joxette_poll_duration_seconds', { topic, quantile: '0.99' })
    pollDurationP50[k] = isNaN(pollP50Raw) ? 0 : pollP50Raw * 1000
    pollDurationP99[k] = isNaN(pollP99Raw) ? 0 : pollP99Raw * 1000
    fetchLatencyMax[k] = getSample(family, 'joxette_kafka_consumer_fetch_latency_max',       { topic }) || 0
    fetchThrottle[k]   = getSample(family, 'joxette_kafka_consumer_fetch_throttle_time_avg', { topic }) || 0
    networkIoRate[k]   = getSample(family, 'joxette_kafka_consumer_network_io_rate',         { topic }) || 0
  }

  // JVM heap: Micrometer uses label `id` not `area`
  const heapUsed = getSample(family, 'jvm_memory_used_bytes',  { id: 'heap' })
             || getSample(family, 'jvm_memory_used_bytes', { area: 'heap' }) || 0
  const heapMax  = getSample(family, 'jvm_memory_max_bytes',   { id: 'heap' })
             || getSample(family, 'jvm_memory_max_bytes',  { area: 'heap' }) || 0

  return {
    ts: Date.now(), topicKeys, topicLabels,
    lag, committedLag, consumed, written, consumedRate, bytesRate, fetchLatency,
    pollDurationP50, pollDurationP99, fetchLatencyMax, fetchThrottle, networkIoRate,
    partitionLag, partitionConsumedRate,
    writeDepth: getSample(family, 'joxette_write_channel_depth') || 0,
    writeDuration: (() => {
      const s = sumSamples(family, 'joxette_write_duration_seconds_sum')
      const c = sumSamples(family, 'joxette_write_duration_seconds_count')
      return (!isNaN(s) && c > 0) ? (s / c) * 1000 : 0
    })(),
    compactionFiles: getSample(family, 'joxette_compaction_files_processed_total') || 0,
    retentionRows: Object.values(getSamplesBy(family, 'joxette_retention_rows_deleted_total', 'table_type')).reduce((a, b) => a + b, 0),
    catalogBytes: getSample(family, 'joxette_catalog_size_bytes')   || 0,
    inlinedBytes: getSample(family, 'joxette_catalog_inlined_bytes') || 0,
    activeReplays: getSample(family, 'joxette_replay_active') || 0,
    heapUsed: Math.max(0, heapUsed),
    heapMax:  Math.max(0, heapMax),
  }
}

// ---------------------------------------------------------------------------
// Formatters
// ---------------------------------------------------------------------------

function fmt(n: number, d = 1) {
  if (!n || isNaN(n)) return '0'
  if (n >= 1e9) return `${(n / 1e9).toFixed(d)}B`
  if (n >= 1e6) return `${(n / 1e6).toFixed(d)}M`
  if (n >= 1e3) return `${(n / 1e3).toFixed(d)}K`
  return n.toFixed(d)
}
function fmtBytes(n: number) {
  if (!n || isNaN(n) || n <= 0) return '0'
  if (n >= 1 << 30) return `${(n / (1 << 30)).toFixed(2)} GB`
  if (n >= 1 << 20) return `${(n / (1 << 20)).toFixed(1)} MB`
  if (n >= 1 << 10) return `${(n / (1 << 10)).toFixed(1)} KB`
  return `${n} B`
}
function fmtMs(n: number) { return (isNaN(n) || n <= 0) ? '—' : `${n.toFixed(1)} ms` }
function timeTick(ts: number) {
  const d = new Date(ts)
  return `${d.getHours().toString().padStart(2,'0')}:${d.getMinutes().toString().padStart(2,'0')}:${d.getSeconds().toString().padStart(2,'0')}`
}

const PALETTE = ['#6674cc', '#3E9A7A', '#A26612', '#8B2121', '#1E5A8A', '#6B46A0']

// ---------------------------------------------------------------------------
// Tooltip bubble (shared)
// ---------------------------------------------------------------------------

const TIP_STYLE: React.CSSProperties = {
  position: 'absolute', bottom: 'calc(100% + 8px)', left: 0,
  zIndex: 200,
  background: '#1a1d2e',
  border: '1px solid #3a3d52',
  borderRadius: 6,
  padding: '8px 11px',
  width: 260,
  fontSize: '0.72rem',
  color: '#d4d8f0',
  lineHeight: 1.55,
  boxShadow: '0 8px 24px rgba(0,0,0,0.55)',
  pointerEvents: 'none',
  whiteSpace: 'normal',
  textAlign: 'left',
}

// ---------------------------------------------------------------------------
// Stat pill
// ---------------------------------------------------------------------------

function Stat({ label, value, sub, title }: { label: string; value: string; sub?: string; title?: string }) {
  const [tip, setTip] = useState(false)
  return (
    <div
      style={{ display: 'flex', flexDirection: 'column', minWidth: 110, position: 'relative', cursor: title ? 'default' : undefined }}
      onMouseEnter={() => title && setTip(true)}
      onMouseLeave={() => setTip(false)}
    >
      <span style={{ fontSize: '0.5625rem', color: 'var(--ink-tertiary)', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 600 }}>
        {label}
      </span>
      <span style={{ fontFamily: 'var(--font-mono)', fontSize: '1.125rem', fontWeight: 700, color: 'var(--ink-primary)', lineHeight: 1.3 }}>{value}</span>
      {sub && <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>{sub}</span>}
      {tip && title && <div style={TIP_STYLE}>{title}</div>}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart card
// ---------------------------------------------------------------------------

function Card({ title, subtitle, description, children }: { title: string; subtitle?: string; description?: string; children: React.ReactNode }) {
  const [tip, setTip] = useState(false)
  return (
    <div style={{ ...cardStyle, padding: '16px 20px' }}>
      <div style={{ marginBottom: 10, display: 'flex', alignItems: 'baseline', gap: 0, position: 'relative' }}>
        <span
          style={{ fontWeight: 600, fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-primary)', cursor: description ? 'default' : undefined, borderBottom: description ? '1px dotted var(--rule-strong)' : undefined }}
          onMouseEnter={() => description && setTip(true)}
          onMouseLeave={() => setTip(false)}
        >{title}</span>
        {subtitle && <span style={{ marginLeft: 8, fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>{subtitle}</span>}
        {tip && description && <div style={TIP_STYLE}>{description}</div>}
      </div>
      {children}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Per-topic chart row
// ---------------------------------------------------------------------------

type PtRecord = Omit<DataPoint, 'ts'> & { ts: string }

function TopicRow({ tk, label, latest, pts, axisProps }: {
  tk: string
  label: string
  latest: DataPoint | undefined
  pts: PtRecord[]
  axisProps: { tick: { fontSize: number; fill: string }; axisLine: boolean; tickLine: boolean }
}) {
  const partitions = Object.keys(latest?.partitionLag?.[label] ?? {})
    .sort((a, b) => Number(a) - Number(b))

  const lagSeries = pts.map(pt => {
    const row: Record<string, string | number> = { ts: pt.ts }
    for (const p of partitions) row[`p${p}`] = pt.partitionLag?.[label]?.[p] ?? 0
    return row
  })

  const rateSeries = pts.map(pt => {
    const row: Record<string, string | number> = { ts: pt.ts }
    for (const p of partitions) row[`p${p}`] = Math.max(0, pt.partitionConsumedRate?.[label]?.[p] ?? 0)
    return row
  })

  const pConfig: ChartConfig = {}
  partitions.forEach((p, i) => {
    pConfig[`p${p}`] = { label: `p${p}`, color: PALETTE[i % PALETTE.length] }
  })

  const netConfig: ChartConfig = {
    p50:  { label: 'poll p50',  color: PALETTE[0] },
    p99:  { label: 'poll p99',  color: PALETTE[1] },
    flmax: { label: 'fetch max', color: PALETTE[2] },
  }
  const netPts = pts.map(pt => ({
    ts: pt.ts,
    p50:  pt.pollDurationP50[tk] ?? 0,
    p99:  pt.pollDurationP99[tk] ?? 0,
    flmax: pt.fetchLatencyMax[tk] ?? 0,
  }))

  const currentLag  = latest ? Object.values(latest.partitionLag?.[label] ?? {}).reduce((a, b) => a + b, 0) : 0
  const currentRate = latest?.consumedRate[tk] ?? 0

  return (
    <div style={{ marginBottom: 28 }}>
      <div style={{
        display: 'flex', alignItems: 'baseline', gap: 16, marginBottom: 10,
        paddingBottom: 6, borderBottom: '1px solid var(--rule)',
      }}>
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-body-sm-size)', fontWeight: 600, color: 'var(--ink-primary)' }}>
          {label}
        </span>
        <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', fontFamily: 'var(--font-mono)' }}>
          lag {fmt(currentLag, 0)} · {fmt(currentRate, 1)} msg/s
        </span>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: 12 }}>
        <Card title="Lag" subtitle="stacked per partition"
          description="Messages available on the broker that the consumer has fetched but not yet processed. Stacked by partition. High lag means the consumer is behind — either slow writes or a burst of incoming messages.">
          <ChartContainer config={pConfig} className="h-[180px] w-full">
            <AreaChart data={lagSeries} stackOffset="none">
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={v => fmt(v, 0)} {...axisProps} width={44} />
              <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} formatter={(v: unknown) => fmt(Number(v), 0)} />} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              {partitions.map((p, i) => (
                <Area key={p} type="monotone" dataKey={`p${p}`} name={`p${p}`}
                  stackId="lag"
                  stroke={PALETTE[i % PALETTE.length]}
                  fill={PALETTE[i % PALETTE.length] + '55'}
                  strokeWidth={1.5} dot={false} isAnimationActive={false} />
              ))}
            </AreaChart>
          </ChartContainer>
        </Card>

        <Card title="Consume Rate" subtitle="msg/s per partition"
          description="Messages consumed per second from the broker, broken down by partition. Derived from the Kafka consumer records-consumed-rate metric. A drop here while lag rises means the consumer is stalling.">
          <ChartContainer config={pConfig} className="h-[180px] w-full">
            <AreaChart data={rateSeries} stackOffset="none">
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={v => fmt(v, 0)} {...axisProps} width={44} />
              <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} formatter={(v: unknown) => `${fmt(Number(v), 1)}/s`} />} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              {partitions.map((p, i) => (
                <Area key={p} type="monotone" dataKey={`p${p}`} name={`p${p}`}
                  stackId="rate"
                  stroke={PALETTE[i % PALETTE.length]}
                  fill={PALETTE[i % PALETTE.length] + '55'}
                  strokeWidth={1.5} dot={false} isAnimationActive={false} />
              ))}
            </AreaChart>
          </ChartContainer>
        </Card>

        <Card title="Bytes Rate" subtitle="bytes / s from broker"
          description="Raw bytes fetched per second from the broker for this topic. Useful for estimating network bandwidth and DuckLake write pressure. Includes message overhead, not just payload size.">
          <ChartContainer config={{ val: { label: 'bytes/s', color: PALETTE[0] } }} className="h-[180px] w-full">
            <LineChart data={pts.map(pt => ({ ts: pt.ts, val: pt.bytesRate[tk] ?? 0 }))}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={v => fmtBytes(v) + '/s'} {...axisProps} width={72} />
              <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} formatter={(v: unknown) => fmtBytes(Number(v)) + '/s'} />} />
              <Line type="monotone" dataKey="val" name="bytes/s"
                stroke={PALETTE[0]} strokeWidth={1.5} dot={false} isAnimationActive={false} />
            </LineChart>
          </ChartContainer>
        </Card>

        <Card title="Fetch Latency" subtitle="avg Kafka fetch round-trip (ms)"
          description="Average time between sending a FETCH request to the broker and receiving a response. On a quiet topic this will sit near fetch.max.wait.ms (500 ms) — the broker holds the request until data arrives. High latency on a busy topic indicates broker saturation or network issues.">
          <ChartContainer config={{ val: { label: 'fetch avg', color: PALETTE[1] } }} className="h-[180px] w-full">
            <LineChart data={pts.map(pt => ({ ts: pt.ts, val: pt.fetchLatency[tk] ?? 0 }))}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={v => v + 'ms'} {...axisProps} width={44} />
              <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} formatter={(v: unknown) => fmtMs(Number(v))} />} />
              <Line type="monotone" dataKey="val" name="fetch avg"
                stroke={PALETTE[1]} strokeWidth={1.5} dot={false} isAnimationActive={false} />
            </LineChart>
          </ChartContainer>
        </Card>

        <Card title="Network" subtitle="poll() p50/p99 + fetch-latency-max (ms)"
          description="poll() p50/p99: time spent inside KafkaConsumer.poll(), including broker wait. Near 100 ms means the consumer is broker-bound (waiting on fetch.max.wait.ms). Near 0 ms means local processing is the bottleneck. fetch-latency-max is the worst single fetch round-trip seen in the interval.">
          <ChartContainer config={netConfig} className="h-[180px] w-full">
            <LineChart data={netPts}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={v => fmtMs(v)} {...axisProps} width={56} domain={[0, 120]} />
              <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} formatter={(v: unknown) => fmtMs(Number(v))} />} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              <Line type="monotone" dataKey="p50" name="poll p50"
                stroke={PALETTE[0]} strokeWidth={2} dot={false} isAnimationActive={false} />
              <Line type="monotone" dataKey="p99" name="poll p99"
                stroke={PALETTE[1]} strokeWidth={1} strokeDasharray="4 2" dot={false} isAnimationActive={false} />
              <Line type="monotone" dataKey="flmax" name="fetch max"
                stroke={PALETTE[2]} strokeWidth={1} strokeDasharray="1 3" dot={false} isAnimationActive={false} />
            </LineChart>
          </ChartContainer>
        </Card>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

function MetricsPage() {
  const [history, setHistory] = useState<DataPoint[]>([])
  const [error, setError]     = useState<string | null>(null)
  const [lastTs, setLastTs]   = useState<number | null>(null)
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)

  async function poll() {
    try {
      const text = await healthApi.getMetricsText()
      const family = parsePrometheus(text)
      const point = scrapeToPoint(family)
      setHistory(prev => [...prev.slice(-(MAX_POINTS - 1)), point])
      setLastTs(Date.now()); setError(null)
    } catch (e) { setError(`Failed to fetch metrics: ${(e as Error).message}`) }
  }

  useEffect(() => {
    void poll()
    timerRef.current = setInterval(() => { void poll() }, POLL_MS)
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [])

  const latest   = history[history.length - 1]
  const topicKeys   = latest?.topicKeys ?? []
  const topicLabels = latest?.topicLabels ?? {}

  // Stringify ts to avoid Recharts treating numbers as object keys
  const pts = history.map(p => ({ ...p, ts: String(p.ts) }))

  const catalogConfig: ChartConfig = {
    catalogBytes: { label: 'catalog file', color: '#6674cc' },
    inlinedBytes: { label: 'inlined',       color: '#A26612' },
  }
  const heapConfig: ChartConfig = {
    heapMax:  { label: 'heap max',  color: 'var(--rule-strong)' },
    heapUsed: { label: 'heap used', color: '#1E5A8A' },
  }
  const writeConfig: ChartConfig = {
    writeDepth:    { label: 'depth',    color: '#6674cc' },
    writeDuration: { label: 'batch ms', color: '#3E9A7A' },
  }
  const replaysConfig: ChartConfig = {
    activeReplays: { label: 'active replays', color: '#6B46A0' },
  }

  const axisProps = {
    tick: { fontSize: 10, fill: 'var(--ink-tertiary)' },
    axisLine: false, tickLine: false,
  }

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 16, marginBottom: 24, flexWrap: 'wrap' }}>
        <h1 style={pageTitle}>Metrics</h1>
        {lastTs && (
          <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', fontFamily: 'var(--font-mono)' }}>
            last scrape {new Date(lastTs).toLocaleTimeString()} · {POLL_MS / 1000}s interval
          </span>
        )}
      </div>

      {error && (
        <div style={{ padding: '10px 14px', background: 'color-mix(in oklab, var(--signal-error) 10%, transparent)', border: '1px solid color-mix(in oklab, var(--signal-error) 30%, transparent)', borderRadius: 'var(--radius-sm)', color: 'var(--signal-error)', fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)', marginBottom: 24 }}>
          {error}
        </div>
      )}

      {/* Headline stats */}
      {latest && (
        <div style={{ ...cardStyle, padding: '16px 24px', marginBottom: 24, display: 'flex', gap: 32, flexWrap: 'wrap', alignItems: 'flex-start' }}>
          {topicKeys.length > 0 && (() => {
            const fetchLag  = topicKeys.reduce((s, k) => s + (latest.lag[k] ?? 0), 0)
            const committed = topicKeys.reduce((s, k) => s + (latest.committedLag[k] ?? 0), 0)
            const totalRate = topicKeys.reduce((s, k) => s + (latest.consumedRate[k] ?? 0), 0)
            return (
              <>
                <Stat label="fetch lag" value={fmt(fetchLag, 0)} sub="position-based"
                  title="Messages available on the broker that the consumer has fetched into its internal buffer but not yet processed. Updated every poll cycle. This lags slightly behind the true broker lag." />
                <Stat label="committed lag" value={committed > 0 ? fmt(committed, 0) : '—'}
                  sub={`${fmt(totalRate, 1)} msg/s · 15 s cache`}
                  title="endOffset − committedOffset, queried via the Kafka AdminClient. This matches what Redpanda Console and kafka-consumer-groups show. Cached for 15 s to avoid AdminClient overhead." />
              </>
            )
          })()}
          {topicKeys.length > 0 && <div style={{ width: 1, background: 'var(--rule)', alignSelf: 'stretch' }} />}
          <Stat label="write depth" value={String(latest.writeDepth)} sub="in-flight batches"
            title="Batches currently queued in the bounded write channel waiting for DuckDB. Non-zero is fine under load; sustained high values mean DuckDB writes are the bottleneck." />
          <Stat label="write latency" value={fmtMs(latest.writeDuration)} sub="avg batch"
            title="Average time to execute one INSERT batch against DuckLake. Includes the DuckDB JDBC call and any inline-to-Parquet flush triggered by the write." />
          {topicKeys.length > 0 && (() => {
            const maxP99      = topicKeys.reduce((m, k) => Math.max(m, latest.pollDurationP99[k] ?? 0), 0)
            const maxThrottle = topicKeys.reduce((m, k) => Math.max(m, latest.fetchThrottle[k] ?? 0), 0)
            return (
              <Stat label="poll p99" value={fmtMs(maxP99)}
                sub={maxThrottle > 1 ? `throttled ${fmtMs(maxThrottle)}` : 'no throttle'}
                title="Worst-case KafkaConsumer.poll() duration across all topics (p99). Near 100 ms means consumers are broker-bound (waiting on fetch.max.wait.ms). Near 0 ms means local DuckDB writes are pacing consumption. A throttle value > 0 means the broker is rate-limiting this client." />
            )
          })()}
          <Stat label="catalog" value={fmtBytes(latest.catalogBytes)} sub={`${fmtBytes(latest.inlinedBytes)} inlined`}
            title="Total size of the DuckDB catalog file. The inlined sub-value is data buffered inside the catalog before being flushed to Parquet on object storage." />
          <Stat label="replays" value={String(latest.activeReplays)} sub="active"
            title="Active replay-to-topic operations in progress. Each holds a Kafka producer and reads from DuckLake concurrently." />
          <Stat label="heap" value={fmtBytes(latest.heapUsed)} sub={`of ${fmtBytes(latest.heapMax)}`}
            title="JVM heap used vs max (-Xmx). Virtual threads are cheap, but batch buffers and DuckDB result sets live here. Consider increasing heap if consistently above 80% of max." />
        </div>
      )}

      {/* Per-topic rows */}
      {topicKeys.map(tk => (
        <TopicRow
          key={tk}
          tk={tk}
          label={topicLabels[tk] ?? tk}
          latest={latest}
          pts={pts}
          axisProps={axisProps}
        />
      ))}

      {/* Infrastructure charts */}
      <div style={{ marginTop: 8 }}>
        <div style={{ paddingBottom: 6, borderBottom: '1px solid var(--rule)', marginBottom: 12 }}>
          <span style={{ fontWeight: 600, fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em', fontSize: '0.6875rem' }}>
            Infrastructure
          </span>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))', gap: 16 }}>

          <Card title="Write Pipeline" subtitle="channel depth & batch latency"
            description="Depth: number of batches currently queued in the bounded write channel waiting for DuckDB. Batch ms: average time to execute one INSERT batch. A rising depth means DuckDB writes are slower than Kafka consumption — the channel is the backpressure valve."
          >
            <ChartContainer config={writeConfig} className="h-[180px] w-full">
              <LineChart data={pts}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
                <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
                <YAxis yAxisId="depth" {...axisProps} width={28} />
                <YAxis yAxisId="ms" orientation="right" tickFormatter={v => v + 'ms'} {...axisProps} width={48} />
                <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} />} />
                <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
                <Line yAxisId="depth" type="monotone" dataKey="writeDepth"    name="depth"    stroke="var(--color-writeDepth)"    strokeWidth={1.5} dot={false} isAnimationActive={false} />
                <Line yAxisId="ms"    type="monotone" dataKey="writeDuration" name="batch ms" stroke="var(--color-writeDuration)" strokeWidth={1.5} dot={false} isAnimationActive={false} />
              </LineChart>
            </ChartContainer>
          </Card>

          <Card title="Catalog Storage" subtitle="catalog file · inlined data"
            description="Catalog file: total size of the DuckDB .ducklake file on disk, including inlined data and metadata. Inlined: bytes currently buffered inside the catalog before being flushed to Parquet on object storage. DuckLake flushes automatically when the inline threshold is reached."
          >
            <ChartContainer config={catalogConfig} className="h-[180px] w-full">
              <AreaChart data={pts}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
                <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
                <YAxis tickFormatter={fmtBytes} {...axisProps} width={68} />
                <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} formatter={(v: unknown) => fmtBytes(Number(v))} />} />
                <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
                <Area type="monotone" dataKey="catalogBytes" name="catalog file" stroke="var(--color-catalogBytes)" fill="var(--color-catalogBytes)" fillOpacity={0.15} strokeWidth={1.5} dot={false} isAnimationActive={false} />
                <Area type="monotone" dataKey="inlinedBytes" name="inlined"       stroke="var(--color-inlinedBytes)" fill="var(--color-inlinedBytes)" fillOpacity={0.15} strokeWidth={1.5} dot={false} isAnimationActive={false} />
              </AreaChart>
            </ChartContainer>
          </Card>

          <Card title="JVM Heap" subtitle="used · max"
            description="JVM heap memory used vs the max heap size (-Xmx). Virtual threads are cheap on heap, but large DuckDB result sets and batch buffers are allocated here. Sustained usage above 80% of max warrants a heap increase."
          >
            <ChartContainer config={heapConfig} className="h-[180px] w-full">
              <AreaChart data={pts}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
                <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
                <YAxis tickFormatter={fmtBytes} {...axisProps} width={68} />
                <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} formatter={(v: unknown) => fmtBytes(Number(v))} />} />
                <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
                <Area type="monotone" dataKey="heapMax"  name="heap max"  stroke="var(--color-heapMax)"  fill="transparent"               strokeDasharray="4 3" strokeWidth={1}   dot={false} isAnimationActive={false} />
                <Area type="monotone" dataKey="heapUsed" name="heap used" stroke="var(--color-heapUsed)" fill="var(--color-heapUsed)" fillOpacity={0.12} strokeWidth={1.5} dot={false} isAnimationActive={false} />
              </AreaChart>
            </ChartContainer>
          </Card>

          <Card title="Active Replays" subtitle="concurrent replay-to-topic operations"
            description="Number of in-flight replay-to-topic operations currently running. Each replay reads from DuckLake and produces back to Kafka. Multiple concurrent replays share the same DuckDB read path (concurrent reads are safe) but each holds a Kafka producer."
          >
            <ChartContainer config={replaysConfig} className="h-[180px] w-full">
              <AreaChart data={pts}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
                <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
                <YAxis {...axisProps} allowDecimals={false} width={28} />
                <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} />} />
                <Area type="stepAfter" dataKey="activeReplays" name="active replays" stroke="var(--color-activeReplays)" fill="var(--color-activeReplays)/20" strokeWidth={1.5} dot={false} isAnimationActive={false} />
              </AreaChart>
            </ChartContainer>
          </Card>

        </div>
      </div>
    </Layout>
  )
}

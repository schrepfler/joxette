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

function getSample(family: Record<string, MetricFamily>, name: string, labelFilter: Record<string, string> = {}): number {
  const f = family[name]; if (!f) return NaN
  for (const s of f.samples) {
    if (s.labels.__name__ !== name) continue
    if (Object.entries(labelFilter).every(([k, v]) => s.labels[k] === v)) return s.value
  }
  return NaN
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

  for (const topic of topicNames) {
    const k = safeKey(topic)
    topicKeys.push(k); topicLabels[k] = topic
    lag[k]          = getSample(family, 'joxette_consumer_lag',           { topic }) || 0
    committedLag[k] = getSample(family, 'joxette_consumer_committed_lag', { topic }) || 0
    consumed[k]     = getSample(family, 'joxette_messages_consumed_total',             { topic }) || 0
    written[k]      = getSample(family, 'joxette_messages_written_total',               { topic }) || 0
    consumedRate[k] = getSample(family, 'joxette_kafka_consumer_records_consumed_rate', { topic }) || 0
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

  // Per-partition metrics keyed by base topic name
  const partitionLag         = getSamplesByTopicAndPartition(family, 'joxette_consumer_lag')
  const partitionConsumedRate = getSamplesByTopicAndPartition(family, 'joxette_kafka_consumer_records_consumed_rate')

  return {
    ts: Date.now(), topicKeys, topicLabels,
    lag, committedLag, consumed, written, consumedRate, bytesRate, fetchLatency,
    pollDurationP50, pollDurationP99, fetchLatencyMax, fetchThrottle, networkIoRate,
    partitionLag, partitionConsumedRate,
    writeDepth: getSample(family, 'joxette_write_channel_depth') || 0,
    writeDuration: (getSample(family, 'joxette_write_duration_seconds_sum') /
                   (getSample(family, 'joxette_write_duration_seconds_count') || 1)) * 1000,
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
// Stat pill
// ---------------------------------------------------------------------------

function Stat({ label, value, sub, title }: { label: string; value: string; sub?: string; title?: string }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', minWidth: 110 }} title={title}>
      <span style={{ fontSize: '0.5625rem', color: 'var(--ink-tertiary)', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 600 }}>{label}</span>
      <span style={{ fontFamily: 'var(--font-mono)', fontSize: '1.125rem', fontWeight: 700, color: 'var(--ink-primary)', lineHeight: 1.3 }}>{value}</span>
      {sub && <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>{sub}</span>}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart card
// ---------------------------------------------------------------------------

function Card({ title, subtitle, children }: { title: string; subtitle?: string; children: React.ReactNode }) {
  return (
    <div style={{ ...cardStyle, padding: '16px 20px' }}>
      <div style={{ marginBottom: 10 }}>
        <span style={{ fontWeight: 600, fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-primary)' }}>{title}</span>
        {subtitle && <span style={{ marginLeft: 8, fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>{subtitle}</span>}
      </div>
      {children}
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

  // Collect stable partition list across all topics (e.g. ["0","1","2"])
  const allPartitions = [...new Set(
    Object.values(latest?.partitionLag ?? {}).flatMap(parts => Object.keys(parts))
  )].sort((a, b) => Number(a) - Number(b))

  // Per-partition stacked lag series: each point has p0, p1, p2 ...
  const lagStackedSeries = pts.map(pt => {
    const row: Record<string, string | number> = { ts: pt.ts }
    // sum across all topics for each partition
    for (const p of allPartitions) {
      let sum = 0
      for (const parts of Object.values(pt.partitionLag ?? {})) sum += parts[p] ?? 0
      row[`p${p}`] = sum
    }
    row['total'] = allPartitions.reduce((acc, p) => acc + (Number(row[`p${p}`]) || 0), 0)
    return row
  })

  // Per-partition stacked consume-rate series
  const rateStackedSeries = history.map((pt, i) => {
    const row: Record<string, string | number> = { ts: String(pt.ts) }
    if (i === 0) { allPartitions.forEach(p => { row[`p${p}`] = 0 }); row['total'] = 0; return row }
    const prev = history[i - 1]
    const dtSec = (pt.ts - prev.ts) / 1000
    let total = 0
    for (const p of allPartitions) {
      let sum = 0
      for (const parts of Object.values(pt.partitionConsumedRate ?? {})) {
        sum += parts[p] ?? 0
      }
      row[`p${p}`] = dtSec > 0 ? Math.max(0, sum) : 0
      total += Number(row[`p${p}`])
    }
    row['total'] = total
    return row
  })

  // Build ChartConfig for partition stacks
  function partitionConfig(partitions: string[]): ChartConfig {
    const cfg: ChartConfig = { total: { label: 'total', color: '#6674cc' } }
    partitions.forEach((p, i) => {
      cfg[`p${p}`] = { label: `partition ${p}`, color: PALETTE[(i + 1) % PALETTE.length] }
    })
    return cfg
  }
  const pLagConfig  = partitionConfig(allPartitions)
  const pRateConfig = partitionConfig(allPartitions)

  // Build ChartConfig for shadcn chart theming
  function topicConfig(keys: string[], labels: Record<string, string>): ChartConfig {
    const cfg: ChartConfig = {}
    keys.forEach((k, i) => {
      cfg[k] = { label: labels[k] ?? k, color: PALETTE[i % PALETTE.length] }
    })
    return cfg
  }

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

  const tConfig = topicConfig(topicKeys, topicLabels)

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
          {/* Total across all topics/partitions */}
          {topicKeys.length > 0 && (() => {
            const fetchLag     = topicKeys.reduce((s, k) => s + (latest.lag[k] ?? 0), 0)
            const committed    = topicKeys.reduce((s, k) => s + (latest.committedLag[k] ?? 0), 0)
            const totalRate    = topicKeys.reduce((s, k) => s + (latest.consumedRate[k] ?? 0), 0)
            return (
              <>
                <Stat label="fetch lag"     value={fmt(fetchLag, 0)}  sub="position-based" title="Messages fetched but not yet consumed by the KafkaConsumer. Updated every poll cycle." />
                <Stat label="committed lag" value={committed > 0 ? fmt(committed, 0) : '—'} sub={`${fmt(totalRate, 1)} msg/s · 15 s cache`} title="endOffset − committedOffset. Matches what Redpanda/Kafka console shows. Updated every 15 s." />
              </>
            )
          })()}
          {/* Per-partition breakdown */}
          {allPartitions.length > 0 && allPartitions.map(p => {
            const pLag  = Object.values(latest.partitionLag  ?? {}).reduce((s, parts) => s + (parts[p] ?? 0), 0)
            const pRate = Object.values(latest.partitionConsumedRate ?? {}).reduce((s, parts) => s + (parts[p] ?? 0), 0)
            return (
              <Stat key={p} label={`lag · p${p}`}
                value={fmt(pLag, 0)}
                sub={`${fmt(pRate, 1)} msg/s`} />
            )
          })}
          {topicKeys.length > 0 && <div style={{ width: 1, background: 'var(--rule)', alignSelf: 'stretch' }} />}
          <Stat label="write depth"   value={String(latest.writeDepth)}    sub="in-flight batches" />
          <Stat label="write latency" value={fmtMs(latest.writeDuration)}  sub="avg batch" />
          {topicKeys.length > 0 && (() => {
            // poll p99 across all topics — near 100ms = broker-gated, near 0 = local-bound
            const maxP99 = topicKeys.reduce((m, k) => Math.max(m, latest.pollDurationP99[k] ?? 0), 0)
            const maxThrottle = topicKeys.reduce((m, k) => Math.max(m, latest.fetchThrottle[k] ?? 0), 0)
            return (
              <Stat label="poll p99" value={fmtMs(maxP99)}
                sub={maxThrottle > 1 ? `throttled ${fmtMs(maxThrottle)}` : 'no throttle'} />
            )
          })()}
          <Stat label="catalog"       value={fmtBytes(latest.catalogBytes)} sub={`${fmtBytes(latest.inlinedBytes)} inlined`} />
          <Stat label="replays"       value={String(latest.activeReplays)} sub="active" />
          <Stat label="heap"          value={fmtBytes(latest.heapUsed)}    sub={`of ${fmtBytes(latest.heapMax)}`} />
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(480px, 1fr))', gap: 20 }}>

        {/* Consumer Lag — stacked per partition + total */}
        <Card title="Consumer Lag" subtitle="stacked per partition · top line = total">
          <ChartContainer config={pLagConfig} className="h-[200px] w-full">
            <AreaChart data={lagStackedSeries} stackOffset="none">
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={v => fmt(v, 0)} {...axisProps} width={44} />
              <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} formatter={(v: unknown) => fmt(Number(v), 0)} />} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              {allPartitions.map((p, i) => (
                <Area key={p} type="monotone" dataKey={`p${p}`} name={`partition ${p}`}
                  stackId="lag"
                  stroke={PALETTE[(i + 1) % PALETTE.length]}
                  fill={PALETTE[(i + 1) % PALETTE.length] + '55'}
                  strokeWidth={1.5} dot={false} isAnimationActive={false} />
              ))}
            </AreaChart>
          </ChartContainer>
        </Card>

        {/* Consume Rate — stacked per partition + total */}
        <Card title="Consume Rate" subtitle="stacked per partition · top = total msg/s">
          <ChartContainer config={pRateConfig} className="h-[200px] w-full">
            <AreaChart data={rateStackedSeries} stackOffset="none">
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={v => fmt(v, 0)} {...axisProps} width={44} />
              <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} formatter={(v: unknown) => `${fmt(Number(v), 1)}/s`} />} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              {allPartitions.map((p, i) => (
                <Area key={p} type="monotone" dataKey={`p${p}`} name={`partition ${p}`}
                  stackId="rate"
                  stroke={PALETTE[(i + 1) % PALETTE.length]}
                  fill={PALETTE[(i + 1) % PALETTE.length] + '55'}
                  strokeWidth={1.5} dot={false} isAnimationActive={false} />
              ))}
            </AreaChart>
          </ChartContainer>
        </Card>

        {/* Bytes consumed rate */}
        <Card title="Bytes Consumed Rate" subtitle="bytes / s from broker">
          <ChartContainer config={tConfig} className="h-[200px] w-full">
            <LineChart data={pts}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={v => fmtBytes(v) + '/s'} {...axisProps} width={72} />
              <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} formatter={(v: unknown) => fmtBytes(Number(v)) + '/s'} />} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              {topicKeys.map((k, i) => (
                <Line key={k} type="monotone" dataKey={`bytesRate.${k}`} name={topicLabels[k] ?? k}
                  stroke={PALETTE[i % PALETTE.length]} strokeWidth={1.5} dot={false} isAnimationActive={false} />
              ))}
            </LineChart>
          </ChartContainer>
        </Card>

        {/* Write pipeline */}
        <Card title="Write Pipeline" subtitle="channel depth & batch latency">
          <ChartContainer config={writeConfig} className="h-[200px] w-full">
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

        {/* Fetch latency */}
        <Card title="Fetch Latency" subtitle="avg Kafka fetch round-trip (ms)">
          <ChartContainer config={tConfig} className="h-[200px] w-full">
            <LineChart data={pts}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={v => v + 'ms'} {...axisProps} width={44} />
              <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} formatter={(v: unknown) => fmtMs(Number(v))} />} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              {topicKeys.map((k, i) => (
                <Line key={k} type="monotone" dataKey={`fetchLatency.${k}`} name={topicLabels[k] ?? k}
                  stroke={PALETTE[i % PALETTE.length]} strokeWidth={1.5} dot={false} isAnimationActive={false} />
              ))}
            </LineChart>
          </ChartContainer>
        </Card>

        {/* Catalog storage */}
        <Card title="Catalog Storage" subtitle="catalog file · inlined data">
          <ChartContainer config={catalogConfig} className="h-[200px] w-full">
            <AreaChart data={pts}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={fmtBytes} {...axisProps} width={68} />
              <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} formatter={(v: unknown) => fmtBytes(Number(v))} />} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              <Area type="monotone" dataKey="catalogBytes" name="catalog file" stroke="var(--color-catalogBytes)" fill="var(--color-catalogBytes)/20" strokeWidth={1.5} dot={false} isAnimationActive={false} />
              <Area type="monotone" dataKey="inlinedBytes" name="inlined"       stroke="var(--color-inlinedBytes)" fill="var(--color-inlinedBytes)/20" strokeWidth={1.5} dot={false} isAnimationActive={false} />
            </AreaChart>
          </ChartContainer>
        </Card>

        {/* JVM Heap */}
        <Card title="JVM Heap" subtitle="used · max">
          <ChartContainer config={heapConfig} className="h-[200px] w-full">
            <AreaChart data={pts}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={fmtBytes} {...axisProps} width={68} />
              <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} formatter={(v: unknown) => fmtBytes(Number(v))} />} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              <Area type="monotone" dataKey="heapMax"  name="heap max"  stroke="var(--color-heapMax)"  fill="transparent"               strokeDasharray="4 3" strokeWidth={1}   dot={false} isAnimationActive={false} />
              <Area type="monotone" dataKey="heapUsed" name="heap used" stroke="var(--color-heapUsed)" fill="var(--color-heapUsed)/15" strokeWidth={1.5} dot={false} isAnimationActive={false} />
            </AreaChart>
          </ChartContainer>
        </Card>

        {/* Active replays */}
        <Card title="Active Replays" subtitle="concurrent replay-to-topic operations">
          <ChartContainer config={replaysConfig} className="h-[200px] w-full">
            <AreaChart data={pts}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis {...axisProps} allowDecimals={false} width={28} />
              <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} />} />
              <Area type="stepAfter" dataKey="activeReplays" name="active replays" stroke="var(--color-activeReplays)" fill="var(--color-activeReplays)/20" strokeWidth={1.5} dot={false} isAnimationActive={false} />
            </AreaChart>
          </ChartContainer>
        </Card>

        {/* Network Diagnostics — poll() latency vs fetch-latency-max */}
        {(() => {
          const netConfig: ChartConfig = {}
          topicKeys.forEach((k, i) => {
            netConfig[`poll50_${k}`] = { label: `${topicLabels[k]} poll p50`, color: PALETTE[i % PALETTE.length] }
            netConfig[`poll99_${k}`] = { label: `${topicLabels[k]} poll p99`, color: PALETTE[i % PALETTE.length] }
            netConfig[`flmax_${k}`]  = { label: `${topicLabels[k]} fetch max`, color: PALETTE[i % PALETTE.length] }
          })
          const netPts = history.map(dp => {
            const row: Record<string, string | number> = { ts: String(dp.ts) }
            topicKeys.forEach(k => {
              row[`poll50_${k}`] = dp.pollDurationP50[k] ?? 0
              row[`poll99_${k}`] = dp.pollDurationP99[k] ?? 0
              row[`flmax_${k}`]  = dp.fetchLatencyMax[k] ?? 0
            })
            return row
          })
          return (
            <Card title="Network Diagnostics"
              subtitle="poll() p50/p99 + Kafka fetch-latency-max · near 100ms = broker-bound">
              <ChartContainer config={netConfig} className="h-[200px] w-full">
                <LineChart data={netPts}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
                  <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
                  <YAxis tickFormatter={v => fmtMs(v)} {...axisProps} width={56}
                    domain={[0, 120]} />
                  <ChartTooltip content={<ChartTooltipContent labelFormatter={v => timeTick(Number(v))} formatter={(v: unknown) => fmtMs(Number(v))} />} />
                  <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
                  {topicKeys.map((k, i) => (
                    <Line key={`p50-${k}`} type="monotone" dataKey={`poll50_${k}`}
                      name={`${topicLabels[k] ?? k} poll p50`}
                      stroke={PALETTE[i % PALETTE.length]} strokeWidth={2} dot={false} isAnimationActive={false} />
                  ))}
                  {topicKeys.map((k, i) => (
                    <Line key={`p99-${k}`} type="monotone" dataKey={`poll99_${k}`}
                      name={`${topicLabels[k] ?? k} poll p99`}
                      stroke={PALETTE[i % PALETTE.length]} strokeWidth={1} strokeDasharray="4 2" dot={false} isAnimationActive={false} />
                  ))}
                  {topicKeys.map((k, i) => (
                    <Line key={`flmax-${k}`} type="monotone" dataKey={`flmax_${k}`}
                      name={`${topicLabels[k] ?? k} fetch max`}
                      stroke={PALETTE[i % PALETTE.length]} strokeWidth={1} strokeDasharray="1 3" dot={false} isAnimationActive={false} />
                  ))}
                </LineChart>
              </ChartContainer>
            </Card>
          )
        })()}

      </div>
    </Layout>
  )
}

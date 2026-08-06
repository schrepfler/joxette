import { createFileRoute } from '@tanstack/react-router'
import { memo, useEffect, useId, useMemo, useState } from 'react'
import {
  AreaChart, Area, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Legend, ReferenceLine, ReferenceDot, Tooltip,
} from 'recharts'
import { ChartContainer, type ChartConfig } from '@/components/ui/chart'
import { healthApi } from '../../api/client'
import { Layout } from '../../components/Layout'
import { pageTitle, cardStyle } from '../../styles/shared'
import { useVisibilityAwareInterval } from '../../hooks/useVisibilityAwareInterval'
import {
  computeLagSeries, computeRateSeries, computeNetPts,
  type DataPoint, type PtRecord, type MetricFamily,
} from '../../lib/metricsDerive'

export const Route = createFileRoute('/metrics/')({ component: MetricsPage })

// ---------------------------------------------------------------------------
// Prometheus text-format parser
// ---------------------------------------------------------------------------

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

function getSamplesByTopicAndPartition(family: Record<string, MetricFamily>, name: string): Record<string, Record<string, number>> {
  const f = family[name]; if (!f) return {}
  const result: Record<string, Record<string, number>> = {}
  for (const s of f.samples) {
    if (s.labels.__name__ !== name) continue
    const topic = s.labels['topic'] ?? 'unknown'
    const partition = s.labels['partition']
    if (!partition) continue
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

  const partitionLag          = getSamplesByTopicAndPartition(family, 'joxette_consumer_lag')
  const partitionConsumedRate = getSamplesByTopicAndPartition(family, 'joxette_kafka_consumer_records_consumed_rate')

  for (const topic of topicNames) {
    const k = safeKey(topic)
    topicKeys.push(k); topicLabels[k] = topic
    lag[k]          = Object.values(partitionLag[topic] ?? {}).reduce((a, b) => a + b, 0)
                      || getSample(family, 'joxette_consumer_lag', { topic }) || 0
    committedLag[k] = getSample(family, 'joxette_consumer_committed_lag', { topic }) || 0
    consumed[k]     = getSample(family, 'joxette_messages_consumed_total',             { topic }) || 0
    written[k]      = getSample(family, 'joxette_messages_written_total',               { topic }) || 0
    consumedRate[k] = Object.values(partitionConsumedRate[topic] ?? {}).reduce((a, b) => a + b, 0)
                      || getSample(family, 'joxette_kafka_consumer_records_consumed_rate', { topic }) || 0
    bytesRate[k]    = getSample(family, 'joxette_kafka_consumer_bytes_consumed_rate',   { topic }) || 0
    fetchLatency[k] = getSample(family, 'joxette_kafka_consumer_fetch_latency_avg',     { topic }) || 0
    const pollP50Raw = getSample(family, 'joxette_poll_duration_seconds', { topic, quantile: '0.5' })
    const pollP99Raw = getSample(family, 'joxette_poll_duration_seconds', { topic, quantile: '0.99' })
    pollDurationP50[k] = isNaN(pollP50Raw) ? 0 : pollP50Raw * 1000
    pollDurationP99[k] = isNaN(pollP99Raw) ? 0 : pollP99Raw * 1000
    fetchLatencyMax[k] = getSample(family, 'joxette_kafka_consumer_fetch_latency_max',       { topic }) || 0
    fetchThrottle[k]   = getSample(family, 'joxette_kafka_consumer_fetch_throttle_time_avg', { topic }) || 0
    networkIoRate[k]   = getSample(family, 'joxette_kafka_consumer_network_io_rate',         { topic }) || 0
  }

  function sumHeapUsed(): number {
    const f = familyFor(family, 'jvm_memory_used_bytes')
    if (!f) return 0
    let total = 0
    for (const s of f.samples) {
      if (s.labels.__name__ !== 'jvm_memory_used_bytes') continue
      if (s.labels['area'] === 'heap' && s.value > 0) total += s.value
    }
    return total
  }
  const heapUsed = sumHeapUsed()
  const heapMax  = getSample(family, 'joxette_jvm_heap_max_bytes') || 0

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
    flushedBytes: getSample(family, 'joxette_catalog_flushed_bytes') || 0,
    duckdbMemoryTotal: getSample(family, 'joxette_duckdb_memory_total_bytes') || 0,
    duckdbMemoryByTag: getSamplesBy(family, 'joxette_duckdb_memory_bytes', 'tag'),
    activeReplays: getSample(family, 'joxette_replay_active') || 0,
    heapUsed: Math.max(0, heapUsed),
    heapMax:  Math.max(0, heapMax),
    processRss: getSample(family, 'joxette_process_rss_bytes') || 0,
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
// Cursor-following value label rendered as a Recharts activeDot.
// Shows a dot + paint-order outlined text at the hovered data point.
// Pair with <Tooltip content={() => null} /> which keeps the cursor line and
// activeDot machinery running without rendering a tooltip popup box.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Dot + paint-order-outlined value text — the shared visual for "show a
// value at this point on the chart". Used both by the cursor-following
// activeDot (makeActiveDot) and by LegendHoverDot (value at a series' last
// point, shown on legend hover instead of on cursor movement).
// ---------------------------------------------------------------------------

function ValueLabel({ cx, cy, color, text }: { cx?: number | null; cy?: number | null; color?: string; text: string }) {
  if (cx == null || cy == null) return <g />
  return (
    <g>
      <circle cx={cx} cy={cy} r={4} fill={color ?? '#6674cc'} stroke="rgba(10,12,22,0.7)" strokeWidth={1.5} />
      <text
        x={cx + 8}
        y={cy + 4}
        fontSize={10}
        fontFamily="var(--font-mono)"
        fill="rgba(10,12,22,0.9)"
        style={{
          paintOrder: 'stroke',
          stroke: 'rgba(255,255,255,0.85)',
          strokeWidth: 5,
          strokeLinejoin: 'round',
        } as React.CSSProperties}
      >
        {text}
      </text>
    </g>
  )
}

type ActiveDotProps = {
  cx?: number | null
  cy?: number | null
  stroke?: string
  value?: number | number[]
}

function makeActiveDot(formatter: (v: number) => string) {
  return function ActiveDot({ cx, cy, stroke, value }: ActiveDotProps) {
    // For stacked areas value may arrive as [base, top]; show the contribution.
    const raw = Array.isArray(value)
      ? (value as number[])[1] - (value as number[])[0]
      : (value ?? 0)
    return <ValueLabel cx={cx} cy={cy} color={stroke} text={formatter(Number(raw))} />
  }
}

// Shared cursor style for all charts — a subtle dashed vertical line.
const CURSOR_STYLE = { stroke: 'rgba(200,205,240,0.35)', strokeWidth: 1, strokeDasharray: '3 2' }

// ---------------------------------------------------------------------------
// Interactive legend — hovering a series name highlights that series (dims
// the rest, via `emphasis` below on each Area/Line) without moving any
// label. Pair with useState<string | null>(null) per chart, and render one
// <LegendHoverDot> per series to show its last value at its actual point on
// the chart, the same way the cursor-following activeDot does.
// ---------------------------------------------------------------------------

type LegendPayloadEntry = { value?: string; color?: string; dataKey?: string | number }

function InteractiveLegend({
  payload, activeKey, onHover,
}: {
  payload?: LegendPayloadEntry[]
  activeKey: string | null
  onHover: (key: string | null) => void
}) {
  if (!payload || payload.length === 0) return null
  return (
    <ul style={{
      display: 'flex', flexWrap: 'wrap', justifyContent: 'center', gap: '4px 14px',
      listStyle: 'none', margin: '8px 0 0', padding: 0,
    }}>
      {payload.map(entry => {
        const key = String(entry.dataKey ?? entry.value ?? '')
        const active = activeKey === key
        const dimmed = activeKey !== null && !active
        return (
          <li key={key}
            onMouseEnter={() => onHover(key)}
            onMouseLeave={() => onHover(null)}
            style={{
              display: 'flex', alignItems: 'center', gap: 5, cursor: 'default',
              fontSize: '0.75rem', fontFamily: 'var(--font-mono)',
              color: dimmed ? 'var(--ink-tertiary)' : 'var(--ink-secondary)',
              opacity: dimmed ? 0.5 : 1,
              transition: 'opacity 120ms ease, color 120ms ease',
            }}
          >
            <span style={{ width: 8, height: 8, borderRadius: 2, background: entry.color, flexShrink: 0 }} />
            <span style={{ fontWeight: active ? 700 : 400 }}>{entry.value}</span>
          </li>
        )
      })}
    </ul>
  )
}

/**
 * Renders a ValueLabel at a series' last data point — only while that
 * series is the hovered legend entry. Mirrors what the cursor-following
 * activeDot shows, but anchored to the last point instead of the cursor,
 * so hovering the legend doesn't require moving the mouse onto the chart.
 */
function LegendHoverDot({
  seriesKey, activeKey, data, formatter, color, yAxisId, positionAt,
}: {
  seriesKey: string
  activeKey: string | null
  data: readonly unknown[]
  formatter: (v: number) => string
  color: string
  yAxisId?: string
  /**
   * Y position on the chart, given the last data row. Defaults to
   * `row[seriesKey]`. Stacked areas need the cumulative stack height here
   * instead — the displayed value stays the series' own (non-cumulative)
   * contribution, matching what the cursor-following activeDot shows.
   */
  positionAt?: (last: Record<string, unknown>) => number
}) {
  if (activeKey !== seriesKey) return null
  const last = data[data.length - 1] as Record<string, unknown> | undefined
  const x = last?.ts as string | number | undefined
  const raw = last ? Number(last[seriesKey]) : NaN
  const y = last ? (positionAt ? positionAt(last) : raw) : NaN
  if (x == null || isNaN(raw) || isNaN(y)) return null
  return (
    <ReferenceDot x={x} y={y} yAxisId={yAxisId} r={0} ifOverflow="extendDomain"
      shape={(props: { cx?: number; cy?: number }) => <ValueLabel cx={props.cx} cy={props.cy} color={color} text={formatter(raw)} />} />
  )
}

/** fillOpacity/strokeOpacity for one Area/Line, dimmed when another series is hovered. */
function emphasis(key: string, activeKey: string | null, fillOpacity: number) {
  const dimmed = activeKey !== null && activeKey !== key
  return {
    fillOpacity: dimmed ? fillOpacity * 0.25 : fillOpacity,
    strokeOpacity: dimmed ? 0.3 : 1,
  }
}

// ---------------------------------------------------------------------------
// Tooltip bubble (shared, used by Stat pills and Card headers only)
// ---------------------------------------------------------------------------

const TIP_STYLE: React.CSSProperties = {
  position: 'absolute', bottom: 'calc(100% + 8px)', left: 0,
  zIndex: 200,
  background: 'var(--surface-raised)',
  border: '1px solid var(--rule-strong)',
  borderRadius: 6,
  padding: '8px 11px',
  width: 260,
  fontSize: '0.72rem',
  color: 'var(--ink-primary)',
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
  const tipId = useId()
  return (
    <div
      style={{ display: 'flex', flexDirection: 'column', minWidth: 110, position: 'relative', cursor: title ? 'default' : undefined }}
      tabIndex={title ? 0 : undefined}
      aria-describedby={title ? tipId : undefined}
      onMouseEnter={() => title && setTip(true)}
      onMouseLeave={() => setTip(false)}
      onFocus={() => title && setTip(true)}
      onBlur={() => setTip(false)}
    >
      <span style={{ fontSize: '0.5625rem', color: 'var(--ink-tertiary)', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 600 }}>
        {label}
      </span>
      <span style={{ fontFamily: 'var(--font-mono)', fontSize: '1.125rem', fontWeight: 700, color: 'var(--ink-primary)', lineHeight: 1.3 }}>{value}</span>
      {sub && <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>{sub}</span>}
      {tip && title && <div id={tipId} role="tooltip" style={TIP_STYLE}>{title}</div>}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chart card
// ---------------------------------------------------------------------------

function Card({ title, subtitle, description, children }: { title: string; subtitle?: string; description?: string; children: React.ReactNode }) {
  const [tip, setTip] = useState(false)
  const tipId = useId()
  return (
    <div style={{ ...cardStyle, padding: '16px 20px' }}>
      <div style={{ marginBottom: 10, display: 'flex', alignItems: 'baseline', gap: 0, position: 'relative' }}>
        <h3
          style={{ margin: 0, fontWeight: 600, fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-primary)', cursor: description ? 'default' : undefined, borderBottom: description ? '1px dotted var(--rule-strong)' : undefined }}
          tabIndex={description ? 0 : undefined}
          aria-describedby={description ? tipId : undefined}
          onMouseEnter={() => description && setTip(true)}
          onMouseLeave={() => setTip(false)}
          onFocus={() => description && setTip(true)}
          onBlur={() => setTip(false)}
        >{title}</h3>
        {subtitle && <span style={{ marginLeft: 8, fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>{subtitle}</span>}
        {tip && description && <div id={tipId} role="tooltip" style={TIP_STYLE}>{description}</div>}
      </div>
      {children}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Per-topic chart row
// ---------------------------------------------------------------------------

const TopicRow = memo(function TopicRow({ tk, label, latest, pts, axisProps }: {
  tk: string
  label: string
  latest: DataPoint | undefined
  pts: PtRecord[]
  axisProps: { tick: { fontSize: number; fill: string }; axisLine: boolean; tickLine: boolean }
}) {
  const partitions = useMemo(
    () => Object.keys(latest?.partitionLag?.[label] ?? {}).sort((a, b) => Number(a) - Number(b)),
    [latest, label],
  )

  const lagSeries = useMemo(() => computeLagSeries(pts, partitions, label), [pts, partitions, label])
  const rateSeries = useMemo(() => computeRateSeries(pts, partitions, label, tk), [pts, partitions, label, tk])
  const netPts = useMemo(() => computeNetPts(pts, tk), [pts, tk])

  const pConfig: ChartConfig = {}
  partitions.forEach((p, i) => {
    pConfig[`p${p}`] = { label: `p${p}`, color: PALETTE[i % PALETTE.length] }
  })
  const rateConfig: ChartConfig = {
    ...pConfig,
    total: { label: 'total', color: '#f0c040' },
  }

  const netConfig: ChartConfig = {
    p50:  { label: 'poll p50',  color: PALETTE[0] },
    p99:  { label: 'poll p99',  color: PALETTE[1] },
    flmax: { label: 'fetch max', color: PALETTE[2] },
  }

  const currentLag  = latest ? Object.values(latest.partitionLag?.[label] ?? {}).reduce((a, b) => a + b, 0) : 0
  const currentRate = latest?.consumedRate[tk] ?? 0

  const lagDot  = makeActiveDot(v => fmt(v, 0))
  const rateDot = makeActiveDot(v => `${fmt(v, 1)}/s`)
  const msDot   = makeActiveDot(fmtMs)

  const [lagActive, setLagActive] = useState<string | null>(null)
  const [rateActive, setRateActive] = useState<string | null>(null)
  const [netActive, setNetActive] = useState<string | null>(null)

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
            <AreaChart syncId="metrics" data={lagSeries} stackOffset="none" margin={{ right: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={v => fmt(v, 0)} {...axisProps} width={44} />
              <Tooltip content={() => null} cursor={CURSOR_STYLE} />
              <Legend content={<InteractiveLegend activeKey={lagActive} onHover={setLagActive} />} />
              {partitions.map((p, i) => (
                <Area key={p} type="monotone" dataKey={`p${p}`} name={`p${p}`}
                  stackId="lag"
                  stroke={PALETTE[i % PALETTE.length]}
                  fill={PALETTE[i % PALETTE.length]}
                  strokeWidth={1.5} dot={false} activeDot={lagDot} isAnimationActive={false}
                  {...emphasis(`p${p}`, lagActive, 0.33)} />
              ))}
              {partitions.map((p, i) => (
                <LegendHoverDot key={p} seriesKey={`p${p}`} activeKey={lagActive} data={lagSeries}
                  formatter={v => fmt(v, 0)} color={PALETTE[i % PALETTE.length]}
                  positionAt={last => partitions.slice(0, i + 1)
                    .reduce((sum, pp) => sum + (Number(last[`p${pp}`]) || 0), 0)} />
              ))}
            </AreaChart>
          </ChartContainer>
        </Card>

        <Card title="Consume Rate" subtitle="msg/s per partition"
          description="Messages consumed per second from the broker, broken down by partition. The dashed white line is the cumulative total across all partitions. A drop here while lag rises means the consumer is stalling.">
          <ChartContainer config={rateConfig} className="h-[180px] w-full">
            <LineChart syncId="metrics" data={rateSeries} margin={{ right: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={v => fmt(v, 0)} {...axisProps} width={44} />
              <Tooltip content={() => null} cursor={CURSOR_STYLE} />
              <Legend content={<InteractiveLegend activeKey={rateActive} onHover={setRateActive} />} />
              {partitions.map((p, i) => (
                <Line key={p} type="monotone" dataKey={`p${p}`} name={`p${p}`}
                  stroke={PALETTE[i % PALETTE.length]}
                  strokeWidth={1.5} dot={false} activeDot={rateDot} isAnimationActive={false}
                  {...emphasis(`p${p}`, rateActive, 0)} />
              ))}
              <Line type="monotone" dataKey="total" name="total"
                stroke="#f0c040" strokeWidth={2} strokeDasharray="5 3"
                dot={false} activeDot={makeActiveDot(v => `${fmt(v, 1)}/s`)} isAnimationActive={false}
                {...emphasis('total', rateActive, 0)} />
              {partitions.map((p, i) => (
                <LegendHoverDot key={p} seriesKey={`p${p}`} activeKey={rateActive} data={rateSeries}
                  formatter={v => `${fmt(v, 1)}/s`} color={PALETTE[i % PALETTE.length]} />
              ))}
              <LegendHoverDot seriesKey="total" activeKey={rateActive} data={rateSeries}
                formatter={v => `${fmt(v, 1)}/s`} color="#f0c040" />
            </LineChart>
          </ChartContainer>
        </Card>

        <Card title="Bytes Rate" subtitle="bytes / s from broker"
          description="Raw bytes fetched per second from the broker for this topic. Useful for estimating network bandwidth and DuckLake write pressure. Includes message overhead, not just payload size.">
          <ChartContainer config={{ val: { label: 'bytes/s', color: PALETTE[0] } }} className="h-[180px] w-full">
            <LineChart syncId="metrics" data={pts.map(pt => ({ ts: pt.ts, val: pt.bytesRate[tk] ?? 0 }))} margin={{ right: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={v => fmtBytes(v) + '/s'} {...axisProps} width={72} />
              <Tooltip content={() => null} cursor={CURSOR_STYLE} />
              <Line type="monotone" dataKey="val" name="bytes/s"
                stroke={PALETTE[0]} strokeWidth={1.5} dot={false}
                activeDot={makeActiveDot(v => fmtBytes(v) + '/s')} isAnimationActive={false} />
            </LineChart>
          </ChartContainer>
        </Card>

        <Card title="Fetch Latency" subtitle="avg Kafka fetch round-trip (ms)"
          description="Average time between sending a FETCH request to the broker and receiving a response. On a quiet topic this will sit near fetch.max.wait.ms (500 ms) — the broker holds the request until data arrives. High latency on a busy topic indicates broker saturation or network issues.">
          <ChartContainer config={{ val: { label: 'fetch avg', color: PALETTE[1] } }} className="h-[180px] w-full">
            <LineChart syncId="metrics" data={pts.map(pt => ({ ts: pt.ts, val: pt.fetchLatency[tk] ?? 0 }))} margin={{ right: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={v => v + 'ms'} {...axisProps} width={44} />
              <Tooltip content={() => null} cursor={CURSOR_STYLE} />
              <Line type="monotone" dataKey="val" name="fetch avg"
                stroke={PALETTE[1]} strokeWidth={1.5} dot={false}
                activeDot={msDot} isAnimationActive={false} />
            </LineChart>
          </ChartContainer>
        </Card>

        <Card title="Network" subtitle="poll() p50/p99 + fetch-latency-max (ms)"
          description="poll() p50/p99: time spent inside KafkaConsumer.poll(), including broker wait. Near 100 ms means the consumer is broker-bound (waiting on fetch.max.wait.ms). Near 0 ms means local processing is the bottleneck. fetch-latency-max is the worst single fetch round-trip seen in the interval.">
          <ChartContainer config={netConfig} className="h-[180px] w-full">
            <LineChart syncId="metrics" data={netPts} margin={{ right: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
              <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...axisProps} minTickGap={40} />
              <YAxis tickFormatter={v => fmtMs(v)} {...axisProps} width={56} domain={[0, 120]} />
              <Tooltip content={() => null} cursor={CURSOR_STYLE} />
              <Legend content={<InteractiveLegend activeKey={netActive} onHover={setNetActive} />} />
              <Line type="monotone" dataKey="p50" name="poll p50"
                stroke={PALETTE[0]} strokeWidth={2} dot={false} activeDot={msDot} isAnimationActive={false}
                {...emphasis('p50', netActive, 0)} />
              <Line type="monotone" dataKey="p99" name="poll p99"
                stroke={PALETTE[1]} strokeWidth={1} strokeDasharray="4 2"
                dot={false} activeDot={msDot} isAnimationActive={false}
                {...emphasis('p99', netActive, 0)} />
              <Line type="monotone" dataKey="flmax" name="fetch max"
                stroke={PALETTE[2]} strokeWidth={1} strokeDasharray="1 3"
                dot={false} activeDot={msDot} isAnimationActive={false}
                {...emphasis('flmax', netActive, 0)} />
              <LegendHoverDot seriesKey="p50" activeKey={netActive} data={netPts} formatter={fmtMs} color={PALETTE[0]} />
              <LegendHoverDot seriesKey="p99" activeKey={netActive} data={netPts} formatter={fmtMs} color={PALETTE[1]} />
              <LegendHoverDot seriesKey="flmax" activeKey={netActive} data={netPts} formatter={fmtMs} color={PALETTE[2]} />
            </LineChart>
          </ChartContainer>
        </Card>
      </div>
    </div>
  )
})

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

// Hoisted to module scope: this object is fixed and never varies per render
// or per MetricsPage instance. Recreating it inline in MetricsPage's render
// body defeated React.memo on TopicRow (a fresh object literal every render
// fails memo's shallow prop comparison, forcing TopicRow — and its whole
// Recharts subtree — to re-render unconditionally, even on failed-poll
// re-renders that otherwise touch nothing this chart cares about).
const AXIS_PROPS = {
  tick: { fontSize: 10, fill: 'var(--ink-tertiary)' },
  axisLine: false, tickLine: false,
} as const

function MetricsPage() {
  const [history, setHistory] = useState<DataPoint[]>([])
  const [error, setError]     = useState<string | null>(null)
  const [lastTs, setLastTs]   = useState<number | null>(null)

  async function poll() {
    try {
      const text = await healthApi.getMetricsText()
      const family = parsePrometheus(text)
      const point = scrapeToPoint(family)
      setHistory(prev => [...prev.slice(-(MAX_POINTS - 1)), point])
      setLastTs(Date.now()); setError(null)
    } catch (e) { setError(`Failed to fetch metrics: ${(e as Error).message}`) }
  }

  useEffect(() => { void poll() }, [])
  useVisibilityAwareInterval(() => { void poll() }, POLL_MS)

  const latest   = history[history.length - 1]
  const topicKeys   = latest?.topicKeys ?? []
  const topicLabels = latest?.topicLabels ?? {}

  const pts = useMemo(() => history.map(p => ({ ...p, ts: String(p.ts) })), [history])

  const catalogConfig: ChartConfig = {
    catalogBytes: { label: 'catalog file', color: '#6674cc' },
    inlinedBytes: { label: 'inlined',       color: '#A26612' },
    flushedBytes: { label: 'flushed',       color: '#3E9A7A' },
  }
  const heapConfig: ChartConfig = {
    heapUsed:   { label: 'heap used', color: '#1E5A8A' },
    processRss: { label: 'process RSS', color: '#e07040' },
  }
  const duckdbMemTags = latest
    ? Object.keys(latest.duckdbMemoryByTag).filter(t => (latest.duckdbMemoryByTag[t] ?? 0) > 0)
    : []
  const duckdbMemConfig: ChartConfig = Object.fromEntries(
    duckdbMemTags.map((t, i) => [t, { label: t, color: PALETTE[i % PALETTE.length] }])
  )
  const duckdbMemPts = pts.map(pt => ({
    ts: pt.ts,
    total: pt.duckdbMemoryTotal,
    ...Object.fromEntries(duckdbMemTags.map(t => [t, pt.duckdbMemoryByTag[t] ?? 0])),
  }))
  const writeConfig: ChartConfig = {
    writeDepth:    { label: 'depth',    color: '#6674cc' },
    writeDuration: { label: 'batch ms', color: '#3E9A7A' },
  }
  const replaysConfig: ChartConfig = {
    activeReplays: { label: 'active replays', color: '#6B46A0' },
  }

  const bytesDot   = makeActiveDot(fmtBytes)
  const msDot      = makeActiveDot(fmtMs)
  const countDot   = makeActiveDot(v => fmt(v, 0))

  const [writeActive, setWriteActive] = useState<string | null>(null)
  const [catalogActive, setCatalogActive] = useState<string | null>(null)
  const [duckdbMemActive, setDuckdbMemActive] = useState<string | null>(null)
  const [heapActive, setHeapActive] = useState<string | null>(null)

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
          <Stat label="total data" value={fmtBytes(latest.catalogBytes + latest.flushedBytes)}
            sub={`${fmtBytes(latest.catalogBytes)} catalog · ${fmtBytes(latest.flushedBytes)} flushed`}
            title="All cassette data under management: the local DuckDB catalog file (catalog, which includes any not-yet-flushed inlined data) plus everything already flushed to Parquet in object storage." />
          <Stat label="duckdb mem" value={fmtBytes(latest.duckdbMemoryTotal)} sub="allocator total"
            title="Total memory used by DuckDB's internal allocator across all allocation tags (duckdb_memory()). A value that keeps growing without compaction may indicate a memory leak inside the embedded DuckDB instance." />
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
          axisProps={AXIS_PROPS}
        />
      ))}

      {/* Infrastructure charts */}
      <div style={{ marginTop: 8 }}>
        <div style={{ paddingBottom: 6, borderBottom: '1px solid var(--rule)', marginBottom: 12 }}>
          <span style={{ fontWeight: 600, fontSize: '0.6875rem', color: 'var(--ink-secondary)', textTransform: 'uppercase', letterSpacing: '0.06em' }}>
            Infrastructure
          </span>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))', gap: 16 }}>

          <Card title="Write Pipeline" subtitle="channel depth & batch latency"
            description="Depth: number of batches currently queued in the bounded write channel waiting for DuckDB. Batch ms: average time to execute one INSERT batch. A rising depth means DuckDB writes are slower than Kafka consumption — the channel is the backpressure valve."
          >
            <ChartContainer config={writeConfig} className="h-[180px] w-full">
              <LineChart syncId="metrics" data={pts} margin={{ right: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
                <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...AXIS_PROPS} minTickGap={40} />
                <YAxis yAxisId="depth" {...AXIS_PROPS} width={28} />
                <YAxis yAxisId="ms" orientation="right" tickFormatter={v => v + 'ms'} {...AXIS_PROPS} width={48} />
                <Tooltip content={() => null} cursor={CURSOR_STYLE} />
                <Legend content={<InteractiveLegend activeKey={writeActive} onHover={setWriteActive} />} />
                <Line yAxisId="depth" type="monotone" dataKey="writeDepth"    name="depth"    stroke="var(--color-writeDepth)"    strokeWidth={1.5} dot={false} activeDot={countDot} isAnimationActive={false}
                  {...emphasis('writeDepth', writeActive, 0)} />
                <Line yAxisId="ms"    type="monotone" dataKey="writeDuration" name="batch ms" stroke="var(--color-writeDuration)" strokeWidth={1.5} dot={false} activeDot={msDot}   isAnimationActive={false}
                  {...emphasis('writeDuration', writeActive, 0)} />
                <LegendHoverDot seriesKey="writeDepth" activeKey={writeActive} data={pts} yAxisId="depth"
                  formatter={v => fmt(v, 0)} color="var(--color-writeDepth)" />
                <LegendHoverDot seriesKey="writeDuration" activeKey={writeActive} data={pts} yAxisId="ms"
                  formatter={fmtMs} color="var(--color-writeDuration)" />
              </LineChart>
            </ChartContainer>
          </Card>

          <Card title="Catalog Storage" subtitle="catalog file · inlined · flushed"
            description="Catalog file: total size of the DuckDB .ducklake file on disk, including inlined data and metadata. Inlined: bytes currently buffered inside the catalog before being flushed to Parquet on object storage — a subset of the catalog file. Flushed: bytes already written to Parquet in object storage (S3/GCS/Azure), separate from the catalog file. DuckLake flushes automatically when the inline threshold is reached."
          >
            <ChartContainer config={catalogConfig} className="h-[180px] w-full">
              <AreaChart syncId="metrics" data={pts} margin={{ right: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
                <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...AXIS_PROPS} minTickGap={40} />
                <YAxis tickFormatter={fmtBytes} {...AXIS_PROPS} width={68} />
                <Tooltip content={() => null} cursor={CURSOR_STYLE} />
                <Legend content={<InteractiveLegend activeKey={catalogActive} onHover={setCatalogActive} />} />
                <Area type="monotone" dataKey="catalogBytes" name="catalog file" stroke="var(--color-catalogBytes)" fill="var(--color-catalogBytes)" strokeWidth={1.5} dot={false} activeDot={bytesDot} isAnimationActive={false}
                  {...emphasis('catalogBytes', catalogActive, 0.15)} />
                <Area type="monotone" dataKey="inlinedBytes" name="inlined"       stroke="var(--color-inlinedBytes)" fill="var(--color-inlinedBytes)" strokeWidth={1.5} dot={false} activeDot={bytesDot} isAnimationActive={false}
                  {...emphasis('inlinedBytes', catalogActive, 0.15)} />
                <Area type="monotone" dataKey="flushedBytes" name="flushed"       stroke="var(--color-flushedBytes)" fill="var(--color-flushedBytes)" strokeWidth={1.5} dot={false} activeDot={bytesDot} isAnimationActive={false}
                  {...emphasis('flushedBytes', catalogActive, 0.15)} />
                <LegendHoverDot seriesKey="catalogBytes" activeKey={catalogActive} data={pts} formatter={fmtBytes} color="var(--color-catalogBytes)" />
                <LegendHoverDot seriesKey="inlinedBytes" activeKey={catalogActive} data={pts} formatter={fmtBytes} color="var(--color-inlinedBytes)" />
                <LegendHoverDot seriesKey="flushedBytes" activeKey={catalogActive} data={pts} formatter={fmtBytes} color="var(--color-flushedBytes)" />
              </AreaChart>
            </ChartContainer>
          </Card>

          <Card title="DuckDB Memory" subtitle="total · per allocation tag"
            description="Memory used by DuckDB's internal allocator, broken down by tag (BASE_TABLE, HASH_TABLE, PARQUET_READER, ALLOCATOR, etc.). Sourced from duckdb_memory() on every scrape. A steadily growing ALLOCATOR or BASE_TABLE value that does not shrink after compaction may indicate a memory leak inside the embedded DuckDB instance."
          >
            <ChartContainer config={{ total: { label: 'total', color: PALETTE[0] }, ...duckdbMemConfig }} className="h-[180px] w-full">
              <AreaChart syncId="metrics" data={duckdbMemPts} margin={{ right: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
                <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...AXIS_PROPS} minTickGap={40} />
                <YAxis tickFormatter={fmtBytes} {...AXIS_PROPS} width={68} />
                <Tooltip content={() => null} cursor={CURSOR_STYLE} />
                <Legend content={<InteractiveLegend activeKey={duckdbMemActive} onHover={setDuckdbMemActive} />} />
                <Area type="monotone" dataKey="total" name="total"
                  stroke={PALETTE[0]} fill={PALETTE[0]} strokeWidth={2}
                  dot={false} activeDot={bytesDot} isAnimationActive={false}
                  {...emphasis('total', duckdbMemActive, 0.15)} />
                {duckdbMemTags.map((t, i) => (
                  <Area key={t} type="monotone" dataKey={t} name={t}
                    stroke={PALETTE[(i + 1) % PALETTE.length]} fill="none"
                    strokeWidth={1} strokeDasharray="3 2"
                    dot={false} activeDot={bytesDot} isAnimationActive={false}
                    {...emphasis(t, duckdbMemActive, 0)} />
                ))}
                <LegendHoverDot seriesKey="total" activeKey={duckdbMemActive} data={duckdbMemPts} formatter={fmtBytes} color={PALETTE[0]} />
                {duckdbMemTags.map((t, i) => (
                  <LegendHoverDot key={t} seriesKey={t} activeKey={duckdbMemActive} data={duckdbMemPts}
                    formatter={fmtBytes} color={PALETTE[(i + 1) % PALETTE.length]} />
                ))}
              </AreaChart>
            </ChartContainer>
          </Card>

          <Card title="JVM Heap &amp; Process RSS" subtitle="heap used · process RSS · max"
            description="JVM heap used vs max (-Xmx). Process RSS (orange) is the total physical memory used by the process — heap + metaspace + DuckDB native allocations. If RSS grows steadily while heap stays flat, the leak is in DuckDB's C++ allocator or JVM off-heap (metaspace, code cache). Sustained heap above 80% of max warrants a heap increase."
          >
            <ChartContainer config={heapConfig} className="h-[180px] w-full">
              <AreaChart syncId="metrics" data={pts} margin={{ right: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
                <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...AXIS_PROPS} minTickGap={40} />
                <YAxis tickFormatter={fmtBytes} {...AXIS_PROPS} width={68}
                  domain={[0, (latest?.heapMax ?? 0) > 0 ? latest!.heapMax * 1.05 : 'auto']} />
                <Tooltip content={() => null} cursor={CURSOR_STYLE} />
                <Legend content={<InteractiveLegend activeKey={heapActive} onHover={setHeapActive} />} />
                {(latest?.heapMax ?? 0) > 0 && <>
                  <ReferenceLine y={latest!.heapMax} stroke="var(--ink-tertiary)" strokeWidth={1.5} strokeDasharray="6 3"
                    label={{ value: `max  ${fmtBytes(latest!.heapMax)}`, position: 'insideTopLeft', fontSize: 9, fill: 'var(--ink-tertiary)', fontFamily: 'var(--font-mono)' }} />
                  <ReferenceLine y={latest!.heapMax * 0.8} stroke="var(--signal-warn)" strokeWidth={1.5} strokeDasharray="4 3"
                    label={{ value: `80%  ${fmtBytes(latest!.heapMax * 0.8)}`, position: 'insideTopLeft', fontSize: 9, fill: 'var(--signal-warn)', fontFamily: 'var(--font-mono)' }} />
                </>}
                <Area type="monotone" dataKey="heapUsed"   name="heap used"   stroke="var(--color-heapUsed)"   fill="var(--color-heapUsed)"   strokeWidth={1.5} dot={false} activeDot={bytesDot} isAnimationActive={false}
                  {...emphasis('heapUsed', heapActive, 0.12)} />
                <Area type="monotone" dataKey="processRss" name="process RSS" stroke="var(--color-processRss)" fill="none"                     strokeWidth={1.5} strokeDasharray="4 2" dot={false} activeDot={bytesDot} isAnimationActive={false}
                  {...emphasis('processRss', heapActive, 0)} />
                <LegendHoverDot seriesKey="heapUsed" activeKey={heapActive} data={pts} formatter={fmtBytes} color="var(--color-heapUsed)" />
                <LegendHoverDot seriesKey="processRss" activeKey={heapActive} data={pts} formatter={fmtBytes} color="var(--color-processRss)" />
              </AreaChart>
            </ChartContainer>
          </Card>

          <Card title="Active Replays" subtitle="concurrent replay-to-topic operations"
            description="Number of in-flight replay-to-topic operations currently running. Each replay reads from DuckLake and produces back to Kafka. Multiple concurrent replays share the same DuckDB read path (concurrent reads are safe) but each holds a Kafka producer."
          >
            <ChartContainer config={replaysConfig} className="h-[180px] w-full">
              <AreaChart syncId="metrics" data={pts} margin={{ right: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" vertical={false} />
                <XAxis dataKey="ts" tickFormatter={v => timeTick(Number(v))} {...AXIS_PROPS} minTickGap={40} />
                <YAxis {...AXIS_PROPS} allowDecimals={false} width={28} />
                <Tooltip content={() => null} cursor={CURSOR_STYLE} />
                <Area type="stepAfter" dataKey="activeReplays" name="active replays"
                  stroke="var(--color-activeReplays)" fill="var(--color-activeReplays)/20"
                  strokeWidth={1.5} dot={false}
                  activeDot={makeActiveDot(v => String(Math.round(v)))} isAnimationActive={false} />
              </AreaChart>
            </ChartContainer>
          </Card>

        </div>
      </div>
    </Layout>
  )
}

import { createFileRoute } from '@tanstack/react-router'
import { useEffect, useRef, useState } from 'react'
import {
  AreaChart, Area, LineChart, Line,
  XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend,
} from 'recharts'
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
      const rest = line.slice(7)
      const spaceIdx = rest.indexOf(' ')
      currentHelp = rest.slice(spaceIdx + 1)
      continue
    }
    if (line.startsWith('# TYPE ')) {
      const rest = line.slice(7)
      const spaceIdx = rest.indexOf(' ')
      currentType = rest.slice(spaceIdx + 1)
      continue
    }
    if (line.startsWith('#')) continue

    // Parse sample line: name{labels} value [timestamp]
    const braceOpen = line.indexOf('{')
    const braceClose = line.indexOf('}')

    let name: string
    let labels: Record<string, string> = {}
    let valueStr: string

    if (braceOpen >= 0 && braceClose > braceOpen) {
      name = line.slice(0, braceOpen)
      const labelStr = line.slice(braceOpen + 1, braceClose)
      valueStr = line.slice(braceClose + 1).trim().split(' ')[0]
      // parse label pairs: key="value",key2="value2"
      for (const pair of labelStr.split(',')) {
        const eq = pair.indexOf('=')
        if (eq < 0) continue
        const k = pair.slice(0, eq).trim()
        const v = pair.slice(eq + 1).trim().replace(/^"|"$/g, '')
        labels[k] = v
      }
    } else {
      const spaceIdx = line.indexOf(' ')
      if (spaceIdx < 0) continue
      name = line.slice(0, spaceIdx)
      valueStr = line.slice(spaceIdx + 1).split(' ')[0]
    }

    const value = parseFloat(valueStr)
    if (isNaN(value)) continue

    // Normalise histogram/summary suffix families
    const family = name.replace(/_bucket$|_count$|_sum$|_max$/, '')
    if (!result[family]) {
      result[family] = { help: currentHelp, type: currentType, samples: [] }
    }
    result[family].samples.push({ labels: { ...labels, __name__: name }, value })
  }

  return result
}

function getSample(family: Record<string, MetricFamily>, name: string, labelFilter: Record<string, string> = {}): number {
  const f = family[name]
  if (!f) return NaN
  for (const s of f.samples) {
    if (s.labels.__name__ !== name) continue
    if (Object.entries(labelFilter).every(([k, v]) => s.labels[k] === v)) return s.value
  }
  return NaN
}

function getSamplesBy(family: Record<string, MetricFamily>, name: string, groupBy: string): Record<string, number> {
  const f = family[name]
  if (!f) return {}
  const result: Record<string, number> = {}
  for (const s of f.samples) {
    if (s.labels.__name__ !== name) continue
    const key = s.labels[groupBy] ?? 'unknown'
    result[key] = (result[key] ?? 0) + s.value
  }
  return result
}

// ---------------------------------------------------------------------------
// Rolling history (last MAX_POINTS scrapes)
// ---------------------------------------------------------------------------

const MAX_POINTS = 60
const POLL_MS    = 3_000

interface DataPoint {
  ts: number
  // recording
  lag:           Record<string, number>
  consumed:      Record<string, number>
  written:       Record<string, number>
  batchSize:     Record<string, number>
  writeDepth:    number
  writeDuration: number
  // kafka consumer per-topic rates
  consumedRate:  Record<string, number>
  bytesRate:     Record<string, number>
  fetchLatency:  Record<string, number>
  // compaction / retention
  compactionFiles: number
  retentionRows:   number
  // catalog
  catalogBytes:  number
  inlinedBytes:  number
  // replay
  activeReplays: number
  // jvm
  heapUsed:      number
  heapMax:       number
  gcPause:       number
}

function scrapeToPoint(family: Record<string, MetricFamily>): DataPoint {
  const topicKeys = Object.keys(getSamplesBy(family, 'joxette_messages_consumed_total', 'topic'))
  const lag: Record<string, number> = {}
  const consumed: Record<string, number> = {}
  const written: Record<string, number> = {}
  const batchSize: Record<string, number> = {}
  const consumedRate: Record<string, number> = {}
  const bytesRate: Record<string, number> = {}
  const fetchLatency: Record<string, number> = {}

  for (const topic of topicKeys) {
    lag[topic]      = getSample(family, 'joxette_consumer_lag',                { topic }) || 0
    consumed[topic] = getSample(family, 'joxette_messages_consumed_total',       { topic }) || 0
    written[topic]  = getSample(family, 'joxette_messages_written_total',         { topic }) || 0
    batchSize[topic] = getSample(family, 'joxette_write_batch_size_sum', { topic }) /
                      (getSample(family, 'joxette_write_batch_size_count', { topic }) || 1)
    consumedRate[topic]  = getSample(family, 'joxette_kafka_consumer_records_consumed_rate',  { topic }) || 0
    bytesRate[topic]     = getSample(family, 'joxette_kafka_consumer_bytes_consumed_rate',    { topic }) || 0
    fetchLatency[topic]  = getSample(family, 'joxette_kafka_consumer_fetch_latency_avg',      { topic }) || 0
  }

  const heapUsed = getSample(family, 'jvm_memory_used_bytes', { area: 'heap' })
  const heapMax  = getSample(family, 'jvm_memory_max_bytes',  { area: 'heap' })

  // GC pause: sum of all gc pause seconds
  const gcPauseSamples = Object.values(getSamplesBy(family, 'jvm_gc_pause_seconds_sum', 'action'))
  const gcPause = gcPauseSamples.reduce((a, b) => a + b, 0)

  return {
    ts: Date.now(),
    lag, consumed, written, batchSize, writeDepth: getSample(family, 'joxette_write_channel_depth') || 0,
    writeDuration: (getSample(family, 'joxette_write_duration_seconds_sum') /
                   (getSample(family, 'joxette_write_duration_seconds_count') || 1)) * 1000,
    consumedRate, bytesRate, fetchLatency,
    compactionFiles: getSample(family, 'joxette_compaction_files_processed_total') || 0,
    retentionRows:   Object.values(getSamplesBy(family, 'joxette_retention_rows_deleted_total', 'table_type')).reduce((a, b) => a + b, 0),
    catalogBytes:  getSample(family, 'joxette_catalog_size_bytes')  || 0,
    inlinedBytes:  getSample(family, 'joxette_catalog_inlined_bytes') || 0,
    activeReplays: getSample(family, 'joxette_replay_active') || 0,
    heapUsed: isNaN(heapUsed) ? 0 : heapUsed,
    heapMax:  isNaN(heapMax)  ? 0 : heapMax,
    gcPause:  isNaN(gcPause)  ? 0 : gcPause,
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function fmt(n: number, decimals = 1) {
  if (isNaN(n) || n === 0) return '0'
  if (n >= 1e9) return `${(n / 1e9).toFixed(decimals)}B`
  if (n >= 1e6) return `${(n / 1e6).toFixed(decimals)}M`
  if (n >= 1e3) return `${(n / 1e3).toFixed(decimals)}K`
  return n.toFixed(decimals)
}
function fmtBytes(n: number) {
  if (!n || isNaN(n)) return '0'
  if (n >= 1 << 30) return `${(n / (1 << 30)).toFixed(2)} GB`
  if (n >= 1 << 20) return `${(n / (1 << 20)).toFixed(1)} MB`
  if (n >= 1 << 10) return `${(n / (1 << 10)).toFixed(1)} KB`
  return `${n} B`
}
function fmtMs(n: number) { return isNaN(n) ? '—' : `${n.toFixed(1)} ms` }
function timeTick(ts: number) {
  const d = new Date(ts)
  return `${d.getHours().toString().padStart(2,'0')}:${d.getMinutes().toString().padStart(2,'0')}:${d.getSeconds().toString().padStart(2,'0')}`
}

const TOPIC_COLORS = ['#6674cc', '#3E9A7A', '#A26612', '#8B2121', '#1E5A8A', '#6B46A0']

// ---------------------------------------------------------------------------
// Chart card wrapper
// ---------------------------------------------------------------------------

function ChartCard({ title, subtitle, children }: { title: string; subtitle?: string; children: React.ReactNode }) {
  return (
    <div style={{ ...cardStyle, padding: '16px 20px' }}>
      <div style={{ marginBottom: 12 }}>
        <span style={{ fontWeight: 600, fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-primary)' }}>{title}</span>
        {subtitle && <span style={{ marginLeft: 8, fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>{subtitle}</span>}
      </div>
      {children}
    </div>
  )
}

const tooltipStyle = {
  contentStyle: {
    background: 'var(--surface-paper)',
    border: '1px solid var(--rule)',
    borderRadius: 'var(--radius-sm)',
    fontSize: '0.75rem',
    fontFamily: 'var(--font-mono)',
  },
  labelStyle: { color: 'var(--ink-secondary)', fontSize: '0.6875rem' },
}

// ---------------------------------------------------------------------------
// Stat pill
// ---------------------------------------------------------------------------

function Stat({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', minWidth: 100 }}>
      <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 600 }}>{label}</span>
      <span style={{ fontFamily: 'var(--font-mono)', fontSize: '1.125rem', fontWeight: 700, color: 'var(--ink-primary)', lineHeight: 1.3 }}>{value}</span>
      {sub && <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>{sub}</span>}
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
      setLastTs(Date.now())
      setError(null)
    } catch (e) {
      setError(`Failed to fetch metrics: ${(e as Error).message}`)
    }
  }

  useEffect(() => {
    void poll()
    timerRef.current = setInterval(() => { void poll() }, POLL_MS)
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [])

  const latest = history[history.length - 1]
  const topics = latest ? Object.keys(latest.lag) : []

  // Build per-topic rate series (delta between consecutive points)
  const rateSeries = history.map((pt, i) => {
    if (i === 0) return { ts: pt.ts, ...Object.fromEntries(topics.map(t => [t, 0])) }
    const prev = history[i - 1]
    const dtSec = (pt.ts - prev.ts) / 1000
    const row: Record<string, number | string> = { ts: pt.ts }
    for (const t of topics) {
      row[t] = dtSec > 0 ? Math.max(0, (pt.consumed[t] - prev.consumed[t]) / dtSec) : 0
    }
    return row
  })

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

      {/* ── Headline stats ──────────────────────────────────────────────── */}
      {latest && (
        <div style={{ ...cardStyle, padding: '16px 20px', marginBottom: 24, display: 'flex', gap: 32, flexWrap: 'wrap' }}>
          {topics.map(t => (
            <Stat key={t} label={`lag · ${t.length > 28 ? '…' + t.slice(-20) : t}`}
              value={fmt(latest.lag[t] ?? 0, 0)}
              sub={`${fmt(latest.consumedRate[t] ?? 0)} msg/s`} />
          ))}
          <div style={{ width: 1, background: 'var(--rule)', alignSelf: 'stretch' }} />
          <Stat label="write depth" value={String(latest.writeDepth)} sub="channel slots" />
          <Stat label="write latency" value={fmtMs(latest.writeDuration)} sub="avg batch" />
          <Stat label="catalog" value={fmtBytes(latest.catalogBytes)} sub={`${fmtBytes(latest.inlinedBytes)} inlined`} />
          <Stat label="replays" value={String(latest.activeReplays)} sub="active" />
          <Stat label="heap" value={fmtBytes(latest.heapUsed)} sub={`of ${fmtBytes(latest.heapMax)}`} />
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(480px, 1fr))', gap: 20 }}>

        {/* Consumer lag per topic */}
        <ChartCard title="Consumer Lag" subtitle="messages behind head">
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={history}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" />
              <XAxis dataKey="ts" tickFormatter={timeTick} tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} minTickGap={30} />
              <YAxis tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} tickFormatter={v => fmt(v, 0)} width={48} />
              <Tooltip {...tooltipStyle} labelFormatter={timeTick as any} formatter={(v: any) => [fmt(Number(v), 0), '']} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              {topics.map((t, i) => (
                <Area key={t} type="monotone" dataKey={`lag.${t}`} name={t} stroke={TOPIC_COLORS[i % TOPIC_COLORS.length]}
                  fill={TOPIC_COLORS[i % TOPIC_COLORS.length] + '22'} strokeWidth={1.5} dot={false} isAnimationActive={false} />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Consume rate */}
        <ChartCard title="Consume Rate" subtitle="msg / s">
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={rateSeries}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" />
              <XAxis dataKey="ts" tickFormatter={timeTick} tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} minTickGap={30} />
              <YAxis tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} tickFormatter={v => fmt(v, 0)} width={48} />
              <Tooltip {...tooltipStyle} labelFormatter={timeTick as any} formatter={(v: any) => [fmt(Number(v), 1) + '/s', '']} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              {topics.map((t, i) => (
                <Line key={t} type="monotone" dataKey={t} name={t} stroke={TOPIC_COLORS[i % TOPIC_COLORS.length]}
                  strokeWidth={1.5} dot={false} isAnimationActive={false} />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Bytes consumed rate */}
        <ChartCard title="Bytes Consumed Rate" subtitle="bytes / s from broker">
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={history}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" />
              <XAxis dataKey="ts" tickFormatter={timeTick} tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} minTickGap={30} />
              <YAxis tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} tickFormatter={v => fmtBytes(v) + '/s'} width={72} />
              <Tooltip {...tooltipStyle} labelFormatter={timeTick as any} formatter={(v: any) => [fmtBytes(Number(v)) + '/s', '']} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              {topics.map((t, i) => (
                <Line key={t} type="monotone" dataKey={`bytesRate.${t}`} name={t} stroke={TOPIC_COLORS[i % TOPIC_COLORS.length]}
                  strokeWidth={1.5} dot={false} isAnimationActive={false} />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Write channel depth + latency */}
        <ChartCard title="Write Pipeline" subtitle="channel depth & batch latency">
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={history}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" />
              <XAxis dataKey="ts" tickFormatter={timeTick} tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} minTickGap={30} />
              <YAxis yAxisId="depth" tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} width={32} />
              <YAxis yAxisId="ms" orientation="right" tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} tickFormatter={v => v + 'ms'} width={48} />
              <Tooltip {...tooltipStyle} labelFormatter={timeTick as any} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              <Line yAxisId="depth" type="monotone" dataKey="writeDepth" name="depth" stroke="#6674cc" strokeWidth={1.5} dot={false} isAnimationActive={false} />
              <Line yAxisId="ms" type="monotone" dataKey="writeDuration" name="batch ms" stroke="#3E9A7A" strokeWidth={1.5} dot={false} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Fetch latency per topic */}
        <ChartCard title="Fetch Latency" subtitle="avg Kafka fetch round-trip (ms)">
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={history}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" />
              <XAxis dataKey="ts" tickFormatter={timeTick} tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} minTickGap={30} />
              <YAxis tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} tickFormatter={v => v + 'ms'} width={48} />
              <Tooltip {...tooltipStyle} labelFormatter={timeTick as any} formatter={(v: any) => [fmtMs(Number(v)), '']} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              {topics.map((t, i) => (
                <Line key={t} type="monotone" dataKey={`fetchLatency.${t}`} name={t} stroke={TOPIC_COLORS[i % TOPIC_COLORS.length]}
                  strokeWidth={1.5} dot={false} isAnimationActive={false} />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Catalog & inlined bytes */}
        <ChartCard title="Catalog Storage" subtitle="catalog file · inlined data">
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={history}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" />
              <XAxis dataKey="ts" tickFormatter={timeTick} tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} minTickGap={30} />
              <YAxis tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} tickFormatter={fmtBytes} width={68} />
              <Tooltip {...tooltipStyle} labelFormatter={timeTick as any} formatter={(v: any) => [fmtBytes(Number(v)), '']} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              <Area type="monotone" dataKey="catalogBytes" name="catalog file" stroke="#6674cc" fill="#6674cc22" strokeWidth={1.5} dot={false} isAnimationActive={false} />
              <Area type="monotone" dataKey="inlinedBytes" name="inlined" stroke="#A26612" fill="#A2661222" strokeWidth={1.5} dot={false} isAnimationActive={false} />
            </AreaChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* JVM heap */}
        <ChartCard title="JVM Heap" subtitle="used · max">
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={history}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" />
              <XAxis dataKey="ts" tickFormatter={timeTick} tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} minTickGap={30} />
              <YAxis tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} tickFormatter={fmtBytes} width={68} />
              <Tooltip {...tooltipStyle} labelFormatter={timeTick as any} formatter={(v: any) => [fmtBytes(Number(v)), '']} />
              <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              <Area type="monotone" dataKey="heapMax" name="heap max" stroke="var(--rule-strong)" fill="transparent" strokeDasharray="4 3" strokeWidth={1} dot={false} isAnimationActive={false} />
              <Area type="monotone" dataKey="heapUsed" name="heap used" stroke="#1E5A8A" fill="#1E5A8A22" strokeWidth={1.5} dot={false} isAnimationActive={false} />
            </AreaChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Active replays */}
        <ChartCard title="Active Replays" subtitle="concurrent replay-to-topic operations">
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={history}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--rule)" />
              <XAxis dataKey="ts" tickFormatter={timeTick} tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} minTickGap={30} />
              <YAxis tick={{ fontSize: 10, fill: 'var(--ink-tertiary)' }} allowDecimals={false} width={32} />
              <Tooltip {...tooltipStyle} labelFormatter={timeTick as any} />
              <Area type="stepAfter" dataKey="activeReplays" name="replays" stroke="#6B46A0" fill="#6B46A022" strokeWidth={1.5} dot={false} isAnimationActive={false} />
            </AreaChart>
          </ResponsiveContainer>
        </ChartCard>

      </div>
    </Layout>
  )
}

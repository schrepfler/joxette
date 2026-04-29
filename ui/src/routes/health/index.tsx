import { createFileRoute } from '@tanstack/react-router'
import { useQuery } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import {
  BarChart, Bar, Cell, XAxis, YAxis, Tooltip, ResponsiveContainer,
  LineChart, Line, ReferenceLine,
} from 'recharts'
import { TrendingUp, TrendingDown, Minus, Database, Layers } from 'lucide-react'
import { healthApi, type TopicLag } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { pageTitle, cardStyle } from '../../styles/shared'

export const Route = createFileRoute('/health/')({
  component: HealthPage,
})

const MAX_SAMPLES = 20

interface HealthSample {
  ts: number
  lag: TopicLag[]
  catalogSizeBytes: number
  inlinedDataSizeBytes: number
}

function lagTone(lag: number): { fill: string; bg: string; text: string } {
  if (lag === 0) return { fill: 'var(--signal-live)', bg: '#dcfce7', text: 'var(--signal-live-ink)' }
  if (lag <= 1000) return { fill: 'var(--signal-warn)', bg: '#fef9c3', text: 'var(--signal-warn-ink)' }
  return { fill: 'var(--signal-error)', bg: '#fee2e2', text: 'var(--signal-error-ink)' }
}

function getTrend(history: HealthSample[], topic: string): 'up' | 'down' | 'stable' {
  if (history.length < 3) return 'stable'
  const last = history[history.length - 1].lag.find(t => t.topic === topic)?.totalLag ?? 0
  const older = history[Math.max(0, history.length - 4)].lag.find(t => t.topic === topic)?.totalLag ?? 0
  if (last - older > 50) return 'up'
  if (older - last > 50) return 'down'
  return 'stable'
}

function formatBytes(b: number) {
  if (b < 0) return 'N/A'
  if (b < 1024) return `${b} B`
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`
  if (b < 1024 * 1024 * 1024) return `${(b / 1024 / 1024).toFixed(1)} MB`
  return `${(b / 1024 / 1024 / 1024).toFixed(2)} GB`
}

function HealthPage() {
  const [history, setHistory] = useState<HealthSample[]>([])

  const { data, isLoading, error } = useQuery({
    queryKey: ['health'],
    queryFn: healthApi.get,
    refetchInterval: 10_000,
  })

  useEffect(() => {
    if (!data) return
    setHistory(prev =>
      [...prev, {
        ts: Date.now(),
        lag: data.consumerLag,
        catalogSizeBytes: data.catalogSizeBytes,
        inlinedDataSizeBytes: data.inlinedDataSizeBytes,
      }].slice(-MAX_SAMPLES)
    )
  }, [data])

  const statusOk = data?.status === 'UP' || data?.status === 'healthy'

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 24 }}>
        <h1 style={pageTitle}>Health</h1>
        {data && (
          <span className={`jx-badge ${statusOk ? 'jx-badge-success' : 'jx-badge-error'}`}>
            {data.status}
          </span>
        )}
        <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
          auto-refresh every 10 s
        </span>
      </div>

      {isLoading && <LoadingSpinner />}
      {error && <ErrorMessage message={(error as Error).message} />}

      {data && (
        <>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 16, marginBottom: 24 }}>
            <StorageMetric
              icon={<Database size={16} color="var(--ink-tertiary)" />}
              label="Catalog Size"
              value={formatBytes(data.catalogSizeBytes)}
              raw={data.catalogSizeBytes}
            />
            <StorageMetric
              icon={<Layers size={16} color="var(--ink-tertiary)" />}
              label="Inlined Data"
              value={formatBytes(data.inlinedDataSizeBytes)}
              raw={data.inlinedDataSizeBytes}
              total={data.catalogSizeBytes > 0 ? data.catalogSizeBytes : undefined}
              subtitle={
                data.catalogSizeBytes > 0 && data.inlinedDataSizeBytes >= 0
                  ? `${((data.inlinedDataSizeBytes / data.catalogSizeBytes) * 100).toFixed(1)}% of catalog`
                  : undefined
              }
            />
            <div style={cardStyle}>
              <div style={overline}>Catalog Path</div>
              <div style={{ marginTop: 8, fontFamily: 'var(--font-mono)', fontSize: 'var(--type-mono-size)', color: 'var(--ink-primary)', wordBreak: 'break-all' }}>
                {data.catalogPath}
              </div>
            </div>
          </div>

          <div style={{ ...cardStyle, marginBottom: 24 }}>
            <h3 style={{ margin: '0 0 12px', fontSize: 'var(--type-h4-size)', fontWeight: 600 }}>Active Recorders</h3>
            {data.activeRecorders.length === 0 ? (
              <p style={{ margin: 0, color: 'var(--ink-tertiary)', fontSize: 'var(--type-body-sm-size)' }}>No active recorders</p>
            ) : (
              <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                {data.activeRecorders.map(r => (
                  <span key={r} className="jx-badge jx-badge-success">{r}</span>
                ))}
              </div>
            )}
          </div>

          <div style={cardStyle}>
            <h3 style={{ margin: '0 0 4px', fontSize: 'var(--type-h4-size)', fontWeight: 600 }}>Consumer Lag</h3>
            {history.length > 0 && (
              <p style={{ margin: '0 0 16px', fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
                {history.length} sample{history.length !== 1 ? 's' : ''} collected
              </p>
            )}
            {data.consumerLag.length === 0 ? (
              <p style={{ margin: 0, color: 'var(--ink-tertiary)', fontSize: 'var(--type-body-sm-size)' }}>No consumer lag data</p>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column' }}>
                {data.consumerLag.map((topicLag, i) => (
                  <TopicLagCard key={topicLag.topic} topicLag={topicLag} history={history} isFirst={i === 0} />
                ))}
              </div>
            )}
          </div>
        </>
      )}
    </Layout>
  )
}

function StorageMetric({ icon, label, value, raw, total, subtitle }: {
  icon: React.ReactNode
  label: string
  value: string
  raw: number
  total?: number
  subtitle?: string
}) {
  const pct = raw >= 0 && total && total > 0 ? Math.min(100, (raw / total) * 100) : raw >= 0 ? 100 : 0
  return (
    <div style={cardStyle}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
        {icon}
        <span style={overline}>{label}</span>
      </div>
      <div style={{ fontFamily: 'var(--font-mono)', fontSize: '1.5rem', fontWeight: 700, color: 'var(--ink-primary)', marginBottom: 4, fontVariantNumeric: 'tabular-nums' }}>
        {value}
      </div>
      {subtitle && <div style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', marginBottom: 8 }}>{subtitle}</div>}
      {raw >= 0 && (
        <div style={{ height: 4, background: 'var(--surface-sunken)', borderRadius: 'var(--radius-full)', overflow: 'hidden', marginTop: subtitle ? 0 : 8 }}>
          <div style={{
            height: '100%',
            width: total ? `${pct}%` : '100%',
            background: 'var(--accent)',
            borderRadius: 'var(--radius-full)',
            transition: 'width 0.5s ease',
          }} />
        </div>
      )}
    </div>
  )
}

function TopicLagCard({ topicLag, history, isFirst }: {
  topicLag: TopicLag
  history: HealthSample[]
  isFirst: boolean
}) {
  const { topic, totalLag, lagByPartition } = topicLag
  const tone = lagTone(totalLag)
  const trend = getTrend(history, topic)
  const TrendIcon = trend === 'up' ? TrendingUp : trend === 'down' ? TrendingDown : Minus
  const trendColor = trend === 'up' ? 'var(--signal-error)' : trend === 'down' ? 'var(--signal-live)' : 'var(--ink-tertiary)'

  const partitionData = Object.entries(lagByPartition)
    .sort(([a], [b]) => Number(a) - Number(b))
    .map(([p, lag]) => ({ partition: `p${p}`, lag }))

  const sparkData = history.map((s, i) => ({
    i,
    lag: s.lag.find(t => t.topic === topic)?.totalLag ?? 0,
  }))

  return (
    <div style={{ borderTop: isFirst ? 'none' : '1px solid var(--rule)', paddingTop: isFirst ? 0 : 20, marginTop: isFirst ? 0 : 20 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 14, flexWrap: 'wrap' }}>
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-mono-size)', fontWeight: 600, color: 'var(--ink-primary)' }}>{topic}</span>
        <span style={{ background: tone.bg, color: tone.text, padding: '2px 10px', borderRadius: 'var(--radius-full)', fontSize: 'var(--type-caption-size)', fontWeight: 600 }}>
          {totalLag < 0 ? 'unavailable' : `${totalLag.toLocaleString()} behind`}
        </span>
        <span style={{ display: 'flex', alignItems: 'center', gap: 4, color: trendColor, fontSize: 'var(--type-caption-size)' }}>
          <TrendIcon size={14} strokeWidth={2} />
          {trend !== 'stable' && <span>{trend === 'up' ? 'Growing' : 'Shrinking'}</span>}
        </span>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: 16 }}>
        {partitionData.length > 0 && (
          <div>
            <div style={chartLabel}>Lag per Partition</div>
            <ResponsiveContainer width="100%" height={110}>
              <BarChart data={partitionData} margin={{ top: 4, right: 4, left: -8, bottom: 0 }}>
                <XAxis dataKey="partition" tick={{ fontSize: 11, fill: 'var(--ink-tertiary)' }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fontSize: 11, fill: 'var(--ink-tertiary)' }} axisLine={false} tickLine={false} width={44} />
                <Tooltip
                  cursor={{ fill: 'var(--surface-raised)' }}
                  formatter={(val: unknown) => [Number(val).toLocaleString(), 'Lag']}
                  contentStyle={{ fontSize: 12, borderRadius: 'var(--radius-sm)', border: '1px solid var(--rule)' }}
                />
                <Bar dataKey="lag" radius={[2, 2, 0, 0]}>
                  {partitionData.map(entry => (
                    <Cell key={entry.partition} fill={lagTone(entry.lag).fill} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {sparkData.length > 1 && (
          <div>
            <div style={chartLabel}>Total Lag Trend ({sparkData.length} samples, 10 s apart)</div>
            <ResponsiveContainer width="100%" height={110}>
              <LineChart data={sparkData} margin={{ top: 4, right: 4, left: -8, bottom: 0 }}>
                <XAxis dataKey="i" hide />
                <YAxis tick={{ fontSize: 11, fill: 'var(--ink-tertiary)' }} axisLine={false} tickLine={false} width={44} />
                <Tooltip
                  formatter={(val: unknown) => [Number(val).toLocaleString(), 'Total Lag']}
                  labelFormatter={() => ''}
                  contentStyle={{ fontSize: 12, borderRadius: 'var(--radius-sm)', border: '1px solid var(--rule)' }}
                />
                <ReferenceLine y={0} stroke="var(--rule)" />
                <Line type="monotone" dataKey="lag" stroke="var(--accent)" strokeWidth={2} dot={false} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}

        {sparkData.length <= 1 && partitionData.length > 0 && (
          <div>
            <div style={chartLabel}>Partition Detail</div>
            <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginTop: 8 }}>
              {partitionData.map(({ partition, lag }) => {
                const t = lagTone(lag)
                return (
                  <span key={partition} style={{ background: t.bg, color: t.text, padding: '3px 8px', borderRadius: 'var(--radius-xs)', fontSize: 'var(--type-caption-size)', fontWeight: 500, fontFamily: 'var(--font-mono)' }}>
                    {partition}: {lag.toLocaleString()}
                  </span>
                )
              })}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

const overline: React.CSSProperties = {
  fontSize: 'var(--type-micro-size)',
  fontWeight: 'var(--type-micro-weight)' as unknown as number,
  letterSpacing: 'var(--type-micro-tracking)',
  textTransform: 'uppercase',
  color: 'var(--ink-tertiary)',
}

const chartLabel: React.CSSProperties = {
  fontSize: 'var(--type-caption-size)',
  color: 'var(--ink-tertiary)',
  marginBottom: 4,
}

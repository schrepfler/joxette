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

// Lag thresholds: green=0, yellow=1–1000, red=>1000
function lagFill(lag: number): string {
  if (lag === 0) return '#48bb78'
  if (lag <= 1000) return '#ecc94b'
  return '#f56565'
}

function lagBadgeBg(lag: number): string {
  if (lag === 0) return '#c6f6d5'
  if (lag <= 1000) return '#fefcbf'
  return '#fed7d7'
}

function lagBadgeText(lag: number): string {
  if (lag === 0) return '#276749'
  if (lag <= 1000) return '#744210'
  return '#9b2c2c'
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
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: '1.5rem' }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>Health</h1>
        {data && (
          <span style={{
            background: statusOk ? '#c6f6d5' : '#fed7d7',
            color: statusOk ? '#276749' : '#9b2c2c',
            padding: '3px 12px', borderRadius: 12, fontSize: 13, fontWeight: 600,
          }}>
            {data.status}
          </span>
        )}
        <span style={{ fontSize: 12, color: '#a0aec0' }}>auto-refresh every 10s</span>
      </div>

      {isLoading && <LoadingSpinner />}
      {error && <ErrorMessage message={(error as Error).message} />}

      {data && (
        <>
          {/* Storage metrics */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 16, marginBottom: '1.5rem' }}>
            <StorageMetric
              icon={<Database size={18} color="#718096" />}
              label="Catalog Size"
              value={formatBytes(data.catalogSizeBytes)}
              raw={data.catalogSizeBytes}
              accentColor="#667eea"
            />
            <StorageMetric
              icon={<Layers size={18} color="#718096" />}
              label="Inlined Data"
              value={formatBytes(data.inlinedDataSizeBytes)}
              raw={data.inlinedDataSizeBytes}
              total={data.catalogSizeBytes > 0 ? data.catalogSizeBytes : undefined}
              accentColor="#38b2ac"
              subtitle={
                data.catalogSizeBytes > 0 && data.inlinedDataSizeBytes >= 0
                  ? `${((data.inlinedDataSizeBytes / data.catalogSizeBytes) * 100).toFixed(1)}% of catalog`
                  : undefined
              }
            />
            <div style={card}>
              <div style={cardLabel}>Catalog Path</div>
              <div style={{ marginTop: 8, fontSize: 13, fontWeight: 500, color: '#2d3748', wordBreak: 'break-all' }}>
                {data.catalogPath}
              </div>
            </div>
          </div>

          {/* Active recorders */}
          <div style={{ ...card, marginBottom: '1.5rem' }}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Active Recorders</h3>
            {data.activeRecorders.length === 0 ? (
              <p style={{ margin: 0, color: '#718096', fontSize: 14 }}>No active recorders</p>
            ) : (
              <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                {data.activeRecorders.map(r => (
                  <span key={r} style={{ background: '#c6f6d5', color: '#276749', padding: '3px 10px', borderRadius: 12, fontSize: 13 }}>{r}</span>
                ))}
              </div>
            )}
          </div>

          {/* Consumer lag per topic */}
          <div style={card}>
            <h3 style={{ margin: '0 0 0.25rem', fontSize: 15 }}>Consumer Lag</h3>
            {history.length > 0 && (
              <p style={{ margin: '0 0 1rem', fontSize: 11, color: '#a0aec0' }}>{history.length} sample{history.length !== 1 ? 's' : ''} collected</p>
            )}
            {data.consumerLag.length === 0 ? (
              <p style={{ margin: 0, color: '#718096', fontSize: 14 }}>No consumer lag data</p>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column' }}>
                {data.consumerLag.map((topicLag, i) => (
                  <TopicLagCard
                    key={topicLag.topic}
                    topicLag={topicLag}
                    history={history}
                    isFirst={i === 0}
                  />
                ))}
              </div>
            )}
          </div>
        </>
      )}
    </Layout>
  )
}

function StorageMetric({
  icon, label, value, raw, total, accentColor, subtitle,
}: {
  icon: React.ReactNode
  label: string
  value: string
  raw: number
  total?: number
  accentColor: string
  subtitle?: string
}) {
  const pct = raw >= 0 && total && total > 0 ? Math.min(100, (raw / total) * 100) : raw >= 0 ? 100 : 0
  const barVisible = raw >= 0

  return (
    <div style={card}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
        {icon}
        <span style={cardLabel}>{label}</span>
      </div>
      <div style={{ fontSize: 24, fontWeight: 700, color: '#2d3748', marginBottom: 4 }}>{value}</div>
      {subtitle && <div style={{ fontSize: 11, color: '#718096', marginBottom: 8 }}>{subtitle}</div>}
      {barVisible && (
        <div style={{ height: 5, background: '#edf2f7', borderRadius: 3, overflow: 'hidden', marginTop: subtitle ? 0 : 8 }}>
          <div style={{
            height: '100%',
            width: total ? `${pct}%` : '100%',
            background: accentColor,
            borderRadius: 3,
            transition: 'width 0.5s ease',
          }} />
        </div>
      )}
    </div>
  )
}

function TopicLagCard({
  topicLag, history, isFirst,
}: {
  topicLag: TopicLag
  history: HealthSample[]
  isFirst: boolean
}) {
  const { topic, totalLag, lagByPartition } = topicLag

  const trend = getTrend(history, topic)
  const TrendIcon = trend === 'up' ? TrendingUp : trend === 'down' ? TrendingDown : Minus
  const trendColor = trend === 'up' ? '#f56565' : trend === 'down' ? '#48bb78' : '#a0aec0'

  const partitionData = Object.entries(lagByPartition)
    .sort(([a], [b]) => Number(a) - Number(b))
    .map(([p, lag]) => ({ partition: `p${p}`, lag }))

  const sparkData = history.map((s, i) => ({
    i,
    lag: s.lag.find(t => t.topic === topic)?.totalLag ?? 0,
  }))

  return (
    <div style={{ borderTop: isFirst ? 'none' : '1px solid #e2e8f0', paddingTop: isFirst ? 0 : 20, marginTop: isFirst ? 0 : 20 }}>
      {/* Header row */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 14, flexWrap: 'wrap' }}>
        <span style={{ fontWeight: 600, fontSize: 15, color: '#2d3748' }}>{topic}</span>
        <span style={{
          background: lagBadgeBg(totalLag),
          color: lagBadgeText(totalLag),
          padding: '2px 10px', borderRadius: 10, fontSize: 13, fontWeight: 600,
        }}>
          {totalLag < 0 ? 'unavailable' : `${totalLag.toLocaleString()} behind`}
        </span>
        <span style={{ display: 'flex', alignItems: 'center', gap: 4, color: trendColor, fontSize: 13 }}>
          <TrendIcon size={16} strokeWidth={2.5} />
          {trend !== 'stable' && <span>{trend === 'up' ? 'Growing' : 'Shrinking'}</span>}
        </span>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: 16 }}>
        {/* Partition bar chart */}
        {partitionData.length > 0 && (
          <div>
            <div style={chartLabel}>Lag per Partition</div>
            <ResponsiveContainer width="100%" height={110}>
              <BarChart data={partitionData} margin={{ top: 4, right: 4, left: -8, bottom: 0 }}>
                <XAxis dataKey="partition" tick={{ fontSize: 11, fill: '#718096' }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fontSize: 11, fill: '#718096' }} axisLine={false} tickLine={false} width={44} />
                <Tooltip
                  cursor={{ fill: '#f7fafc' }}
                  formatter={(val: unknown) => [Number(val).toLocaleString(), 'Lag']}
                  contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e2e8f0' }}
                />
                <Bar dataKey="lag" radius={[3, 3, 0, 0]}>
                  {partitionData.map(entry => (
                    <Cell key={entry.partition} fill={lagFill(entry.lag)} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Trend sparkline */}
        {sparkData.length > 1 && (
          <div>
            <div style={chartLabel}>Total Lag Trend ({sparkData.length} samples, 10s apart)</div>
            <ResponsiveContainer width="100%" height={110}>
              <LineChart data={sparkData} margin={{ top: 4, right: 4, left: -8, bottom: 0 }}>
                <XAxis dataKey="i" hide />
                <YAxis tick={{ fontSize: 11, fill: '#718096' }} axisLine={false} tickLine={false} width={44} />
                <Tooltip
                  formatter={(val: unknown) => [Number(val).toLocaleString(), 'Total Lag']}
                  labelFormatter={() => ''}
                  contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e2e8f0' }}
                />
                <ReferenceLine y={0} stroke="#e2e8f0" />
                <Line
                  type="monotone"
                  dataKey="lag"
                  stroke="#667eea"
                  strokeWidth={2}
                  dot={false}
                  isAnimationActive={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Individual partition badges (when sparkline not yet available) */}
        {sparkData.length <= 1 && partitionData.length > 0 && (
          <div>
            <div style={chartLabel}>Partition Detail</div>
            <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginTop: 8 }}>
              {partitionData.map(({ partition, lag }) => (
                <span key={partition} style={{
                  background: lagBadgeBg(lag),
                  color: lagBadgeText(lag),
                  padding: '3px 8px', borderRadius: 6, fontSize: 12, fontWeight: 500,
                }}>
                  {partition}: {lag.toLocaleString()}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

const card: React.CSSProperties = {
  background: '#fff',
  border: '1px solid #e2e8f0',
  borderRadius: 8,
  padding: '0.875rem 1.125rem',
}

const cardLabel: React.CSSProperties = {
  fontSize: 11,
  color: '#718096',
  textTransform: 'uppercase',
  letterSpacing: '0.06em',
  fontWeight: 600,
}

const chartLabel: React.CSSProperties = {
  fontSize: 11,
  color: '#a0aec0',
  marginBottom: 4,
}

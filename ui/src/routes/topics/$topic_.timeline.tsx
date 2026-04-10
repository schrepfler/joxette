import { createFileRoute, Link } from '@tanstack/react-router'
import { useState, useCallback, useMemo } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer,
} from 'recharts'
import { cassettesApi, type CassetteRecord } from '../../api/client'
import { CassetteTimeline, type TimelineRecord, PALETTE } from '../../components/CassetteTimeline'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'

export const Route = createFileRoute('/topics/$topic_/timeline')({
  component: TopicTimelinePage,
})

function computePartitionBuckets(records: CassetteRecord[]): {
  buckets: Array<Record<string, number | string>>
  partitionKeys: string[]
} {
  if (records.length === 0) return { buckets: [], partitionKeys: [] }
  const bucketCount = Math.min(40, Math.max(10, Math.floor(records.length / 5)))
  const timestamps = records.map(r => new Date(r.timestamp).getTime())
  const minMs = Math.min(...timestamps)
  const maxMs = Math.max(...timestamps)
  const spanMs = maxMs - minMs || 1
  const partitionKeys = [...new Set(records.map(r => r.partition))]
    .sort((a, b) => a - b)
    .map(p => `partition ${p}`)
  const buckets: Array<Record<string, number | string>> = Array.from(
    { length: bucketCount },
    (_, i) => {
      const d = new Date(minMs + (i / bucketCount) * spanMs)
      const pad = (n: number, l = 2) => String(n).padStart(l, '0')
      const row: Record<string, number | string> = {
        time: `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`,
      }
      for (const pk of partitionKeys) row[pk] = 0
      return row
    },
  )
  for (let i = 0; i < records.length; i++) {
    const bi = Math.min(bucketCount - 1, Math.floor(((timestamps[i] - minMs) / spanMs) * bucketCount))
    const pk = `partition ${records[i].partition}`
    const b = buckets[bi]
    b[pk] = (b[pk] as number) + 1
  }
  return { buckets, partitionKeys }
}

function DensityChart({ records }: { records: CassetteRecord[] }) {
  const { buckets, partitionKeys } = computePartitionBuckets(records)
  if (buckets.length === 0) return null
  return (
    <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '0.625rem 0.75rem 0.5rem' }}>
      <div style={{ fontSize: 11, color: '#a0aec0', marginBottom: 4, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
        Message density by partition
      </div>
      <ResponsiveContainer width="100%" height={180}>
        <BarChart data={buckets} margin={{ top: 4, right: 4, left: -16, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />
          <XAxis dataKey="time" tick={{ fontSize: 10, fill: '#718096' }} axisLine={false} tickLine={false} interval="preserveStartEnd" />
          <YAxis tick={{ fontSize: 10, fill: '#718096' }} axisLine={false} tickLine={false} width={28} allowDecimals={false} />
          <Tooltip cursor={{ fill: '#f7fafc' }} contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e2e8f0' }} />
          {partitionKeys.map((pk, i) => (
            <Bar key={pk} dataKey={pk} stackId="a" fill={PALETTE[i % PALETTE.length]} isAnimationActive={false} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

function PartitionHeatmap({ records }: { records: CassetteRecord[] }) {
  const { buckets, partitionKeys } = computePartitionBuckets(records)
  if (buckets.length === 0 || partitionKeys.length === 0) return null
  const partitionNums = partitionKeys.map(pk => pk.replace('partition ', ''))
  const maxCell = Math.max(1, ...buckets.flatMap(b => partitionKeys.map(pk => b[pk] as number)))
  const totalHeight = 28 + 18 * partitionKeys.length
  return (
    <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '0.5rem 0.75rem', height: totalHeight, boxSizing: 'border-box' }}>
      <div style={{ display: 'flex', alignItems: 'center', height: 20, marginBottom: 8, gap: 4 }}>
        <div style={{ width: 28, flexShrink: 0 }} />
        <div style={{ flex: 1, fontSize: 10, color: '#a0aec0' }}>{buckets[0]?.time ?? ''}</div>
        <div style={{ fontSize: 10, color: '#a0aec0' }}>{buckets[buckets.length - 1]?.time ?? ''}</div>
      </div>
      {partitionKeys.map((pk, rowIdx) => {
        const color = PALETTE[rowIdx % PALETTE.length]
        return (
          <div key={pk} style={{ display: 'flex', alignItems: 'center', height: 18, gap: 4 }}>
            <div style={{ width: 28, flexShrink: 0, fontSize: 10, color: '#718096', fontWeight: 600, textAlign: 'right', paddingRight: 4 }}>
              P{partitionNums[rowIdx]}
            </div>
            <div style={{ flex: 1, display: 'flex', gap: 1 }}>
              {buckets.map((b, colIdx) => {
                const count = b[pk] as number
                const opacity = count === 0 ? 0.08 : 0.08 + 0.92 * (count / maxCell)
                return (
                  <div key={colIdx} title={`${pk} · ${b.time} · ${count} msg`}
                    style={{ flex: '1 1 0', minWidth: 8, height: 16, borderRadius: 2, background: color, opacity }} />
                )
              })}
            </div>
          </div>
        )
      })}
    </div>
  )
}

function toTimelineRecord(r: CassetteRecord): TimelineRecord {
  return {
    timestamp: r.timestamp,
    colorKey: `partition ${r.partition}`,
    value: r.value,
    meta: {
      partition: String(r.partition),
      offset: String(r.offset),
      ...(r.key ? { key: r.key } : {}),
      recorded: r.recordedAt.slice(0, 19).replace('T', ' '),
    },
  }
}

function TopicTimelinePage() {
  const { topic } = Route.useParams()

  const [records, setRecords] = useState<CassetteRecord[]>([])
  const [nextCursor, setNextCursor] = useState<string | undefined>()
  const [hasMoreAfter, setHasMoreAfter] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [initialLoaded, setInitialLoaded] = useState(false)

  const PAGE_SIZE = 200

  const loadPage = useCallback(async (cursor?: string) => {
    setLoading(true)
    setError(null)
    try {
      const page = await cassettesApi.getTopicRecords(topic, {
        cursor,
        limit: PAGE_SIZE,
      })
      setRecords((prev: CassetteRecord[]) => cursor ? [...prev, ...page.data] : page.data)
      setNextCursor(page.nextCursor)
      setHasMoreAfter(page.hasMore)
    } catch (e) {
      setError((e as Error).message)
    } finally {
      setLoading(false)
    }
  }, [topic])

  const handleInitialLoad = useCallback(() => {
    setInitialLoaded(true)
    void loadPage(undefined)
  }, [loadPage])

  const handleLoadAfter = useCallback(() => {
    if (!hasMoreAfter || !nextCursor || loading) return
    void loadPage(nextCursor)
  }, [hasMoreAfter, nextCursor, loading, loadPage])

  const timelineRecords = useMemo<TimelineRecord[]>(
    () => records.map(toTimelineRecord),
    [records],
  )

  return (
    <Layout>
      <div style={{ display: 'flex', flexDirection: 'column', height: 'calc(100vh - 80px)', minHeight: 0 }}>
        {/* Breadcrumb */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '0.75rem 0', marginBottom: '0.5rem', flexShrink: 0 }}>
          <Link
            to="/topics/$topic"
            params={{ topic }}
            style={{ fontSize: 13, color: '#3182ce', textDecoration: 'none' }}
          >
            ← {topic}
          </Link>
          <span style={{ fontSize: 12, color: '#a0aec0' }}>/</span>
          <span style={{ fontSize: 14, fontWeight: 700, color: '#2d3748' }}>Timeline</span>
        </div>

        {error && <ErrorMessage message={error} />}

        {!initialLoaded ? (
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', flex: 1, gap: 16 }}>
            <p style={{ color: '#718096', fontSize: 14, margin: 0 }}>
              Load messages for topic <strong>{topic}</strong> to begin.
            </p>
            <button style={primaryBtn} onClick={handleInitialLoad} disabled={loading}>
              {loading ? 'Loading…' : 'Load Messages'}
            </button>
          </div>
        ) : loading && records.length === 0 ? (
          <LoadingSpinner />
        ) : (
          <>
            {records.length >= 2 && (
              <div style={{ flexShrink: 0, marginBottom: 8, display: 'flex', flexDirection: 'column', gap: 8 }}>
                <DensityChart records={records} />
                <PartitionHeatmap records={records} />
              </div>
            )}
            <div style={{ flex: '1 1 0', minHeight: 0, border: '1px solid #e2e8f0', borderRadius: 8, overflow: 'hidden' }}>
              <CassetteTimeline
                records={timelineRecords}
                onLoadAfter={handleLoadAfter}
                hasMore={hasMoreAfter}
                loading={loading}
                title={topic}
                extraControls={
                  hasMoreAfter && !loading
                    ? <button style={secondaryBtn} onClick={handleLoadAfter}>Load next page</button>
                    : undefined
                }
              />
            </div>
          </>
        )}
      </div>
    </Layout>
  )
}

const primaryBtn: React.CSSProperties = {
  padding: '0.55rem 1.25rem', background: '#3182ce', color: '#fff',
  border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14, fontWeight: 600,
}
const secondaryBtn: React.CSSProperties = {
  padding: '0.3rem 0.7rem', background: '#fff', color: '#4a5568',
  border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 12,
}

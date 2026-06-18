import { createFileRoute, Link } from '@tanstack/react-router'
import { useState, useCallback, useMemo } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid, ResponsiveContainer,
} from 'recharts'
import { cassettesApi, type EntityRecord } from '../../../api/client'
import { CassetteTimeline, type TimelineRecord, type GroupByMode, colorForKey } from '../../../components/CassetteTimeline'
import { Layout } from '../../../components/Layout'
import { LoadingSpinner } from '../../../components/LoadingSpinner'
import { ErrorMessage } from '../../../components/ErrorMessage'

export const Route = createFileRoute('/entities/$entityType/$entityId_/timeline')({
  component: EntityTimelinePage,
})

function getSegmentKey(r: EntityRecord, kind: GroupByMode['kind']): string {
  if (kind === 'messageType') return r.messageType ?? '(unknown)'
  return r.topic
}

function computeActivityBuckets(records: EntityRecord[], kind: GroupByMode['kind']): {
  buckets: Array<Record<string, number | string>>
  segmentKeys: string[]
} {
  if (records.length === 0) return { buckets: [], segmentKeys: [] }
  const bucketCount = Math.min(40, Math.max(10, Math.floor(records.length / 5)))
  const timestamps = records.map(r => new Date(r.timestamp).getTime())
  const minMs = Math.min(...timestamps)
  const maxMs = Math.max(...timestamps)
  const spanMs = maxMs - minMs || 1
  const segmentKeys = [...new Set(records.map(r => getSegmentKey(r, kind)))].sort()
  const buckets: Array<Record<string, number | string>> = Array.from(
    { length: bucketCount },
    (_, i) => {
      const d = new Date(minMs + (i / bucketCount) * spanMs)
      const pad = (n: number, l = 2) => String(n).padStart(l, '0')
      const row: Record<string, number | string> = {
        time: `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`,
      }
      for (const sk of segmentKeys) row[sk] = 0
      return row
    },
  )
  for (let i = 0; i < records.length; i++) {
    const bi = Math.min(bucketCount - 1, Math.floor(((timestamps[i] - minMs) / spanMs) * bucketCount))
    const sk = getSegmentKey(records[i], kind)
    const b = buckets[bi]
    b[sk] = (b[sk] as number) + 1
  }
  return { buckets, segmentKeys }
}

const DIMENSION_LABEL: Partial<Record<GroupByMode['kind'], string>> = {
  messageType: 'message type',
  topic: 'topic',
  colorKey: 'topic',
}

function TopicActivityChart({ records, groupByKind }: { records: EntityRecord[]; groupByKind: GroupByMode['kind'] }) {
  const { buckets, segmentKeys } = computeActivityBuckets(records, groupByKind)
  if (buckets.length === 0) return null
  const label = DIMENSION_LABEL[groupByKind] ?? 'topic'
  return (
    <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '0.625rem 0.75rem 0.5rem' }}>
      <div style={{ fontSize: 11, color: '#a0aec0', marginBottom: 4, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
        Activity by {label}
      </div>
      <ResponsiveContainer width="100%" height={180}>
        <BarChart data={buckets} margin={{ top: 4, right: 4, left: -16, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />
          <XAxis dataKey="time" tick={{ fontSize: 10, fill: '#718096' }} axisLine={false} tickLine={false} interval="preserveStartEnd" />
          <YAxis tick={{ fontSize: 10, fill: '#718096' }} axisLine={false} tickLine={false} width={28} allowDecimals={false} />
          <Tooltip cursor={{ fill: '#f7fafc' }} contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e2e8f0' }} />
          {segmentKeys.map((sk) => (
            <Bar key={sk} dataKey={sk} stackId="a" fill={colorForKey(sk, segmentKeys)} isAnimationActive={false} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

function toTimelineRecord(r: EntityRecord): TimelineRecord {
  return {
    timestamp: r.timestamp,
    colorKey: r.topic,
    value: r.value,
    meta: {
      topic: r.topic,
      partition: String(r.partition),
      offset: String(r.offset),
      ...(r.messageType ? { type: r.messageType } : {}),
      ...(r.key ? { key: r.key } : {}),
      recorded: r.recordedAt.slice(0, 19).replace('T', ' '),
    },
    headers: r.headers,
    sourceTopic: r.topic,
    entityId: r.entityId,
  }
}

function EntityTimelinePage() {
  const { entityType, entityId } = Route.useParams()

  const [records, setRecords] = useState<EntityRecord[]>([])
  const [loading, setLoading] = useState(false)
  const [loadedCount, setLoadedCount] = useState(0)
  const [error, setError] = useState<string | null>(null)
  const [initialLoaded, setInitialLoaded] = useState(false)
  const [groupByKind, setGroupByKind] = useState<GroupByMode['kind']>('messageType')
  const abortRef = useState<{ cancelled: boolean }>({ cancelled: false })[0]

  const PAGE_SIZE = 200

  const loadAll = useCallback(async () => {
    abortRef.cancelled = false
    setLoading(true)
    setError(null)
    setLoadedCount(0)
    let cursor: string | undefined
    let all: EntityRecord[] = []
    try {
      do {
        if (abortRef.cancelled) break
        const page = await cassettesApi.getEntityRecords(entityType, entityId, {
          cursor,
          limit: PAGE_SIZE,
          order: 'asc',
        })
        all = [...all, ...page.data]
        cursor = page.hasMore ? page.nextCursor : undefined
        setRecords([...all])
        setLoadedCount(all.length)
      } while (cursor)
    } catch (e) {
      setError((e as Error).message)
    } finally {
      setLoading(false)
    }
  }, [entityType, entityId, abortRef])

  const handleInitialLoad = useCallback(() => {
    setInitialLoaded(true)
    void loadAll()
  }, [loadAll])

  const handleCancel = useCallback(() => {
    abortRef.cancelled = true
  }, [abortRef])

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
            to="/entities/$entityType/$entityId"
            params={{ entityType, entityId }}
            style={{ fontSize: 13, color: '#3182ce', textDecoration: 'none' }}
          >
            ← {entityType} / {entityId}
          </Link>
          <span style={{ fontSize: 12, color: '#a0aec0' }}>/</span>
          <span style={{ fontSize: 14, fontWeight: 700, color: '#2d3748' }}>Timeline</span>
        </div>

        {error && <ErrorMessage message={error} />}

        {!initialLoaded ? (
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', flex: 1, gap: 16 }}>
            <p style={{ color: '#718096', fontSize: 14, margin: 0 }}>
              Load recorded messages for <strong>{entityId}</strong> ({entityType}) to begin.
            </p>
            <button style={primaryBtn} onClick={handleInitialLoad} disabled={loading}>
              Load Messages
            </button>
          </div>
        ) : loading && records.length === 0 ? (
          <LoadingSpinner />
        ) : (
          <>
            {records.length >= 2 && (
              <div style={{ flexShrink: 0, marginBottom: 8 }}>
                <TopicActivityChart records={records} groupByKind={groupByKind} />
              </div>
            )}
            <div style={{ flex: '1 1 0', minHeight: 0, border: '1px solid #e2e8f0', borderRadius: 8, overflow: 'hidden' }}>
              <CassetteTimeline
                records={timelineRecords}
                hasMore={false}
                loading={loading}
                title={`${entityType} / ${entityId}`}
                supportsMessageType={true}
                onGroupByModeChange={mode => setGroupByKind(mode.kind)}
                extraControls={
                  loading
                    ? <span style={{ fontSize: 12, color: '#718096' }}>
                        Loading… {loadedCount} messages
                        <button style={{ ...secondaryBtn, marginLeft: 8 }} onClick={handleCancel}>Cancel</button>
                      </span>
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

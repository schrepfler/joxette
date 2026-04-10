import { createFileRoute, Link } from '@tanstack/react-router'
import { useState, useCallback, useMemo } from 'react'
import { cassettesApi, type CassetteRecord } from '../../api/client'
import { CassetteTimeline, type TimelineRecord } from '../../components/CassetteTimeline'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'

export const Route = createFileRoute('/topics/$topic_/timeline')({
  component: TopicTimelinePage,
})

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

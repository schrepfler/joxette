import { createFileRoute, useNavigate, Link } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { useState, useRef, useEffect } from 'react'
import { JsonView } from '../../../components/JsonView'
import { cassettesApi, entityOutputApi, entitiesApi, streamEntityRecords, type EntityRecord, type Order, type StreamMode, type EntityStreamParams, type PortraitResult } from '../../../api/client'
import { Layout } from '../../../components/Layout'
import { LoadingSpinner } from '../../../components/LoadingSpinner'
import { ErrorMessage } from '../../../components/ErrorMessage'
import { ConfirmDialog } from '../../../components/ConfirmDialog'
import { ReplayToTopicPanel } from '../../../components/ReplayToTopicPanel'
import { SolQueryPanel } from '../../../components/SolQueryPanel'
import { ViewModeBar } from '../../../components/ViewModeBar'
import { SequenceBarcodeView, BarcodeLegend, BarcodeXModeToggle, type BarcodeRow, type BarcodeXMode } from '../../../components/SequenceBarcodeView'
import { StreamStatusBadge, type StreamStatus } from '../../../components/StreamStatusBadge'
import { useToast } from '../../../components/Toast'
import { useDebounce } from '../../../hooks/useDebounce'
import type { FragmentDefinition } from '../../../transforms/types'
import { SequenceQueryPanel } from '../../../components/SequenceQueryPanel'

interface EntitySearch {
  /** Sort direction for entity event replay. UI default: 'desc' (latest first). */
  order?: Order
}

export const Route = createFileRoute('/entities/$entityType/$entityId')({
  component: EntityInstancePage,
  validateSearch: (raw: Record<string, unknown>): EntitySearch => {
    const o = raw.order
    return { order: o === 'asc' || o === 'desc' ? o : undefined }
  },
})

// ── JSON viewer (same as topics page) ─────────────────────────────────────────
function tryDecodeBase64(s: string): string | null {
  try {
    if (!/^[A-Za-z0-9+/\-_]+=*$/.test(s)) return null
    return atob(s.replace(/-/g, '+').replace(/_/g, '/'))
  } catch {
    return null
  }
}

function tryParseValue(s: string | null): { parsed: unknown; raw: string } | null {
  if (!s) return null
  try { return { parsed: JSON.parse(s) as unknown, raw: s } } catch { /* continue */ }
  const decoded = tryDecodeBase64(s)
  if (decoded) {
    try { return { parsed: JSON.parse(decoded) as unknown, raw: decoded } } catch { /* continue */ }
  }
  return null
}

function ValueCell({ raw }: { raw: string | null }) {
  const [open, setOpen] = useState(false)
  if (!raw) return <span style={{ color: '#a0aec0' }}>—</span>
  const result = tryParseValue(raw)
  const isJson = result !== null
  const preview = raw.length > 80 ? raw.slice(0, 80) + '…' : raw

  if (!isJson) return <span style={{ fontFamily: 'monospace', fontSize: 12 }}>{preview}</span>

  const decodedPreview = result.raw.length > 80 ? result.raw.slice(0, 80) + '…' : result.raw

  return (
    <div>
      <span
        onClick={() => setOpen(o => !o)}
        title={open ? 'Collapse' : 'Expand JSON'}
        style={{
          cursor: 'pointer',
          fontFamily: 'monospace',
          fontSize: 12,
          color: '#2b6cb0',
          userSelect: 'none',
        }}
      >
        <span style={{ marginRight: 4, fontSize: 10, display: 'inline-block', transform: open ? 'rotate(90deg)' : 'rotate(0deg)', transition: 'transform 0.15s' }}>▶</span>
        {open ? (result.raw !== raw ? 'JSON (base64-decoded)' : 'JSON') : decodedPreview}
      </span>
      {open && <JsonView src={result.parsed as object} />}
    </div>
  )
}

// ── Portrait panel ─────────────────────────────────────────────────────────────
function PortraitPanel({ portrait }: { portrait: PortraitResult }) {
  return (
    <div data-testid="portrait-panel">
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: '1rem' }}>
        {[
          ['Events', portrait.eventCount.toLocaleString()],
          ['First Seen', portrait.firstSeen?.slice(0, 19).replace('T', ' ') ?? '—'],
          ['Last Seen',  portrait.lastSeen?.slice(0, 19).replace('T', ' ')  ?? '—'],
        ].map(([k, v]) => (
          <div key={k} data-testid={`portrait-stat-${(k as string).toLowerCase().replace(' ', '-')}`}
               style={{ background: '#f7fafc', border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.5rem 0.85rem', minWidth: 140 }}>
            <div style={{ fontSize: 11, color: '#718096', marginBottom: 2 }}>{k}</div>
            <div style={{ fontSize: 14, fontWeight: 600 }}>{v}</div>
          </div>
        ))}
      </div>
      {Object.keys(portrait.topicBreakdown).length > 0 && (
        <div style={{ marginBottom: '1rem' }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: '#4a5568', marginBottom: 6 }}>Events by Topic</div>
          <div data-testid="portrait-topic-breakdown" style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            {Object.entries(portrait.topicBreakdown).map(([t, count]) => (
              <div key={t} style={{ background: '#ebf8ff', border: '1px solid #bee3f8', borderRadius: 4, padding: '2px 10px', fontSize: 13 }}>
                <strong>{t}</strong>: {count}
              </div>
            ))}
          </div>
        </div>
      )}
      {portrait.recentEvents.length > 0 && (
        <div style={{ marginBottom: '1rem' }}>
          <div style={{ fontSize: 13, fontWeight: 600, color: '#4a5568', marginBottom: 6 }}>Recent Events</div>
          <div data-testid="portrait-recent-events" style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
            {portrait.recentEvents.map((r, i) => (
              <div key={i} data-testid={`portrait-recent-event-${i}`}
                   style={{ display: 'flex', gap: 10, fontSize: 12, alignItems: 'center' }}>
                <span style={{ color: '#718096', minWidth: 140, fontFamily: 'monospace' }}>{r.timestamp.slice(0, 19).replace('T', ' ')}</span>
                {r.messageType && <span style={{ background: '#ebf8ff', border: '1px solid #bee3f8', borderRadius: 4, padding: '1px 6px' }}>{r.messageType}</span>}
                <span style={{ color: '#a0aec0' }}>{r.topic}</span>
              </div>
            ))}
          </div>
        </div>
      )}
      {portrait.currentState && (
        <div>
          <div style={{ fontSize: 13, fontWeight: 600, color: '#4a5568', marginBottom: 6 }}>Current State</div>
          <div data-testid="portrait-current-state">
            <JsonView src={portrait.currentState as object} />
          </div>
        </div>
      )}
    </div>
  )
}

const colHelper = createColumnHelper<EntityRecord>()

const trunc = (s: string | null, n: number) =>
  s == null ? '—' : s.length > n ? s.slice(0, n) + '…' : s

function isStreamActive(s: StreamStatus): boolean {
  return s === 'streaming' || s === 'draining' || s === 'tailing'
}

function EntityInstancePage() {
  const { entityType, entityId } = Route.useParams()
  const search = Route.useSearch()
  const navigate = useNavigate()
  const routeNavigate = useNavigate({ from: Route.fullPath })
  const qc = useQueryClient()
  const { addToast } = useToast()

  const order: Order = search.order ?? 'desc'
  function setOrder(next: Order) {
    if (next === order) return
    void routeNavigate({ search: (prev: EntitySearch) => ({ ...prev, order: next }) })
  }
  const [fromRaw, setFromRaw] = useState('')
  const [toRaw, setToRaw] = useState('')
  const from = useDebounce(fromRaw, 300)
  const to = useDebounce(toRaw, 300)
  const [cursor, setCursor] = useState<string | undefined>()
  const [cursors, setCursors] = useState<string[]>([])
  const [deleteStep, setDeleteStep] = useState<0 | 1 | 2>(0)
  const [activeTab, setActiveTab] = useState<'records' | 'sol' | 'timeline' | 'barcode' | 'sequence' | 'diff' | 'portrait' | 'state'>('records')
  const [barcodeXMode, setBarcodeXMode] = useState<BarcodeXMode>('time')
  const [_replayPipelineFragments, _setReplayPipelineFragments] = useState<FragmentDefinition[]>([])

  // Streaming state
  const [streamMode, setStreamMode] = useState<StreamMode>('json')
  const [followLive, setFollowLive] = useState(false)
  const [streamStatus, setStreamStatus] = useState<StreamStatus>('idle')
  const [streamedRecords, setStreamedRecords] = useState<EntityRecord[]>([])
  const [streamError, setStreamError] = useState<string | null>(null)
  const abortRef = useRef<AbortController | null>(null)
  const streamBufferRef = useRef<EntityRecord[]>([])
  const flushIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const followActiveRef = useRef(false)
  const terminalRef = useRef(false)

  const statsQuery = useQuery({
    queryKey: ['cassettes', 'entities', entityType, entityId, 'stats'],
    queryFn: () => cassettesApi.getEntityStats(entityType, entityId),
  })

  const sourcesQuery = useQuery({
    queryKey: ['entities', entityType, 'sources'],
    queryFn: () => entitiesApi.getSources(entityType),
  })

  const recordsQuery = useQuery({
    queryKey: ['cassettes', 'entities', entityType, entityId, 'records', { from, to, cursor, order }],
    queryFn: () => cassettesApi.getEntityRecords(entityType, entityId, {
      from: from || undefined,
      to: to || undefined,
      cursor,
      limit: 50,
      order,
    }),
  })

  const stateQuery = useQuery({
    queryKey: ['cassettes', 'entities', entityType, entityId, 'state', { from, to }],
    queryFn: () => entityOutputApi.getState(entityType, entityId, { from: from || undefined, to: to || undefined }),
    enabled: activeTab === 'state',
  })

  const diffQuery = useQuery({
    queryKey: ['cassettes', 'entities', entityType, entityId, 'diff', { from, to }],
    queryFn: () => entityOutputApi.getDiff(entityType, entityId, { from: from || undefined, to: to || undefined }),
    enabled: activeTab === 'diff',
  })

  const portraitQuery = useQuery({
    queryKey: ['cassettes', 'entities', entityType, entityId, 'portrait', { from, to }],
    queryFn: () => entityOutputApi.getPortrait(entityType, entityId, { from: from || undefined, to: to || undefined }),
    enabled: activeTab === 'portrait',
  })

  const deleteMutation = useMutation({
    mutationFn: () => cassettesApi.deleteEntity(entityType, entityId),
    onSuccess: (d) => {
      void qc.invalidateQueries({ queryKey: ['cassettes', 'entities', entityType] })
      addToast(`Deleted ${d.deleted} records`, 'success')
      void navigate({ to: '/entities/$entityType', params: { entityType } })
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  function stopStream() {
    abortRef.current?.abort()
    abortRef.current = null
    if (flushIntervalRef.current) { clearInterval(flushIntervalRef.current); flushIntervalRef.current = null }
    followActiveRef.current = false
    terminalRef.current = true
    setStreamedRecords([...streamBufferRef.current])
    setStreamStatus(s => (s === 'streaming' || s === 'draining' || s === 'tailing') ? 'stopped' : s)
  }

  function clearStream() {
    abortRef.current?.abort()
    abortRef.current = null
    if (flushIntervalRef.current) { clearInterval(flushIntervalRef.current); flushIntervalRef.current = null }
    followActiveRef.current = false
    terminalRef.current = false
    streamBufferRef.current = []
    setStreamedRecords([])
    setStreamError(null)
    setStreamStatus('idle')
  }

  function startStream() {
    if (streamMode === 'json') return
    clearStream()
    const isFollowing = followLive
    setStreamStatus(isFollowing ? 'draining' : 'streaming')
    flushIntervalRef.current = setInterval(() => {
      setStreamedRecords([...streamBufferRef.current])
    }, 250)
    const params: EntityStreamParams & { follow?: boolean } = {
      from: from || undefined,
      // Follow mode rejects bounded ranges server-side; the `to` input is
      // also greyed out in the UI. Guard both paths.
      to: isFollowing ? undefined : (to || undefined),
      follow: isFollowing || undefined,
      order,
    }
    const shouldPrependLive = isFollowing && order === 'desc'
    abortRef.current = streamEntityRecords(entityType, entityId, streamMode, params, {
      onRecord: (r) => {
        if (shouldPrependLive && followActiveRef.current) {
          streamBufferRef.current.unshift(r)
        } else {
          streamBufferRef.current.push(r)
        }
      },
      onStatus: (event) => {
        if (event === 'follow') {
          followActiveRef.current = true
          setStreamStatus('tailing')
        } else if (event === 'overflow') {
          terminalRef.current = true
          setStreamStatus('overflow')
        }
      },
      onDone: () => {
        if (flushIntervalRef.current) { clearInterval(flushIntervalRef.current); flushIntervalRef.current = null }
        setStreamedRecords([...streamBufferRef.current])
        if (terminalRef.current) return
        if (isFollowing) setStreamStatus('disconnected')
        else setStreamStatus('done')
      },
      onError: (e) => {
        if (flushIntervalRef.current) { clearInterval(flushIntervalRef.current); flushIntervalRef.current = null }
        setStreamedRecords([...streamBufferRef.current])
        setStreamError(e.message)
        setStreamStatus('error')
      },
    })
  }

  // Abort on unmount
  useEffect(() => () => {
    abortRef.current?.abort()
    if (flushIntervalRef.current) clearInterval(flushIntervalRef.current)
  }, [])

  // Stop stream when filters or sort change so data doesn't go stale
  useEffect(() => {
    abortRef.current?.abort()
    abortRef.current = null
    if (flushIntervalRef.current) { clearInterval(flushIntervalRef.current); flushIntervalRef.current = null }
    followActiveRef.current = false
    setStreamStatus(s => (s === 'streaming' || s === 'draining' || s === 'tailing') ? 'idle' : s)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [from, to, order])

  // Reset paged cursor state when the sort direction changes.
  useEffect(() => {
    setCursor(undefined)
    setCursors([])
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [order])

  const columns = [
    colHelper.accessor('timestamp', {
      header: 'Timestamp',
      cell: i => i.getValue().slice(0, 19).replace('T', ' '),
    }),
    colHelper.accessor('messageType', {
      header: 'Message Type',
      cell: i => i.getValue()
        ? <span style={{ background: '#ebf8ff', border: '1px solid #bee3f8', borderRadius: 4, padding: '1px 6px', fontSize: 12, whiteSpace: 'nowrap' }}>{i.getValue()}</span>
        : <span style={{ color: '#a0aec0' }}>—</span>,
    }),
    colHelper.accessor('topic', { header: 'Topic' }),
    colHelper.accessor('partition', { header: 'Part.' }),
    colHelper.accessor('offset', { header: 'Offset' }),
    colHelper.accessor('key', { header: 'Key', cell: i => trunc(i.getValue(), 30) }),
    colHelper.accessor('value', {
      header: 'Value',
      cell: i => <ValueCell raw={i.getValue()} />,
    }),
    colHelper.accessor('recordedAt', {
      header: 'Recorded At',
      cell: i => i.getValue().slice(0, 19).replace('T', ' '),
    }),
  ]

  const tableData = streamMode === 'json' ? (recordsQuery.data?.data ?? []) : streamedRecords

  const table = useReactTable({ data: tableData, columns, getCoreRowModel: getCoreRowModel() })

  function nextPage() {
    if (recordsQuery.data?.nextCursor) {
      setCursors(prev => [...prev, cursor ?? ''])
      setCursor(recordsQuery.data!.nextCursor)
    }
  }
  function prevPage() {
    const prev = cursors[cursors.length - 1]
    setCursors(c => c.slice(0, -1))
    setCursor(prev || undefined)
  }

  const stats = statsQuery.data

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
        <div>
          <div style={{ fontSize: 12, color: '#718096', marginBottom: 4 }}>{entityType}</div>
          <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>{entityId}</h1>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <Link
            to="/entities/$entityType/$entityId/timeline"
            params={{ entityType, entityId }}
            data-testid="btn-open-timeline"
            aria-label={`Open full timeline for ${entityId}`}
            style={{ padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', borderRadius: 4, fontSize: 14, textDecoration: 'none', fontWeight: 500 }}
          >
            <span aria-hidden="true">⏱</span> Timeline
          </Link>
          <button
            data-testid="btn-delete-entity"
            aria-label={`Delete all data for entity ${entityId}`}
            style={{ padding: '0.45rem 1rem', background: '#e53e3e', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }}
            onClick={() => setDeleteStep(1)}
          >
            Delete All Data (GDPR)
          </button>
        </div>
      </div>

      {statsQuery.isLoading && <LoadingSpinner />}
      {statsQuery.error && <ErrorMessage message={(statsQuery.error as Error).message} />}
      {stats && (
        <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem' }}>
          <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Stats</h3>
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: '0.75rem' }}>
            {[
              ['Messages', stats.messageCount.toLocaleString()],
              ['First Message', stats.firstMessage?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['Last Message', stats.lastMessage?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['First Seen', stats.firstSeen?.slice(0, 19).replace('T', ' ') ?? '—'],
              ['Last Seen', stats.lastSeen?.slice(0, 19).replace('T', ' ') ?? '—'],
            ].map(([k, v]) => (
              <div key={k} style={{ background: '#f7fafc', border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.5rem 0.85rem', minWidth: 140 }}>
                <div style={{ fontSize: 11, color: '#718096', marginBottom: 2 }}>{k}</div>
                <div style={{ fontSize: 14, fontWeight: 600 }}>{v}</div>
              </div>
            ))}
          </div>
          {Object.keys(stats.countByTopic).length > 0 && (
            <div>
              <div style={{ fontSize: 13, fontWeight: 600, color: '#4a5568', marginBottom: 4 }}>Messages by Topic</div>
              <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                {Object.entries(stats.countByTopic).map(([t, count]) => (
                  <div key={t} style={{ background: '#ebf8ff', border: '1px solid #bee3f8', borderRadius: 4, padding: '2px 10px', fontSize: 13 }}>
                    <strong>{t}</strong>: {count}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Replay to Topic panel */}
      <ReplayToTopicPanel
        mode="entity"
        entityType={entityType}
        entityId={entityId}
        totalCount={statsQuery.data?.messageCount}
        sourceTopics={sourcesQuery.data?.map(s => s.topic)}
      />

      {/* View mode switcher */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
        <ViewModeBar
          aria-label="Entity view"
          modes={[
            { id: 'records',  label: 'Records',  icon: '☰' },
            { id: 'diff',     label: 'Diff',     icon: '±' },
            { id: 'state',    label: 'State',    icon: '◉' },
            { id: 'portrait', label: 'Portrait', icon: '👤' },
            { id: 'sol',      label: 'SOL',       icon: '⌥' },
            { id: 'timeline', label: 'Timeline',  icon: '⏱' },
            { id: 'barcode',  label: 'Barcode',   icon: '▦' },
            { id: 'sequence', label: 'Sequence',  icon: '⛓' },
          ]}
          active={activeTab}
          onChange={setActiveTab}
        />
      </div>

      {/* SOL tab */}
      {activeTab === 'sol' && (
        <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem' }}>
          <SolQueryPanel
            mode="entity"
            entityType={entityType}
            entityId={entityId}
            from={from || undefined}
            to={to || undefined}
          />
        </div>
      )}

      {/* Timeline tab — link out to the existing timeline page */}
      {activeTab === 'timeline' && (
        <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem', textAlign: 'center' }}>
          <p style={{ margin: '0 0 12px', fontSize: 14, color: '#718096' }}>
            The full interactive timeline opens in its own page for keyboard navigation and zoom.
          </p>
          <Link
            to="/entities/$entityType/$entityId/timeline"
            params={{ entityType, entityId }}
            style={{ padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', borderRadius: 4, fontSize: 14, textDecoration: 'none', fontWeight: 500 }}
          >
            Open Timeline ⏱
          </Link>
        </div>
      )}

      {/* Barcode tab */}
      {activeTab === 'barcode' && (() => {
        const allRecords = streamMode === 'json' ? (recordsQuery.data?.data ?? []) : streamedRecords
        const messageTypes = [...new Set(allRecords.map(r => r.messageType).filter(Boolean) as string[])]
        const barcodeRow: BarcodeRow = { entityId, records: allRecords }
        return (
          <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, overflow: 'hidden', marginBottom: '1.5rem' }}>
            <div style={{ padding: '8px 16px', borderBottom: '1px solid #e2e8f0', display: 'flex', alignItems: 'center', gap: 12 }}>
              <div style={{ flex: 1 }}><BarcodeLegend messageTypes={messageTypes} /></div>
              <BarcodeXModeToggle value={barcodeXMode} onChange={setBarcodeXMode} />
            </div>
            {recordsQuery.isLoading && <div style={{ padding: 16 }}><LoadingSpinner /></div>}
            {!recordsQuery.isLoading && allRecords.length > 0 && (
              <SequenceBarcodeView rows={[barcodeRow]} xMode={barcodeXMode} colorMode="type" cellHeight={28} />
            )}
            {!recordsQuery.isLoading && allRecords.length === 0 && (
              <p style={{ padding: 16, color: '#a0aec0', fontSize: 13 }}>No records to display.</p>
            )}
          </div>
        )
      })()}

      {/* Sequence tab */}
      {activeTab === 'sequence' && (
        <div style={{ border: '1px solid #e2e8f0', borderRadius: 8, overflow: 'hidden', marginBottom: '1.5rem', height: 600 }}>
          <SequenceQueryPanel mode="entity" entityType={entityType} />
        </div>
      )}

      {/* State tab */}
      {activeTab === 'state' && (
        <div
          role="tabpanel"
          aria-label="Folded entity state"
          data-testid="tab-panel-state"
          style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem' }}
        >
          <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Current State</h3>
          {stateQuery.isLoading && <LoadingSpinner />}
          {stateQuery.error && <ErrorMessage message={(stateQuery.error as Error).message} />}
          {stateQuery.data && (
            <>
              <div style={{ display: 'flex', gap: 12, marginBottom: '0.75rem', flexWrap: 'wrap' }}>
                <div style={{ background: '#f7fafc', border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.5rem 0.85rem' }}>
                  <div style={{ fontSize: 11, color: '#718096', marginBottom: 2 }}>Events folded</div>
                  <div data-testid="state-event-count" style={{ fontSize: 14, fontWeight: 600 }}>{stateQuery.data.eventCount}</div>
                </div>
                {stateQuery.data.asOf && (
                  <div style={{ background: '#f7fafc', border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.5rem 0.85rem' }}>
                    <div style={{ fontSize: 11, color: '#718096', marginBottom: 2 }}>As of</div>
                    <div data-testid="state-as-of" style={{ fontSize: 14, fontWeight: 600 }}>{stateQuery.data.asOf.slice(0, 19).replace('T', ' ')}</div>
                  </div>
                )}
              </div>
              {stateQuery.data.state ? (
                <div data-testid="state-json">
                  <JsonView src={stateQuery.data.state as object} />
                </div>
              ) : (
                <p style={{ color: '#a0aec0', fontSize: 13, margin: 0 }}>No state — entity has no parseable event values.</p>
              )}
            </>
          )}
        </div>
      )}

      {/* Diff tab */}
      {activeTab === 'diff' && (
        <div
          role="tabpanel"
          aria-label="Event changelog"
          data-testid="tab-panel-diff"
          style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem' }}
        >
          <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Changelog</h3>
          {diffQuery.isLoading && <LoadingSpinner />}
          {diffQuery.error && <ErrorMessage message={(diffQuery.error as Error).message} />}
          {diffQuery.data && diffQuery.data.length === 0 && (
            <p style={{ color: '#a0aec0', fontSize: 13, margin: 0 }}>No events with parseable values in this range.</p>
          )}
          {diffQuery.data && diffQuery.data.length > 0 && (
            <table data-testid="diff-table" aria-label="Entity event changelog" style={tableStyle}>
              <thead>
                <tr>
                  <th style={thStyle}>Timestamp</th>
                  <th style={thStyle}>Message Type</th>
                  <th style={thStyle}>Changed Fields</th>
                  <th style={thStyle}>Before</th>
                  <th style={thStyle}>After</th>
                </tr>
              </thead>
              <tbody>
                {diffQuery.data.map((dr, idx) => (
                  <tr key={idx} data-testid={`diff-row-${idx}`}>
                    <td style={tdStyle}>{dr.event.timestamp.slice(0, 19).replace('T', ' ')}</td>
                    <td style={tdStyle}>
                      {dr.event.messageType
                        ? <span style={{ background: '#ebf8ff', border: '1px solid #bee3f8', borderRadius: 4, padding: '1px 6px', fontSize: 12 }}>{dr.event.messageType}</span>
                        : <span style={{ color: '#a0aec0' }}>—</span>}
                    </td>
                    <td style={tdStyle}>
                      {dr.changedFields
                        ? <span style={{ fontFamily: 'monospace', fontSize: 12, color: '#2d7d46' }}>{dr.changedFields.join(', ')}</span>
                        : <span style={{ color: '#a0aec0', fontSize: 12 }}>first event</span>}
                    </td>
                    <td style={tdStyle}>
                      {dr.before
                        ? <JsonView src={dr.before as object} />
                        : <span style={{ color: '#a0aec0', fontSize: 12 }}>—</span>}
                    </td>
                    <td style={tdStyle}>
                      {dr.event.value ? <ValueCell raw={dr.event.value} /> : <span style={{ color: '#a0aec0' }}>—</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}

      {/* Portrait tab */}
      {activeTab === 'portrait' && (
        <div
          role="tabpanel"
          aria-label="Entity portrait summary"
          data-testid="tab-panel-portrait"
          style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem' }}
        >
          <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Portrait</h3>
          {portraitQuery.isLoading && <LoadingSpinner />}
          {portraitQuery.error && <ErrorMessage message={(portraitQuery.error as Error).message} />}
          {portraitQuery.data && <PortraitPanel portrait={portraitQuery.data} />}
        </div>
      )}

      {/* Records tab */}
      {activeTab === 'records' && (
      <div role="tabpanel" aria-label="Replay records" data-testid="tab-panel-records" style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.75rem' }}>
          <h3 style={{ margin: 0, fontSize: 15 }}>Replay Records</h3>
          {/* Stream mode toggle */}
          <div role="group" aria-label="Stream mode" data-testid="stream-mode-toggle" style={{ display: 'flex', border: '1px solid #cbd5e0', borderRadius: 4, overflow: 'hidden' }}>
            {(['json', 'sse', 'ndjson'] as StreamMode[]).map((m, i) => (
              <button
                key={m}
                data-testid={`stream-mode-${m}`}
                aria-pressed={streamMode === m}
                aria-label={m === 'json' ? 'Paged mode' : `${m.toUpperCase()} streaming mode`}
                onClick={() => { clearStream(); setStreamMode(m) }}
                style={{
                  padding: '0.3rem 0.75rem',
                  background: streamMode === m ? '#3182ce' : '#fff',
                  color: streamMode === m ? '#fff' : '#4a5568',
                  border: 'none',
                  borderRight: i < 2 ? '1px solid #cbd5e0' : 'none',
                  cursor: 'pointer',
                  fontSize: 12,
                  fontWeight: streamMode === m ? 600 : 400,
                }}
              >
                {m === 'json' ? 'Paged' : m.toUpperCase()}
              </button>
            ))}
          </div>
        </div>

        <div style={{ display: 'flex', gap: 12, alignItems: 'center', marginBottom: '0.75rem', flexWrap: 'wrap' }}>
          <label style={{ fontSize: 13, fontWeight: 600, color: '#4a5568', marginRight: 4 }}>Order</label>
          <div style={{ display: 'flex', border: '1px solid #cbd5e0', borderRadius: 4, overflow: 'hidden' }}>
            {([
              ['desc', 'Latest first'],
              ['asc',  'Oldest first'],
            ] as Array<[Order, string]>).map(([val, label], i) => (
              <button
                key={val}
                onClick={() => setOrder(val)}
                style={{
                  padding: '0.3rem 0.75rem',
                  background: order === val ? '#3182ce' : '#fff',
                  color: order === val ? '#fff' : '#4a5568',
                  border: 'none',
                  borderRight: i < 1 ? '1px solid #cbd5e0' : 'none',
                  cursor: 'pointer',
                  fontSize: 12,
                  fontWeight: order === val ? 600 : 400,
                }}
              >
                {label}
              </button>
            ))}
          </div>
        </div>

        <div style={{ display: 'flex', gap: 10, marginBottom: '1rem', flexWrap: 'wrap' }}>
          {([
            ['From', fromRaw, setFromRaw, false],
            ['To', toRaw, setToRaw, true],
          ] as Array<[string, string, React.Dispatch<React.SetStateAction<string>>, boolean]>).map(([label, val, setter, disabledByFollow]) => {
            const disabled = disabledByFollow && followLive && streamMode !== 'json'
            return (
              <div key={label}>
                <label style={{ ...labelStyle, color: disabled ? '#a0aec0' : labelStyle.color }}>{label}</label>
                <input
                  type="datetime-local"
                  disabled={disabled}
                  title={disabled ? 'Disabled while following live — bounded ranges are incompatible with follow mode' : undefined}
                  style={{
                    padding: '0.4rem 0.6rem',
                    border: '1px solid #cbd5e0',
                    borderRadius: 4,
                    fontSize: 14,
                    width: 200,
                    ...(disabled ? { background: '#f7fafc', color: '#a0aec0', cursor: 'not-allowed' } : null),
                  }}
                  value={val}
                  onChange={e => setter(e.target.value)}
                />
              </div>
            )
          })}
        </div>

        {/* Streaming controls */}
        {streamMode !== 'json' && (
          <div style={{ display: 'flex', gap: 12, alignItems: 'center', marginBottom: '0.75rem', flexWrap: 'wrap' }}>
            {!isStreamActive(streamStatus) ? (
              <button data-testid="btn-start-stream" aria-label="Start streaming" style={primaryBtnStyle} onClick={startStream}>Start Streaming</button>
            ) : (
              <button data-testid="btn-stop-stream" aria-label="Stop streaming" style={{ ...primaryBtnStyle, background: '#e53e3e' }} onClick={stopStream}>Stop</button>
            )}
            {streamedRecords.length > 0 && !isStreamActive(streamStatus) && (
              <button data-testid="btn-clear-stream" aria-label="Clear streamed records" style={secondaryBtnStyle} onClick={clearStream}>Clear</button>
            )}
            <label style={{ display: 'inline-flex', alignItems: 'center', gap: 6, fontSize: 13, color: '#4a5568', cursor: isStreamActive(streamStatus) ? 'not-allowed' : 'pointer' }}>
              <input
                data-testid="checkbox-follow-live"
                type="checkbox"
                checked={followLive}
                onChange={e => setFollowLive(e.target.checked)}
                disabled={isStreamActive(streamStatus)}
                aria-label="Follow live updates"
              />
              Follow live
            </label>
            <StreamStatusBadge
              status={streamStatus}
              count={streamedRecords.length}
              errorMessage={streamError}
            />
          </div>
        )}

        {streamMode === 'json' && recordsQuery.isLoading && <LoadingSpinner />}
        {streamMode === 'json' && recordsQuery.error && <ErrorMessage message={(recordsQuery.error as Error).message} />}
        {(streamMode !== 'json' || !recordsQuery.isLoading) && (
          <>
            <table data-testid="records-table" aria-label="Entity cassette records" style={tableStyle}>
              <thead>
                {table.getHeaderGroups().map(hg => (
                  <tr key={hg.id}>{hg.headers.map(h => <th key={h.id} style={thStyle}>{flexRender(h.column.columnDef.header, h.getContext())}</th>)}</tr>
                ))}
              </thead>
              <tbody>
                {table.getRowModel().rows.map((row, rowIdx) => (
                  <tr key={row.id} data-testid={`record-row-${rowIdx}`}>
                    {row.getVisibleCells().map(cell => <td key={cell.id} style={tdStyle}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
            {streamMode === 'json' && (
              <div style={{ display: 'flex', gap: 8, marginTop: '0.75rem', alignItems: 'center' }}>
                <button data-testid="btn-prev-page" aria-label="Previous page" style={secondaryBtnStyle} disabled={cursors.length === 0} onClick={prevPage}>← Prev</button>
                <button data-testid="btn-next-page" aria-label="Next page" style={secondaryBtnStyle} disabled={!recordsQuery.data?.hasMore} onClick={nextPage}>Next →</button>
                <span data-testid="record-count" style={{ fontSize: 13, color: '#718096' }}>{recordsQuery.data?.data.length ?? 0} records</span>
              </div>
            )}
          </>
        )}
      </div>
      )}

      {deleteStep === 1 && (
        <ConfirmDialog
          message={`Delete all data for entity "${entityId}"? This cannot be undone.`}
          onConfirm={() => setDeleteStep(2)}
          onCancel={() => setDeleteStep(0)}
        />
      )}
      {deleteStep === 2 && (
        <ConfirmDialog
          message={`Final confirmation: permanently delete all records for entity "${entityId}"? This is irreversible and cannot be recovered.`}
          onConfirm={() => { deleteMutation.mutate(); setDeleteStep(0) }}
          onCancel={() => setDeleteStep(0)}
        />
      )}
    </Layout>
  )
}

const labelStyle: React.CSSProperties = { display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#4a5568' }
const primaryBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const secondaryBtnStyle: React.CSSProperties = { padding: '0.35rem 0.8rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 13 }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', background: '#fff', fontSize: 13 }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.5rem 0.6rem', background: '#edf2f7', fontWeight: 600, color: '#4a5568', borderBottom: '1px solid #e2e8f0' }
const tdStyle: React.CSSProperties = { padding: '0.45rem 0.6rem', borderBottom: '1px solid #e2e8f0' }


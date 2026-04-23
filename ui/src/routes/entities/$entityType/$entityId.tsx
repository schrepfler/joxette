import { createFileRoute, useNavigate, Link } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { useState, useRef, useEffect } from 'react'
import { VisualJson, TreeView, type JsonValue } from '@visual-json/react'
import { cassettesApi, streamEntityRecords, type EntityRecord, type Order, type StreamMode, type EntityStreamParams } from '../../../api/client'
import { Layout } from '../../../components/Layout'
import { LoadingSpinner } from '../../../components/LoadingSpinner'
import { ErrorMessage } from '../../../components/ErrorMessage'
import { ConfirmDialog } from '../../../components/ConfirmDialog'
import { ReplayToTopicPanel } from '../../../components/ReplayToTopicPanel'
import { SequenceQueryPanel } from '../../../components/SequenceQueryPanel'
import { StreamStatusBadge, type StreamStatus } from '../../../components/StreamStatusBadge'
import { useToast } from '../../../components/Toast'
import { useDebounce } from '../../../hooks/useDebounce'
import type { FragmentDefinition } from '../../../transforms/types'

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

function tryParseValue(s: string | null): { parsed: JsonValue; raw: string } | null {
  if (!s) return null
  try { return { parsed: JSON.parse(s) as JsonValue, raw: s } } catch { /* continue */ }
  const decoded = tryDecodeBase64(s)
  if (decoded) {
    try { return { parsed: JSON.parse(decoded) as JsonValue, raw: decoded } } catch { /* continue */ }
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
      {open && (
        <div style={vjTheme}>
          <VisualJson value={result.parsed}>
            <TreeView showValues showCounts />
          </VisualJson>
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
  const [activeTab, setActiveTab] = useState<'records' | 'sequence'>('records')
  const [_replayPipelineFragments, setReplayPipelineFragments] = useState<FragmentDefinition[]>([])

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
            style={{ padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', borderRadius: 4, fontSize: 14, textDecoration: 'none', fontWeight: 500 }}
          >
            ⏱ Timeline
          </Link>
          <button
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
      />

      {/* Tab bar */}
      <div style={{ display: 'flex', borderBottom: '1px solid #e2e8f0', marginBottom: '1rem', gap: 0 }}>
        {(['records', 'sequence'] as const).map(tab => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            style={{
              padding: '0.5rem 1.25rem',
              background: 'none',
              border: 'none',
              borderBottom: activeTab === tab ? '2px solid #3182ce' : '2px solid transparent',
              cursor: 'pointer',
              fontSize: 14,
              fontWeight: activeTab === tab ? 600 : 400,
              color: activeTab === tab ? '#3182ce' : '#718096',
              marginBottom: -1,
            }}
          >
            {tab === 'records' ? 'Records' : 'Sequence'}
          </button>
        ))}
      </div>

      {/* Sequence tab */}
      {activeTab === 'sequence' && (
        <div style={{ marginBottom: '1.5rem' }}>
          <SequenceQueryPanel
            mode="entity"
            entityType={entityType}
            entityId={entityId}
            onSaveFragment={(frag) => setReplayPipelineFragments(fs => [...fs, frag])}
          />
        </div>
      )}

      {/* Records tab */}
      {activeTab === 'records' && (
      <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.75rem' }}>
          <h3 style={{ margin: 0, fontSize: 15 }}>Replay Records</h3>
          {/* Stream mode toggle */}
          <div style={{ display: 'flex', border: '1px solid #cbd5e0', borderRadius: 4, overflow: 'hidden' }}>
            {(['json', 'sse', 'ndjson'] as StreamMode[]).map((m, i) => (
              <button
                key={m}
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
              <button style={primaryBtnStyle} onClick={startStream}>Start Streaming</button>
            ) : (
              <button style={{ ...primaryBtnStyle, background: '#e53e3e' }} onClick={stopStream}>Stop</button>
            )}
            {streamedRecords.length > 0 && !isStreamActive(streamStatus) && (
              <button style={secondaryBtnStyle} onClick={clearStream}>Clear</button>
            )}
            <label style={{ display: 'inline-flex', alignItems: 'center', gap: 6, fontSize: 13, color: '#4a5568', cursor: isStreamActive(streamStatus) ? 'not-allowed' : 'pointer' }}>
              <input
                type="checkbox"
                checked={followLive}
                onChange={e => setFollowLive(e.target.checked)}
                disabled={isStreamActive(streamStatus)}
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
            <table style={tableStyle}>
              <thead>
                {table.getHeaderGroups().map(hg => (
                  <tr key={hg.id}>{hg.headers.map(h => <th key={h.id} style={thStyle}>{flexRender(h.column.columnDef.header, h.getContext())}</th>)}</tr>
                ))}
              </thead>
              <tbody>
                {table.getRowModel().rows.map(row => (
                  <tr key={row.id}>
                    {row.getVisibleCells().map(cell => <td key={cell.id} style={tdStyle}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
            {streamMode === 'json' && (
              <div style={{ display: 'flex', gap: 8, marginTop: '0.75rem', alignItems: 'center' }}>
                <button style={secondaryBtnStyle} disabled={cursors.length === 0} onClick={prevPage}>← Prev</button>
                <button style={secondaryBtnStyle} disabled={!recordsQuery.data?.hasMore} onClick={nextPage}>Next →</button>
                <span style={{ fontSize: 13, color: '#718096' }}>{recordsQuery.data?.data.length ?? 0} records</span>
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

// ── @visual-json/react theme ───────────────────────────────────────────────────
const vjTheme: React.CSSProperties = {
  marginTop: 4,
  borderRadius: 6,
  overflow: 'hidden',
  border: '1px solid #cbd5e0',
  // Base colours
  ['--vj-bg' as string]: '#f7fafc',
  ['--vj-text' as string]: '#1a202c',
  ['--vj-text-muted' as string]: '#4a5568',
  // Selected row: light blue + dark text
  ['--vj-text-selected' as string]: '#1a202c',
  ['--vj-bg-hover' as string]: '#ebf8ff',
  ['--vj-bg-selected' as string]: '#bee3f8',
  ['--vj-bg-selected-muted' as string]: '#e6f6ff',
  // Search/match highlights
  ['--vj-bg-match' as string]: '#fefcbf',
  ['--vj-bg-match-active' as string]: '#f6e05e',
  // Inline button bar (copy/expand)
  ['--vj-btn-bg' as string]: '#ffffff',
  ['--vj-btn-text' as string]: '#2d3748',
  ['--vj-btn-bg-hover' as string]: '#edf2f7',
  // Popup / context menu
  ['--vj-menu-bg' as string]: '#ffffff',
  ['--vj-menu-text' as string]: '#1a202c',
  ['--vj-menu-bg-hover' as string]: '#edf2f7',
  ['--vj-menu-text-hover' as string]: '#1a202c',
  ['--vj-menu-border' as string]: '#e2e8f0',
  ['--vj-menu-shadow' as string]: '0 4px 16px rgba(0,0,0,0.12)',
  // Accent & value colours
  ['--vj-accent' as string]: '#3182ce',
  ['--vj-string' as string]: '#276749',
  ['--vj-number' as string]: '#2b6cb0',
  ['--vj-font' as string]: '"Manrope", ui-sans-serif, system-ui, sans-serif',
}

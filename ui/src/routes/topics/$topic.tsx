import { createFileRoute, Link } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { useForm } from '@tanstack/react-form'
import { useState, useRef, useEffect } from 'react'
import { VisualJson, TreeView, type JsonValue } from '@visual-json/react'
import { topicsApi, brokersApi, cassettesApi, streamTopicRecords, type CassetteRecord, type StreamMode, type TopicStreamParams, type TopicMatcherConfig, type AddMatcherRequest } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { TruncateDialog } from '../../components/TruncateDialog'
import { ConfirmDialog } from '../../components/ConfirmDialog'
import { ReplayToTopicPanel } from '../../components/ReplayToTopicPanel'
import { useToast } from '../../components/Toast'
import { useDebounce } from '../../hooks/useDebounce'

// ── JSON viewer ────────────────────────────────────────────────────────────────
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

  // Use the decoded JSON string for the preview, not the raw (possibly base64) value
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

export const Route = createFileRoute('/topics/$topic')({
  component: TopicDetailPage,
})

const colHelper = createColumnHelper<CassetteRecord>()
const matcherColHelper = createColumnHelper<TopicMatcherConfig>()

function AddMatcherModal({ topic, onClose }: { topic: string; onClose: () => void }) {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const mutation = useMutation({
    mutationFn: (d: AddMatcherRequest) => topicsApi.addMatcher(topic, d),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['topics', topic, 'matchers'] })
      addToast('Matcher added', 'success')
      onClose()
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })
  const form = useForm({
    defaultValues: { messageType: '', idSource: 'value', idExpression: '' },
    onSubmit: async ({ value }) => mutation.mutate(value),
  })
  return (
    <div style={overlayStyle} onClick={onClose}>
      <div style={modalStyle} onClick={e => e.stopPropagation()}>
        <h2 style={{ margin: '0 0 1.25rem', fontSize: 18 }}>Add Matcher</h2>
        <form onSubmit={e => { e.preventDefault(); void form.handleSubmit() }}>
          <form.Field name="messageType">
            {(f) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>Message Type *</label>
                <input style={inputStyleFull} value={f.state.value} onChange={e => f.handleChange(e.target.value)} required />
              </div>
            )}
          </form.Field>
          <form.Field name="idSource">
            {(f) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>ID Source</label>
                <select style={inputStyleFull} value={f.state.value} onChange={e => f.handleChange(e.target.value)}>
                  <option value="value">value</option>
                  <option value="key">key</option>
                  <option value="header">header</option>
                </select>
              </div>
            )}
          </form.Field>
          <form.Field name="idExpression">
            {(f) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>ID Expression *</label>
                <input style={inputStyleFull} placeholder="e.g. $.order_id" value={f.state.value} onChange={e => f.handleChange(e.target.value)} required />
              </div>
            )}
          </form.Field>
          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '0.5rem' }}>
            <button type="button" onClick={onClose} style={cancelBtnStyle}>Cancel</button>
            <button type="submit" disabled={mutation.isPending} style={primaryBtnStyle}>
              {mutation.isPending ? 'Adding…' : 'Add'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

const trunc = (s: string | null, n: number) =>
  s == null ? '—' : s.length > n ? s.slice(0, n) + '…' : s

function formatBytes(b: number) {
  if (b < 1024) return `${b} B`
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`
  return `${(b / 1024 / 1024).toFixed(1)} MB`
}

function TopicDetailPage() {
  const { topic } = Route.useParams()
  const qc = useQueryClient()
  const { addToast } = useToast()

  // Filter state (raw)
  const [fromRaw, setFromRaw] = useState('')
  const [toRaw, setToRaw] = useState('')
  const [partitionRaw, setPartitionRaw] = useState('')
  const [offsetFromRaw, setOffsetFromRaw] = useState('')
  const [offsetToRaw, setOffsetToRaw] = useState('')
  const [cursor, setCursor] = useState<string | undefined>()
  const [cursors, setCursors] = useState<string[]>([])
  const [showTruncateDialog, setShowTruncateDialog] = useState(false)
  const [showAddMatcher, setShowAddMatcher] = useState(false)
  const [confirmDeleteMatcher, setConfirmDeleteMatcher] = useState<string | null>(null)

  // Streaming state
  const [streamMode, setStreamMode] = useState<StreamMode>('json')
  const [streamStatus, setStreamStatus] = useState<'idle' | 'streaming' | 'done' | 'error'>('idle')
  const [streamedRecords, setStreamedRecords] = useState<CassetteRecord[]>([])
  const [streamError, setStreamError] = useState<string | null>(null)
  const abortRef = useRef<AbortController | null>(null)
  const streamBufferRef = useRef<CassetteRecord[]>([])
  const flushIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Debounced filters
  const from = useDebounce(fromRaw, 300)
  const to = useDebounce(toRaw, 300)
  const partition = useDebounce(partitionRaw, 300)
  const offsetFrom = useDebounce(offsetFromRaw, 300)
  const offsetTo = useDebounce(offsetToRaw, 300)

  const topicQuery = useQuery({
    queryKey: ['topics', topic],
    queryFn: () => topicsApi.get(topic),
  })

  const matchersQuery = useQuery({
    queryKey: ['topics', topic, 'matchers'],
    queryFn: () => topicsApi.listMatchers(topic),
  })

  const deleteMatcherMutation = useMutation({
    mutationFn: (messageType: string) => topicsApi.deleteMatcher(topic, messageType),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['topics', topic, 'matchers'] }); addToast('Matcher deleted', 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const statsQuery = useQuery({
    queryKey: ['cassettes', 'topics', topic, 'stats'],
    queryFn: () => cassettesApi.getTopicStats(topic),
  })

  const recordsQuery = useQuery({
    queryKey: ['cassettes', 'topics', topic, 'records', { from, to, partition, offsetFrom, offsetTo, cursor }],
    queryFn: () =>
      cassettesApi.getTopicRecords(topic, {
        from: from || undefined,
        to: to || undefined,
        partition: partition ? Number(partition) : undefined,
        offset_from: offsetFrom ? Number(offsetFrom) : undefined,
        offset_to: offsetTo ? Number(offsetTo) : undefined,
        cursor,
        limit: 50,
      }),
  })

  const { data: brokers = [] } = useQuery({ queryKey: ['brokers'], queryFn: brokersApi.list })

  const updateMutation = useMutation({
    mutationFn: (req: { mode: string; brokerId?: string | null }) => topicsApi.update(topic, req),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['topics', topic] }); addToast('Topic updated', 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const retentionMutation = useMutation({
    mutationFn: (days: number) => topicsApi.updateRetention(topic, days),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['topics', topic] }); addToast('Retention updated', 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const compactMutation = useMutation({
    mutationFn: () => cassettesApi.compactTopic(topic),
    onSuccess: () => addToast('Compaction started', 'success'),
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const truncateMutation = useMutation({
    mutationFn: (before: string) => cassettesApi.truncateTopic(topic, before),
    onSuccess: (d) => { void qc.invalidateQueries({ queryKey: ['cassettes', 'topics', topic] }); addToast(`Deleted ${d.deleted} records`, 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const form = useForm({
    defaultValues: { mode: topicQuery.data?.mode ?? '', brokerId: topicQuery.data?.brokerId ?? '' },
    onSubmit: async ({ value }) => updateMutation.mutate({ mode: value.mode, brokerId: value.brokerId || null }),
  })

  const retentionForm = useForm({
    defaultValues: { retentionDays: topicQuery.data?.retentionDays ?? 0 },
    onSubmit: async ({ value }) => retentionMutation.mutate(value.retentionDays),
  })

  function stopStream() {
    abortRef.current?.abort()
    abortRef.current = null
    if (flushIntervalRef.current) { clearInterval(flushIntervalRef.current); flushIntervalRef.current = null }
    setStreamStatus(s => s === 'streaming' ? 'idle' : s)
  }

  function clearStream() {
    abortRef.current?.abort()
    abortRef.current = null
    if (flushIntervalRef.current) { clearInterval(flushIntervalRef.current); flushIntervalRef.current = null }
    streamBufferRef.current = []
    setStreamedRecords([])
    setStreamError(null)
    setStreamStatus('idle')
  }

  function startStream() {
    if (streamMode === 'json') return
    clearStream()
    setStreamStatus('streaming')
    flushIntervalRef.current = setInterval(() => {
      setStreamedRecords([...streamBufferRef.current])
    }, 250)
    const params: TopicStreamParams = {
      from: from || undefined,
      to: to || undefined,
      partition: partition ? Number(partition) : undefined,
      offset_from: offsetFrom ? Number(offsetFrom) : undefined,
      offset_to: offsetTo ? Number(offsetTo) : undefined,
    }
    abortRef.current = streamTopicRecords(topic, streamMode, params, {
      onRecord: (r) => { streamBufferRef.current.push(r) },
      onDone: () => {
        if (flushIntervalRef.current) { clearInterval(flushIntervalRef.current); flushIntervalRef.current = null }
        setStreamedRecords([...streamBufferRef.current])
        setStreamStatus('done')
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

  // Stop stream when filters change so the user knows the data is stale
  useEffect(() => {
    abortRef.current?.abort()
    abortRef.current = null
    if (flushIntervalRef.current) { clearInterval(flushIntervalRef.current); flushIntervalRef.current = null }
    setStreamStatus(s => s === 'streaming' ? 'idle' : s)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [from, to, partition, offsetFrom, offsetTo])

  const matcherColumns = [
    matcherColHelper.accessor('messageType', { header: 'Message Type' }),
    matcherColHelper.accessor('idSource', { header: 'ID Source' }),
    matcherColHelper.accessor('idExpression', { header: 'ID Expression' }),
    matcherColHelper.display({
      id: 'actions', header: 'Actions',
      cell: ({ row }) => (
        <button style={dangerBtnSmall} onClick={() => setConfirmDeleteMatcher(row.original.messageType)}>Delete</button>
      ),
    }),
  ]

  const matcherTable = useReactTable({
    data: matchersQuery.data ?? [],
    columns: matcherColumns,
    getCoreRowModel: getCoreRowModel(),
  })

  const columns = [
    colHelper.accessor('timestamp', { header: 'Timestamp', cell: i => i.getValue().slice(0, 19).replace('T', ' ') }),
    colHelper.accessor('partition', { header: 'Partition' }),
    colHelper.accessor('offset', { header: 'Offset' }),
    colHelper.accessor('key', { header: 'Key', cell: i => trunc(i.getValue(), 40) }),
    colHelper.accessor('value', { header: 'Value', cell: i => <ValueCell raw={i.getValue()} /> }),
    colHelper.accessor('recordedAt', { header: 'Recorded At', cell: i => i.getValue().slice(0, 19).replace('T', ' ') }),
  ]

  const tableData = streamMode === 'json' ? (recordsQuery.data?.data ?? []) : streamedRecords

  const table = useReactTable({
    data: tableData,
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

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

  return (
    <Layout>
      {topicQuery.isLoading && <LoadingSpinner />}
      {topicQuery.error && <ErrorMessage message={(topicQuery.error as Error).message} />}
      {topicQuery.data && (
        <>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: '1.5rem' }}>
            <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>{topic}</h1>
            <span style={{ background: topicQuery.data.paused ? '#fed7d7' : '#c6f6d5', color: topicQuery.data.paused ? '#9b2c2c' : '#276749', padding: '2px 8px', borderRadius: 12, fontSize: 12 }}>
              {topicQuery.data.paused ? 'Paused' : 'Active'}
            </span>
          </div>

          {/* Stats */}
          {statsQuery.data && (
            <div style={{ display: 'flex', gap: 16, marginBottom: '1.5rem', flexWrap: 'wrap' }}>
              {[
                ['Records', statsQuery.data.rowCount.toLocaleString()],
                ['Size', formatBytes(statsQuery.data.estimatedSizeBytes)],
                ['Table', statsQuery.data.tableName],
              ].map(([k, v]) => (
                <div key={k} style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.6rem 1rem', minWidth: 120 }}>
                  <div style={{ fontSize: 11, color: '#718096', marginBottom: 2 }}>{k}</div>
                  <div style={{ fontSize: 16, fontWeight: 600 }}>{v}</div>
                </div>
              ))}
            </div>
          )}

          {/* Edit form */}
          <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem' }}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Edit Topic</h3>
            <form onSubmit={(e) => { e.preventDefault(); void form.handleSubmit() }} style={{ display: 'flex', gap: 12, alignItems: 'flex-end', flexWrap: 'wrap' }}>
              <form.Field name="mode">
                {(field) => (
                  <div>
                    <label style={labelStyle}>Mode</label>
                    <select style={{ ...inputStyle, width: 160 }} value={field.state.value} onChange={e => field.handleChange(e.target.value)}>
                      <option value="general">general</option>
                      <option value="entity_only">entity_only</option>
                      <option value="both">both</option>
                    </select>
                  </div>
                )}
              </form.Field>
              <form.Field name="brokerId">
                {(field) => (
                  <div>
                    <label style={labelStyle}>Broker</label>
                    <select style={{ ...inputStyle, width: 220 }} value={field.state.value ?? ''} onChange={e => field.handleChange(e.target.value)}>
                      <option value="">(default)</option>
                      {brokers.map(b => (
                        <option key={b.brokerId} value={b.brokerId}>{b.brokerId} — {b.bootstrapServers}</option>
                      ))}
                    </select>
                  </div>
                )}
              </form.Field>
              <button type="submit" disabled={updateMutation.isPending} style={primaryBtnStyle}>Save</button>
            </form>
            <div style={{ borderTop: '1px solid #e2e8f0', marginTop: '0.75rem', paddingTop: '0.75rem' }}>
              <p style={{ margin: '0 0 0.5rem', fontSize: 13, color: '#718096' }}>
                {topicQuery.data.retentionDays
                  ? `Data retained for ${topicQuery.data.retentionDays} days`
                  : 'No retention limit (unlimited)'}
              </p>
              <form onSubmit={e => { e.preventDefault(); void retentionForm.handleSubmit() }} style={{ display: 'flex', gap: 12, alignItems: 'flex-end' }}>
                <retentionForm.Field name="retentionDays">
                  {(field) => (
                    <div>
                      <label style={labelStyle}>Retention (days, 0 = unlimited)</label>
                      <input
                        type="number" min="0"
                        style={{ ...inputStyle, width: 120 }}
                        value={field.state.value}
                        onChange={e => field.handleChange(Number(e.target.value))}
                      />
                    </div>
                  )}
                </retentionForm.Field>
                <button type="submit" disabled={retentionMutation.isPending} style={primaryBtnStyle}>
                  {retentionMutation.isPending ? 'Saving…' : 'Save'}
                </button>
              </form>
            </div>
          </div>

          {/* Matchers */}
          <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
              <h3 style={{ margin: 0, fontSize: 15 }}>Matchers</h3>
              <button style={primaryBtnStyle} onClick={() => setShowAddMatcher(true)}>+ Add Matcher</button>
            </div>
            <p style={{ margin: '0 0 0.75rem', fontSize: 13, color: '#718096' }}>
              Tag messages with a <code>messageType</code> by extracting an ID from the message header, key, or value. First matching rule wins.
            </p>
            {matchersQuery.isLoading && <LoadingSpinner />}
            {matchersQuery.error && <ErrorMessage message={(matchersQuery.error as Error).message} />}
            {!matchersQuery.isLoading && (
              <table style={tableStyle}>
                <thead>
                  {matcherTable.getHeaderGroups().map(hg => (
                    <tr key={hg.id}>{hg.headers.map(h => <th key={h.id} style={thStyle}>{flexRender(h.column.columnDef.header, h.getContext())}</th>)}</tr>
                  ))}
                </thead>
                <tbody>
                  {matcherTable.getRowModel().rows.length === 0 && (
                    <tr><td colSpan={4} style={{ ...tdStyle, color: '#a0aec0', fontStyle: 'italic' }}>No matchers configured</td></tr>
                  )}
                  {matcherTable.getRowModel().rows.map(row => (
                    <tr key={row.id}>
                      {row.getVisibleCells().map(cell => <td key={cell.id} style={tdStyle}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* Actions */}
          <div style={{ display: 'flex', gap: 8, marginBottom: '1.5rem', flexWrap: 'wrap', alignItems: 'center' }}>
            <Link
              to="/topics/$topic/timeline"
              params={{ topic }}
              style={{ ...primaryBtnStyle, background: '#3182ce', textDecoration: 'none', display: 'inline-block' }}
            >
              ⏱ Timeline
            </Link>
            <button style={{ ...primaryBtnStyle, background: '#805ad5' }} onClick={() => compactMutation.mutate()} disabled={compactMutation.isPending}>
              {compactMutation.isPending ? 'Compacting…' : 'Compact'}
            </button>
            <button style={{ ...primaryBtnStyle, background: '#e53e3e' }} onClick={() => setShowTruncateDialog(true)}>
              Truncate
            </button>
          </div>

          {/* Replay to Topic panel */}
          <ReplayToTopicPanel
            mode="topic"
            topic={topic}
            from={from || undefined}
            to={to || undefined}
            totalCount={statsQuery.data?.rowCount}
          />

          {/* Replay panel */}
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

            {/* Filters */}
            <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginBottom: '1rem' }}>
              {[
                ['From', fromRaw, setFromRaw, 'datetime-local'],
                ['To', toRaw, setToRaw, 'datetime-local'],
                ['Partition', partitionRaw, setPartitionRaw, 'number'],
                ['Offset From', offsetFromRaw, setOffsetFromRaw, 'number'],
                ['Offset To', offsetToRaw, setOffsetToRaw, 'number'],
              ].map(([label, val, setter, type]) => (
                <div key={label as string}>
                  <label style={labelStyle}>{label as string}</label>
                  <input
                    type={type as string}
                    style={{ ...inputStyle, width: 170 }}
                    value={val as string}
                    onChange={e => (setter as React.Dispatch<React.SetStateAction<string>>)(e.target.value)}
                  />
                </div>
              ))}
            </div>

            {/* Streaming controls */}
            {streamMode !== 'json' && (
              <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: '0.75rem' }}>
                {streamStatus !== 'streaming' ? (
                  <button style={primaryBtnStyle} onClick={startStream}>Start Streaming</button>
                ) : (
                  <button style={{ ...primaryBtnStyle, background: '#e53e3e' }} onClick={stopStream}>Stop</button>
                )}
                {streamedRecords.length > 0 && streamStatus !== 'streaming' && (
                  <button style={secondaryBtnStyle} onClick={clearStream}>Clear</button>
                )}
                <span style={{
                  fontSize: 13,
                  color: streamStatus === 'done' ? '#276749' : streamStatus === 'error' ? '#e53e3e' : '#718096',
                }}>
                  {streamStatus === 'streaming' && `● Receiving\u2026 ${streamedRecords.length.toLocaleString()} records`}
                  {streamStatus === 'done' && `\u2713 Complete \u2014 ${streamedRecords.length.toLocaleString()} records`}
                  {streamStatus === 'error' && streamError && `\u2717 ${streamError}`}
                </span>
              </div>
            )}

            {streamMode === 'json' && recordsQuery.isLoading && <LoadingSpinner />}
            {streamMode === 'json' && recordsQuery.error && <ErrorMessage message={(recordsQuery.error as Error).message} />}
            {(streamMode !== 'json' || !recordsQuery.isLoading) && (
              <>
                <table style={tableStyle}>
                  <thead>
                    {table.getHeaderGroups().map(hg => (
                      <tr key={hg.id}>
                        {hg.headers.map(h => <th key={h.id} style={thStyle}>{flexRender(h.column.columnDef.header, h.getContext())}</th>)}
                      </tr>
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
        </>
      )}
      {showTruncateDialog && (
        <TruncateDialog
          label={`topic "${topic}"`}
          onConfirm={(before) => { truncateMutation.mutate(before); setShowTruncateDialog(false) }}
          onCancel={() => setShowTruncateDialog(false)}
        />
      )}
      {showAddMatcher && <AddMatcherModal topic={topic} onClose={() => setShowAddMatcher(false)} />}
      {confirmDeleteMatcher && (
        <ConfirmDialog
          message={`Delete matcher "${confirmDeleteMatcher}"?`}
          onConfirm={() => { deleteMatcherMutation.mutate(confirmDeleteMatcher); setConfirmDeleteMatcher(null) }}
          onCancel={() => setConfirmDeleteMatcher(null)}
        />
      )}
    </Layout>
  )
}

const labelStyle: React.CSSProperties = { display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#4a5568' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14 }
const inputStyleFull: React.CSSProperties = { width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14, boxSizing: 'border-box' }
const primaryBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const cancelBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const secondaryBtnStyle: React.CSSProperties = { padding: '0.35rem 0.8rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 13 }
const dangerBtnSmall: React.CSSProperties = { padding: '0.2rem 0.6rem', background: '#e53e3e', color: '#fff', border: 'none', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', background: '#fff', fontSize: 13 }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.5rem 0.6rem', background: '#edf2f7', fontWeight: 600, color: '#4a5568', borderBottom: '1px solid #e2e8f0' }
const tdStyle: React.CSSProperties = { padding: '0.45rem 0.6rem', borderBottom: '1px solid #e2e8f0' }
const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem 2rem', minWidth: 380, boxShadow: '0 10px 30px rgba(0,0,0,0.2)' }
const fieldWrap: React.CSSProperties = { marginBottom: '0.75rem' }

// ── @visual-json/react theme — mapped to site design tokens ───────────────────
// --vj-bg           : tree background   → foam (light) / foam (dark) via CSS var
// --vj-text         : default text      → sea-ink
// --vj-text-muted   : muted text        → sea-ink-soft
// --vj-bg-hover     : row hover         → light tint of lagoon
// --vj-bg-selected  : selected row      → lagoon-deep
// --vj-accent       : drag/focus accent → lagoon
// --vj-string       : string values     → palm (earthy green)
// --vj-number       : number values     → lagoon-deep
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

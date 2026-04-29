import { createFileRoute, Link, useNavigate } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { useForm } from '@tanstack/react-form'
import { useState, useRef, useEffect, type CSSProperties } from 'react'
import ReactJson from '@microlink/react-json-view'
import {
  topicsApi,
  brokersApi,
  cassettesApi,
  streamTopicRecords,
  type CassetteRecord,
  type Order,
  type StreamMode,
  type TopicStreamParams,
  type TopicMatcherConfig,
  type AddMatcherRequest,
} from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { TruncateDialog } from '../../components/TruncateDialog'
import { ConfirmDialog } from '../../components/ConfirmDialog'
import { ReplayToTopicPanel } from '../../components/ReplayToTopicPanel'
import { SequenceQueryPanel } from '../../components/SequenceQueryPanel'
import { StreamStatusBadge, type StreamStatus } from '../../components/StreamStatusBadge'
import { useToast } from '../../components/Toast'
import { useDebounce } from '../../hooks/useDebounce'
import { Button, Input, Select, Hairline, Tabular, Badge, StatusDot } from '../../design/primitives'
import type { FragmentDefinition } from '../../transforms/types'

// ── JSON viewer ────────────────────────────────────────────────────────────────
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
  try { return { parsed: JSON.parse(s) as JsonValue, raw: s } } catch { /* continue */ }
  const decoded = tryDecodeBase64(s)
  if (decoded) {
    try { return { parsed: JSON.parse(decoded) as JsonValue, raw: decoded } } catch { /* continue */ }
  }
  return null
}

function ValueCell({ raw }: { raw: string | null }) {
  const [open, setOpen] = useState(false)
  if (!raw) return <span style={{ color: 'var(--ink-tertiary)' }}>—</span>
  const result = tryParseValue(raw)
  const isJson = result !== null
  const preview = raw.length > 80 ? raw.slice(0, 80) + '…' : raw

  if (!isJson) {
    return (
      <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-mono-size)', color: 'var(--ink-primary)' }}>
        {preview}
      </span>
    )
  }

  const decodedPreview = result.raw.length > 80 ? result.raw.slice(0, 80) + '…' : result.raw

  return (
    <div>
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        title={open ? 'Collapse' : 'Expand JSON'}
        style={{
          cursor: 'pointer',
          fontFamily: 'var(--font-mono)',
          fontSize: 'var(--type-mono-size)',
          color: 'var(--accent)',
          userSelect: 'none',
          background: 'none',
          border: 0,
          padding: 0,
          display: 'inline-flex',
          alignItems: 'baseline',
          gap: 6,
          textAlign: 'left',
        }}
      >
        <span
          aria-hidden
          style={{
            fontSize: 9,
            display: 'inline-block',
            transform: open ? 'rotate(90deg)' : 'rotate(0deg)',
            transition: 'transform var(--duration-quick) var(--ease-out-soft)',
          }}
        >▶</span>
        {open ? (result.raw !== raw ? 'JSON — base64 decoded' : 'JSON') : decodedPreview}
      </button>
      {open && (
        <div style={rjvWrap}>
          <ReactJson
            src={result.parsed as object}
            name={null}
            collapsed={2}
            indentWidth={2}
            displayDataTypes={false}
            displayObjectSize={false}
            enableClipboard
            style={rjvStyle}
            theme="flat"
          />
        </div>
      )}
    </div>
  )
}

interface TopicSearch {
  /** Sort direction for cassette replay. UI default: 'desc' (latest first). */
  order?: Order
}

export const Route = createFileRoute('/topics/$topic')({
  component: TopicDetailPage,
  validateSearch: (raw: Record<string, unknown>): TopicSearch => {
    const o = raw.order
    return { order: o === 'asc' || o === 'desc' ? o : undefined }
  },
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
        <h2 className="type-h2" style={{ margin: '0 0 8px' }}>Add matcher</h2>
        <p style={{ margin: '0 0 24px', color: 'var(--ink-secondary)', fontSize: 'var(--type-caption-size)' }}>
          Tag incoming messages so they can be grouped by an extracted identifier.
        </p>
        <form onSubmit={e => { e.preventDefault(); void form.handleSubmit() }} style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
          <form.Field name="messageType">
            {(f) => (
              <Input
                label="Message type *"
                value={f.state.value}
                onChange={e => f.handleChange(e.target.value)}
                required
              />
            )}
          </form.Field>
          <form.Field name="idSource">
            {(f) => (
              <Select
                label="ID source"
                value={f.state.value}
                onChange={e => f.handleChange(e.target.value)}
              >
                <option value="value">value</option>
                <option value="key">key</option>
                <option value="header">header</option>
              </Select>
            )}
          </form.Field>
          <form.Field name="idExpression">
            {(f) => (
              <Input
                label="ID expression *"
                mono
                placeholder="$.order_id"
                value={f.state.value}
                onChange={e => f.handleChange(e.target.value)}
                required
              />
            )}
          </form.Field>
          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 8 }}>
            <Button type="button" variant="ghost" onClick={onClose}>Cancel</Button>
            <Button type="submit" variant="primary" disabled={mutation.isPending}>
              {mutation.isPending ? 'Adding…' : 'Add matcher'}
            </Button>
          </div>
        </form>
      </div>
    </div>
  )
}

const trunc = (s: string | null, n: number) =>
  s == null ? '—' : s.length > n ? s.slice(0, n) + '…' : s

function isStreamActive(s: StreamStatus): boolean {
  return s === 'streaming' || s === 'draining' || s === 'tailing'
}

function formatBytes(b: number) {
  if (b < 1024) return `${b} B`
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`
  return `${(b / 1024 / 1024).toFixed(1)} MB`
}

function TopicDetailPage() {
  const { topic } = Route.useParams()
  const search = Route.useSearch()
  const navigate = useNavigate({ from: Route.fullPath })
  const qc = useQueryClient()
  const { addToast } = useToast()

  // UI default is latest-first; backend default is oldest-first. The URL is
  // only updated when the user flips the toggle.
  const order: Order = search.order ?? 'desc'
  function setOrder(next: Order) {
    if (next === order) return
    void navigate({ search: (prev: TopicSearch) => ({ ...prev, order: next }) })
  }

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
  const [activeTab, setActiveTab] = useState<'records' | 'sequence'>('records')
  const [_replayPipelineFragments, setReplayPipelineFragments] = useState<FragmentDefinition[]>([])

  // Streaming state
  const [streamMode, setStreamMode] = useState<StreamMode>('json')
  const [followLive, setFollowLive] = useState(false)
  const [streamStatus, setStreamStatus] = useState<StreamStatus>('idle')
  const [streamedRecords, setStreamedRecords] = useState<CassetteRecord[]>([])
  const [streamError, setStreamError] = useState<string | null>(null)
  const abortRef = useRef<AbortController | null>(null)
  const streamBufferRef = useRef<CassetteRecord[]>([])
  const flushIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const followActiveRef = useRef(false)
  const terminalRef = useRef(false)

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
    queryKey: ['cassettes', 'topics', topic, 'records', { from, to, partition, offsetFrom, offsetTo, cursor, order }],
    queryFn: () =>
      cassettesApi.getTopicRecords(topic, {
        from: from || undefined,
        to: to || undefined,
        partition: partition ? Number(partition) : undefined,
        offset_from: offsetFrom ? Number(offsetFrom) : undefined,
        offset_to: offsetTo ? Number(offsetTo) : undefined,
        cursor,
        limit: 50,
        order,
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
    followActiveRef.current = false
    terminalRef.current = true
    setStreamedRecords([...streamBufferRef.current])
    setStreamStatus(s => isStreamActive(s) ? 'stopped' : s)
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
    const params: TopicStreamParams & { follow?: boolean } = {
      from: from || undefined,
      to: isFollowing ? undefined : (to || undefined),
      partition: partition ? Number(partition) : undefined,
      offset_from: offsetFrom ? Number(offsetFrom) : undefined,
      offset_to: isFollowing ? undefined : (offsetTo ? Number(offsetTo) : undefined),
      follow: isFollowing || undefined,
      order,
    }
    // When order=desc + follow is on, post-preamble live records must land at
    // the top of the list (latest-first) rather than the tail. During the
    // historical drain records still arrive newest→oldest from the backend
    // and are appended in arrival order, which is already the correct visual
    // order. So: push-append during drain; prepend once the `follow` preamble
    // flips `followActiveRef`.
    const shouldPrependLive = isFollowing && order === 'desc'
    abortRef.current = streamTopicRecords(topic, streamMode, params, {
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

  useEffect(() => () => {
    abortRef.current?.abort()
    if (flushIntervalRef.current) clearInterval(flushIntervalRef.current)
  }, [])

  useEffect(() => {
    abortRef.current?.abort()
    abortRef.current = null
    if (flushIntervalRef.current) { clearInterval(flushIntervalRef.current); flushIntervalRef.current = null }
    followActiveRef.current = false
    setStreamStatus(s => (s === 'streaming' || s === 'draining' || s === 'tailing') ? 'idle' : s)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [from, to, partition, offsetFrom, offsetTo, order])

  // When the sort flips, also reset paged cursor state so the paged tab
  // doesn't merge records from the previous direction with the new one.
  useEffect(() => {
    setCursor(undefined)
    setCursors([])
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [order])

  const matcherColumns = [
    matcherColHelper.accessor('messageType', {
      header: 'Message type',
      cell: i => <span style={{ color: 'var(--ink-primary)', fontWeight: 500 }}>{i.getValue()}</span>,
    }),
    matcherColHelper.accessor('idSource', {
      header: 'Source',
      cell: i => <Tabular size="xs">{i.getValue()}</Tabular>,
    }),
    matcherColHelper.accessor('idExpression', {
      header: 'Expression',
      cell: i => <Tabular size="xs">{i.getValue()}</Tabular>,
    }),
    matcherColHelper.display({
      id: 'actions',
      header: '',
      cell: ({ row }) => (
        <Button size="sm" variant="danger" onClick={() => setConfirmDeleteMatcher(row.original.messageType)}>
          Remove
        </Button>
      ),
    }),
  ]

  const matcherTable = useReactTable({
    data: matchersQuery.data ?? [],
    columns: matcherColumns,
    getCoreRowModel: getCoreRowModel(),
  })

  const columns = [
    colHelper.accessor('timestamp', {
      header: 'Timestamp',
      cell: i => <Tabular>{i.getValue().slice(0, 19).replace('T', ' ')}</Tabular>,
    }),
    colHelper.accessor('partition', {
      header: 'Part.',
      cell: i => <Tabular muted>{i.getValue()}</Tabular>,
    }),
    colHelper.accessor('offset', {
      header: 'Offset',
      cell: i => <Tabular>{i.getValue().toLocaleString()}</Tabular>,
    }),
    colHelper.accessor('key', {
      header: 'Key',
      cell: i => {
        const v = i.getValue()
        if (v == null) return <span style={{ color: 'var(--ink-tertiary)' }}>—</span>
        return <Tabular muted>{trunc(v, 40)}</Tabular>
      },
    }),
    colHelper.accessor('value', {
      header: 'Value',
      cell: i => <ValueCell raw={i.getValue()} />,
    }),
    colHelper.accessor('recordedAt', {
      header: 'Recorded',
      cell: i => <Tabular muted>{i.getValue().slice(0, 19).replace('T', ' ')}</Tabular>,
    }),
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

  const isPaused = topicQuery.data?.paused ?? false

  return (
    <Layout>
      {topicQuery.isLoading && <LoadingSpinner />}
      {topicQuery.error && <ErrorMessage message={(topicQuery.error as Error).message} />}
      {topicQuery.data && (
        <div
          style={{
            maxWidth: 1180,
            marginInline: 'auto',
            display: 'flex',
            flexDirection: 'column',
            gap: 'var(--space-7)',
            paddingBlock: 'var(--space-5)',
          }}
        >
          {/* ── Editorial header ────────────────────────────────────── */}
          <header>
            <div
              style={{
                fontFamily: 'var(--font-body)',
                fontSize: 'var(--type-micro-size)',
                letterSpacing: 'var(--type-micro-tracking)',
                textTransform: 'uppercase',
                fontWeight: 'var(--type-micro-weight)',
                color: 'var(--ink-tertiary)',
                marginBottom: 12,
              }}
            >
              Topic cassette
            </div>
            <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: 24, flexWrap: 'wrap' }}>
              <h1
                className="type-display"
                style={{ margin: 0, color: 'var(--ink-primary)' }}
              >
                {topic}
              </h1>
              <Badge
                tone={isPaused ? 'warn' : 'live'}
                dot={{
                  shape: isPaused ? 'slash' : 'filled',
                  color: isPaused ? 'var(--signal-warn)' : 'var(--signal-live)',
                }}
              >
                {isPaused ? 'Paused' : 'Recording'}
              </Badge>
            </div>
            {/* Mono metadata strip */}
            <div
              style={{
                display: 'flex',
                gap: 32,
                flexWrap: 'wrap',
                alignItems: 'baseline',
                marginTop: 20,
              }}
            >
              {statsQuery.data && [
                ['records', statsQuery.data.rowCount.toLocaleString()],
                ['size', formatBytes(statsQuery.data.estimatedSizeBytes)],
                ['table', statsQuery.data.tableName],
                ['mode', topicQuery.data.mode],
              ].map(([k, v]) => (
                <div key={k} style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  <span
                    style={{
                      fontFamily: 'var(--font-body)',
                      fontSize: 'var(--type-micro-size)',
                      letterSpacing: 'var(--type-micro-tracking)',
                      textTransform: 'uppercase',
                      fontWeight: 'var(--type-micro-weight)',
                      color: 'var(--ink-tertiary)',
                    }}
                  >
                    {k}
                  </span>
                  <Tabular size="md">{v}</Tabular>
                </div>
              ))}
            </div>
            <Hairline style={{ marginTop: 24 }} />
          </header>

          {/* ── Actions (toolbar) ───────────────────────────────────── */}
          <section>
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
              <Link
                to="/topics/$topic/timeline"
                params={{ topic }}
                style={{ textDecoration: 'none' }}
              >
                <Button variant="secondary">Timeline</Button>
              </Link>
              <Button
                variant="secondary"
                onClick={() => compactMutation.mutate()}
                disabled={compactMutation.isPending}
              >
                {compactMutation.isPending ? 'Compacting…' : 'Compact'}
              </Button>
              <Button variant="danger" onClick={() => setShowTruncateDialog(true)}>
                Truncate
              </Button>
            </div>
          </section>

          {/* ── Config ──────────────────────────────────────────────── */}
          <section style={sectionStyle}>
            <SectionTitle kicker="Configuration" title="Recording settings" />
            <form
              onSubmit={(e) => { e.preventDefault(); void form.handleSubmit() }}
              style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 24, alignItems: 'end' }}
            >
              <form.Field name="mode">
                {(field) => (
                  <Select
                    label="Mode"
                    value={field.state.value}
                    onChange={e => field.handleChange(e.target.value)}
                  >
                    <option value="general">General</option>
                    <option value="entity_only">Entity only</option>
                    <option value="both">Both</option>
                  </Select>
                )}
              </form.Field>
              <form.Field name="brokerId">
                {(field) => (
                  <Select
                    label="Broker"
                    value={field.state.value ?? ''}
                    onChange={e => field.handleChange(e.target.value)}
                  >
                    <option value="">(default)</option>
                    {brokers.map(b => (
                      <option key={b.brokerId} value={b.brokerId}>{b.brokerId} — {b.bootstrapServers}</option>
                    ))}
                  </Select>
                )}
              </form.Field>
              <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
                <Button type="submit" variant="primary" disabled={updateMutation.isPending}>
                  Save
                </Button>
              </div>
            </form>

            <Hairline style={{ margin: '28px 0 20px' }} />

            <form
              onSubmit={e => { e.preventDefault(); void retentionForm.handleSubmit() }}
              style={{ display: 'flex', gap: 24, alignItems: 'end', flexWrap: 'wrap' }}
            >
              <retentionForm.Field name="retentionDays">
                {(field) => (
                  <Input
                    label="Retention (days — 0 is unlimited)"
                    type="number"
                    min={0}
                    mono
                    value={field.state.value}
                    onChange={e => field.handleChange(Number(e.target.value))}
                    style={{ width: 160 }}
                  />
                )}
              </retentionForm.Field>
              <p style={{
                margin: 0,
                color: 'var(--ink-tertiary)',
                fontSize: 'var(--type-caption-size)',
                flex: 1,
              }}>
                {topicQuery.data.retentionDays
                  ? <>Data retained for <Tabular size="xs">{topicQuery.data.retentionDays}</Tabular> days.</>
                  : <>No retention limit — data is kept indefinitely.</>}
              </p>
              <Button type="submit" variant="secondary" disabled={retentionMutation.isPending}>
                {retentionMutation.isPending ? 'Saving…' : 'Save retention'}
              </Button>
            </form>
          </section>

          {/* ── Matchers ────────────────────────────────────────────── */}
          <section style={sectionStyle}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 4 }}>
              <SectionTitle kicker="Matchers" title="Message tagging rules" compact />
              <Button variant="primary" size="sm" onClick={() => setShowAddMatcher(true)}>
                Add matcher
              </Button>
            </div>
            <p style={{ margin: '0 0 20px', fontSize: 'var(--type-caption-size)', color: 'var(--ink-secondary)', maxWidth: 620 }}>
              Tag messages with a <code>messageType</code> by extracting an identifier from the message header, key, or value. The first matching rule wins.
            </p>
            {matchersQuery.isLoading && <LoadingSpinner />}
            {matchersQuery.error && <ErrorMessage message={(matchersQuery.error as Error).message} />}
            {!matchersQuery.isLoading && (
              matcherTable.getRowModel().rows.length === 0 ? (
                <EmptyBlock
                  headline="No matchers yet"
                  body="Add a matcher to tag incoming messages. Matchers let you group records by a business identifier without changing their structure."
                />
              ) : (
                <RuledTable table={matcherTable} />
              )
            )}
          </section>

          {/* ── Replay to topic (untouched, per brief) ──────────────── */}
          <ReplayToTopicPanel
            mode="topic"
            topic={topic}
            from={from || undefined}
            to={to || undefined}
            totalCount={statsQuery.data?.rowCount}
          />

          {/* ── Tabs ────────────────────────────────────────────────── */}
          <div>
            <div style={{ display: 'inline-flex', gap: 2, position: 'relative' }}>
              {(['records', 'sequence'] as const).map(tab => {
                const active = activeTab === tab
                return (
                  <button
                    key={tab}
                    type="button"
                    onClick={() => setActiveTab(tab)}
                    style={{
                      padding: '10px 18px 12px',
                      background: 'none',
                      border: 0,
                      cursor: 'pointer',
                      fontFamily: 'var(--font-body)',
                      fontSize: '0.9375rem',
                      fontWeight: active ? 600 : 440,
                      color: active ? 'var(--ink-primary)' : 'var(--ink-tertiary)',
                      position: 'relative',
                      transition: 'color var(--duration-quick) var(--ease-out-soft)',
                    }}
                  >
                    {tab === 'records' ? 'Records' : 'Sequence'}
                    <span
                      aria-hidden
                      style={{
                        position: 'absolute',
                        left: 18,
                        right: 18,
                        bottom: 0,
                        height: 2,
                        background: 'var(--accent)',
                        transform: active ? 'scaleX(1)' : 'scaleX(0)',
                        transformOrigin: 'left',
                        transition: 'transform var(--duration-default) var(--ease-out-soft)',
                      }}
                    />
                  </button>
                )
              })}
            </div>
            <Hairline />
          </div>

          {activeTab === 'sequence' && (
            <section>
              <SequenceQueryPanel
                mode="topic"
                topic={topic}
                onSaveFragment={(frag) => setReplayPipelineFragments(fs => [...fs, frag])}
              />
            </section>
          )}

          {activeTab === 'records' && (
            <section style={sectionStyle}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 20, flexWrap: 'wrap', marginBottom: 20 }}>
                <SectionTitle kicker="Replay" title="Records" compact />
                <SegmentedControl
                  value={streamMode}
                  onChange={(v) => { clearStream(); setStreamMode(v) }}
                  options={[
                    { value: 'json', label: 'Paged' },
                    { value: 'sse', label: 'SSE' },
                    { value: 'ndjson', label: 'NDJSON' },
                  ]}
                />
              </div>

              {/* Sort direction — above the filter strip so it's first in reading order */}
              <div style={{
                display: 'flex',
                alignItems: 'center',
                gap: 16,
                marginBottom: 20,
              }}>
                <span style={{
                  fontFamily: 'var(--font-body)',
                  fontSize: 'var(--type-micro-size)',
                  letterSpacing: 'var(--type-micro-tracking)',
                  textTransform: 'uppercase',
                  fontWeight: 'var(--type-micro-weight)',
                  color: 'var(--ink-tertiary)',
                }}>
                  Order
                </span>
                <SegmentedControl<Order>
                  value={order}
                  onChange={setOrder}
                  options={[
                    { value: 'desc', label: 'Latest first' },
                    { value: 'asc',  label: 'Oldest first' },
                  ]}
                />
              </div>

              {/* Filter strip — ruled inputs, generous space */}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))', gap: 24, marginBottom: 24 }}>
                {([
                  ['From', fromRaw, setFromRaw, 'datetime-local', false],
                  ['To', toRaw, setToRaw, 'datetime-local', true],
                  ['Partition', partitionRaw, setPartitionRaw, 'number', false],
                  ['Offset from', offsetFromRaw, setOffsetFromRaw, 'number', false],
                  ['Offset to', offsetToRaw, setOffsetToRaw, 'number', true],
                ] as Array<[string, string, React.Dispatch<React.SetStateAction<string>>, string, boolean]>).map(([label, val, setter, type, disabledByFollow]) => {
                  const disabled = disabledByFollow && followLive && streamMode !== 'json'
                  return (
                    <Input
                      key={label}
                      label={label}
                      mono={type === 'number'}
                      type={type}
                      disabled={disabled}
                      title={disabled ? 'Disabled while following live — bounded ranges are incompatible with follow mode.' : undefined}
                      value={val}
                      onChange={e => setter(e.target.value)}
                    />
                  )
                })}
              </div>

              {/* Status area — first-class real estate */}
              {streamMode !== 'json' && (
                <div style={{
                  display: 'flex',
                  gap: 20,
                  alignItems: 'center',
                  flexWrap: 'wrap',
                  padding: '16px 0 20px',
                  borderTop: '1px solid var(--rule)',
                  borderBottom: '1px solid var(--rule)',
                  marginBottom: 24,
                }}>
                  {!isStreamActive(streamStatus) ? (
                    <Button variant="primary" onClick={startStream}>Start streaming</Button>
                  ) : (
                    <Button variant="danger" onClick={stopStream}>Stop</Button>
                  )}
                  {streamedRecords.length > 0 && !isStreamActive(streamStatus) && (
                    <Button variant="ghost" size="sm" onClick={clearStream}>Clear</Button>
                  )}
                  <label style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: 8,
                    fontSize: 'var(--type-caption-size)',
                    color: 'var(--ink-secondary)',
                    cursor: isStreamActive(streamStatus) ? 'not-allowed' : 'pointer',
                    userSelect: 'none',
                  }}>
                    <input
                      type="checkbox"
                      checked={followLive}
                      onChange={e => setFollowLive(e.target.checked)}
                      disabled={isStreamActive(streamStatus)}
                      style={{ accentColor: 'var(--accent)' }}
                    />
                    Follow live
                  </label>
                  <div style={{ flex: 1, minWidth: 200 }}>
                    <StreamStatusBadge
                      status={streamStatus}
                      count={streamedRecords.length}
                      errorMessage={streamError}
                    />
                  </div>
                </div>
              )}

              {streamMode === 'json' && recordsQuery.isLoading && <LoadingSpinner />}
              {streamMode === 'json' && recordsQuery.error && <ErrorMessage message={(recordsQuery.error as Error).message} />}
              {(streamMode !== 'json' || !recordsQuery.isLoading) && (
                <>
                  {tableData.length === 0 ? (
                    <EmptyBlock
                      headline={streamMode === 'json' ? 'The tape is quiet here' : 'Not yet streaming'}
                      body={
                        streamMode === 'json'
                          ? 'No records match this filter. Loosen the time window or the offset range, or wait for more data to arrive.'
                          : 'Press Start to open the stream. Enable Follow live to tail new records as they arrive.'
                      }
                    />
                  ) : (
                    <>
                      <RuledTable table={table} density="dense" />
                      {streamMode === 'json' && (
                        <div style={{
                          display: 'flex',
                          gap: 12,
                          marginTop: 20,
                          alignItems: 'center',
                        }}>
                          <Button size="sm" variant="ghost" disabled={cursors.length === 0} onClick={prevPage}>← Previous</Button>
                          <Button size="sm" variant="ghost" disabled={!recordsQuery.data?.hasMore} onClick={nextPage}>Next →</Button>
                          <span style={{ marginLeft: 'auto', color: 'var(--ink-tertiary)', fontSize: 'var(--type-caption-size)' }}>
                            <Tabular size="xs" muted>{(recordsQuery.data?.data.length ?? 0).toLocaleString()}</Tabular> records on this page
                          </span>
                        </div>
                      )}
                    </>
                  )}
                </>
              )}
            </section>
          )}
        </div>
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

// ── Section helpers ─────────────────────────────────────────────────────────

const sectionStyle: CSSProperties = {
  padding: '28px 0 4px',
}

function SectionTitle({ kicker, title, compact = false }: { kicker: string; title: string; compact?: boolean }) {
  return (
    <div style={{ marginBottom: compact ? 4 : 18 }}>
      <div
        style={{
          fontFamily: 'var(--font-body)',
          fontSize: 'var(--type-micro-size)',
          letterSpacing: 'var(--type-micro-tracking)',
          textTransform: 'uppercase',
          fontWeight: 'var(--type-micro-weight)',
          color: 'var(--ink-tertiary)',
          marginBottom: 4,
        }}
      >
        {kicker}
      </div>
      <h2 className="type-h2" style={{ margin: 0, color: 'var(--ink-primary)' }}>{title}</h2>
    </div>
  )
}

function EmptyBlock({ headline, body }: { headline: string; body: string }) {
  return (
    <div
      style={{
        padding: '40px 0 16px',
        maxWidth: 520,
      }}
    >
      <h3
        className="type-h2"
        style={{
          margin: '0 0 12px',
          color: 'var(--ink-primary)',
          fontStyle: 'italic',
          fontWeight: 380,
        }}
      >
        {headline}
      </h3>
      <p style={{ margin: 0, color: 'var(--ink-secondary)', lineHeight: 1.6 }}>
        {body}
      </p>
    </div>
  )
}

function SegmentedControl<T extends string>({
  value,
  onChange,
  options,
}: {
  value: T
  onChange: (v: T) => void
  options: Array<{ value: T; label: string }>
}) {
  return (
    <div
      role="tablist"
      style={{
        display: 'inline-flex',
        gap: 0,
        position: 'relative',
      }}
    >
      {options.map(opt => {
        const active = opt.value === value
        return (
          <button
            key={opt.value}
            role="tab"
            aria-selected={active}
            type="button"
            onClick={() => onChange(opt.value)}
            style={{
              padding: '8px 16px 10px',
              background: 'none',
              border: 0,
              borderBottom: active ? '1px solid transparent' : '1px solid var(--rule)',
              cursor: 'pointer',
              fontFamily: 'var(--font-body)',
              fontSize: '0.8125rem',
              fontWeight: active ? 600 : 480,
              letterSpacing: '0.01em',
              color: active ? 'var(--ink-primary)' : 'var(--ink-tertiary)',
              position: 'relative',
              transition: 'color var(--duration-quick) var(--ease-out-soft)',
            }}
          >
            {opt.label}
            <span
              aria-hidden
              style={{
                position: 'absolute',
                left: 8,
                right: 8,
                bottom: -1,
                height: 2,
                background: 'var(--accent)',
                transform: active ? 'scaleX(1)' : 'scaleX(0)',
                transformOrigin: 'center',
                transition: 'transform var(--duration-default) var(--ease-out-soft)',
              }}
            />
          </button>
        )
      })}
    </div>
  )
}

/* eslint-disable @typescript-eslint/no-explicit-any */
function RuledTable({ table, density = 'regular' }: { table: any; density?: 'regular' | 'dense' }) {
  const pad = density === 'dense' ? '10px 12px' : '14px 14px'
  return (
    <div style={{ overflowX: 'auto' }}>
      <table
        style={{
          width: '100%',
          borderCollapse: 'collapse',
          fontSize: 'var(--type-body-size)',
          fontFamily: 'var(--font-body)',
          color: 'var(--ink-primary)',
        }}
      >
        <thead>
          {table.getHeaderGroups().map((hg: any) => (
            <tr key={hg.id} style={{ borderBottom: '1px solid var(--rule-strong)' }}>
              {hg.headers.map((h: any) => (
                <th
                  key={h.id}
                  style={{
                    textAlign: 'left',
                    padding: pad,
                    fontFamily: 'var(--font-body)',
                    fontSize: 'var(--type-micro-size)',
                    letterSpacing: 'var(--type-micro-tracking)',
                    textTransform: 'uppercase',
                    fontWeight: 'var(--type-micro-weight)',
                    color: 'var(--ink-tertiary)',
                  }}
                >
                  {flexRender(h.column.columnDef.header, h.getContext())}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map((row: any) => (
            <tr
              key={row.id}
              style={{
                borderBottom: '1px solid var(--rule)',
              }}
            >
              {row.getVisibleCells().map((cell: any) => (
                <td
                  key={cell.id}
                  style={{
                    padding: pad,
                    verticalAlign: 'top',
                  }}
                >
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
/* eslint-enable @typescript-eslint/no-explicit-any */

// ── Modal styles ────────────────────────────────────────────────────────────

const overlayStyle: CSSProperties = {
  position: 'fixed',
  inset: 0,
  background: 'color-mix(in oklab, var(--surface-sunken) 40%, rgba(10, 8, 6, 0.5))',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  zIndex: 1000,
  backdropFilter: 'blur(2px)',
  animation: 'jx-overlay-in var(--duration-quick) var(--ease-out-soft)',
}

const modalStyle: CSSProperties = {
  background: 'var(--surface-raised)',
  border: '1px solid var(--rule-strong)',
  borderRadius: 'var(--radius-md)',
  padding: '32px 32px 28px',
  minWidth: 420,
  maxWidth: 520,
  boxShadow: '0 30px 80px rgba(10, 8, 6, 0.22)',
}

if (typeof document !== 'undefined' && !document.getElementById('jx-overlay-anim')) {
  const el = document.createElement('style')
  el.id = 'jx-overlay-anim'
  el.textContent = `@keyframes jx-overlay-in { from { opacity: 0 } to { opacity: 1 } }`
  document.head.appendChild(el)
}

// ── Unused imports suppression ──────────────────────────────────────────────
// Keep StatusDot exported from primitives barrel in case future inline usage
// in this file grows. Reference it here so the import doesn't tree-shake out
// of the module graph during TS strict checks.
void StatusDot

// ── @microlink/react-json-view wrapper ───────────────────────────────────────
const rjvWrap: React.CSSProperties = {
  marginTop: 10,
  borderRadius: 'var(--radius-sm)',
  overflow: 'hidden',
  border: '1px solid var(--rule-strong)',
}

const rjvStyle: React.CSSProperties = {
  fontFamily: 'var(--font-mono)',
  fontSize: 'var(--type-mono-size)',
  padding: '8px 12px',
  background: 'var(--surface-sunken)',
}

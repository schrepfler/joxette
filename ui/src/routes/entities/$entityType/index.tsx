import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { useForm } from '@tanstack/react-form'
import { useState } from 'react'
import {
  entitiesApi,
  cassettesApi,
  type EntitySourceConfig,
  type MatcherConfig,
  type EntityInfo,
  type AddSourceRequest,
  type AddMatcherRequest,
} from '../../../api/client'
import { Layout } from '../../../components/Layout'
import { LoadingSpinner } from '../../../components/LoadingSpinner'
import { ErrorMessage } from '../../../components/ErrorMessage'
import { ConfirmDialog } from '../../../components/ConfirmDialog'
import { TruncateDialog } from '../../../components/TruncateDialog'
import { useToast } from '../../../components/Toast'
import { useDebounce } from '../../../hooks/useDebounce'

export const Route = createFileRoute('/entities/$entityType/')({
  component: EntityTypeDetailPage,
})

const entityColHelper = createColumnHelper<EntityInfo>()

// ---- AddSourceModal ----

function AddSourceModal({ entityType, onClose }: { entityType: string; onClose: () => void }) {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const [topic, setTopic] = useState('')
  const [mode, setMode] = useState('entity_only')
  const [matchers, setMatchers] = useState<MatcherConfig[]>([{ messageType: '', idSource: 'value', idExpression: '' }])

  const mutation = useMutation({
    mutationFn: (d: AddSourceRequest) => entitiesApi.addSource(entityType, d),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['entities', entityType, 'sources'] })
      addToast('Source added', 'success')
      onClose()
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  function updateMatcher(i: number, patch: Partial<MatcherConfig>) {
    setMatchers(prev => prev.map((m, idx) => idx === i ? { ...m, ...patch } : m))
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    const payload = matchers.filter(m => m.idExpression.trim())
    mutation.mutate({ topic, mode, matchers: payload })
  }

  return (
    <div style={overlayStyle} onClick={onClose}>
      <div style={{ ...modalStyle, minWidth: 560 }} onClick={e => e.stopPropagation()}>
        <h2 style={{ margin: '0 0 1.25rem', fontSize: 18 }}>Add Source</h2>
        <form onSubmit={handleSubmit}>
          <div style={fieldWrap}>
            <label style={labelStyle}>Topic *</label>
            <input style={inputStyleFull} value={topic} onChange={e => setTopic(e.target.value)} required />
          </div>
          <div style={fieldWrap}>
            <label style={labelStyle}>Mode</label>
            <select style={inputStyleFull} value={mode} onChange={e => setMode(e.target.value)}>
              <option value="entity_only">entity_only</option>
              <option value="both">both</option>
            </select>
          </div>
          <div style={{ marginBottom: '0.75rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.4rem' }}>
              <label style={labelStyle}>Matchers</label>
              <button
                type="button"
                onClick={() => setMatchers(prev => [...prev, { messageType: '', idSource: 'value', idExpression: '' }])}
                style={{ ...secondaryBtnStyle, fontSize: 12, padding: '0.2rem 0.6rem' }}
              >+ Add Matcher</button>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '140px 90px 1fr 28px', gap: '4px 6px', marginBottom: 4, paddingBottom: 4, borderBottom: '1px solid #e2e8f0' }}>
              <span style={{ fontSize: 11, color: '#718096', fontWeight: 600 }}>Message type</span>
              <span style={{ fontSize: 11, color: '#718096', fontWeight: 600 }}>Source</span>
              <span style={{ fontSize: 11, color: '#718096', fontWeight: 600 }}>Expression *</span>
              <span />
            </div>
            {matchers.map((m, i) => (
              <div key={i} style={{ display: 'grid', gridTemplateColumns: '140px 90px 1fr 28px', gap: '4px 6px', marginBottom: 6, alignItems: 'center' }}>
                <input
                  style={{ ...inputStyleFull, fontSize: 13 }}
                  placeholder="e.g. orderCreated"
                  value={m.messageType}
                  onChange={e => updateMatcher(i, { messageType: e.target.value })}
                />
                <select style={{ ...inputStyleFull, fontSize: 13 }} value={m.idSource} onChange={e => updateMatcher(i, { idSource: e.target.value })}>
                  <option value="value">value</option>
                  <option value="key">key</option>
                  <option value="header">header</option>
                </select>
                <input
                  style={{ ...inputStyleFull, fontSize: 13 }}
                  placeholder="e.g. $.order_id"
                  value={m.idExpression}
                  onChange={e => updateMatcher(i, { idExpression: e.target.value })}
                  required
                />
                <button
                  type="button"
                  onClick={() => setMatchers(prev => prev.filter((_, idx) => idx !== i))}
                  disabled={matchers.length === 1}
                  style={{ ...dangerBtnSmall, opacity: matchers.length === 1 ? 0.4 : 1, width: 24, padding: 0, textAlign: 'center' }}
                >✕</button>
              </div>
            ))}
          </div>
          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '0.5rem' }}>
            <button type="button" onClick={onClose} style={cancelBtnStyle}>Cancel</button>
            <button type="submit" disabled={mutation.isPending} style={primaryBtnStyle}>{mutation.isPending ? 'Adding…' : 'Add'}</button>
          </div>
        </form>
      </div>
    </div>
  )
}

// ---- SourceCard ----

function SourceCard({
  entityType,
  source,
  onDeleteSource,
}: {
  entityType: string
  source: EntitySourceConfig
  onDeleteSource: (topic: string) => void
}) {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const [showAddMatcher, setShowAddMatcher] = useState(false)
  const [confirmDeleteMatcher, setConfirmDeleteMatcher] = useState<string | null>(null)

  const addMatcherMutation = useMutation({
    mutationFn: (body: AddMatcherRequest) => entitiesApi.addMatcher(entityType, source.topic, body),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['entities', entityType, 'sources'] })
      addToast('Matcher added', 'success')
      setShowAddMatcher(false)
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const deleteMatcherMutation = useMutation({
    mutationFn: (messageType: string) => entitiesApi.deleteMatcher(entityType, source.topic, messageType),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['entities', entityType, 'sources'] })
      addToast('Matcher deleted', 'success')
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const matcherForm = useForm({
    defaultValues: { messageType: '', idSource: 'value', idExpression: '' },
    onSubmit: async ({ value }) => {
      addMatcherMutation.mutate({
        messageType: value.messageType.trim(),
        idSource: value.idSource,
        idExpression: value.idExpression.trim(),
      })
    },
  })

  return (
    <div style={sourceCardStyle}>
      {/* Source header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.6rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontWeight: 600, fontSize: 14 }}>{source.topic}</span>
          <span style={modeBadgeStyle(source.mode)}>{source.mode}</span>
        </div>
        <button style={dangerBtnSmall} onClick={() => onDeleteSource(source.topic)}>Delete Source</button>
      </div>

      {/* Matchers */}
      <div>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.35rem' }}>
          <span style={{ fontSize: 11, fontWeight: 700, color: '#718096', textTransform: 'uppercase', letterSpacing: 0.5 }}>Matchers</span>
          {!showAddMatcher && (
            <button style={secondaryBtnSmall} onClick={() => setShowAddMatcher(true)}>+ Add Matcher</button>
          )}
        </div>

        {source.matchers.length === 0 && !showAddMatcher && (
          <p style={{ fontSize: 12, color: '#a0aec0', margin: '0 0 0.4rem' }}>No matchers — entity IDs cannot be extracted from this source.</p>
        )}

        {source.matchers.length > 0 && (
          <table style={{ ...tableStyle, marginBottom: '0.5rem' }}>
            <thead>
              <tr>
                <th style={thStyle}>Message Type</th>
                <th style={thStyle}>ID Source</th>
                <th style={thStyle}>ID Expression</th>
                <th style={{ ...thStyle, width: 72 }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {source.matchers.map(m => (
                <tr key={m.messageType}>
                  <td style={tdStyle}><code style={monoStyle}>{m.messageType}</code></td>
                  <td style={tdStyle}>{m.idSource}</td>
                  <td style={tdStyle}><code style={monoStyle}>{m.idExpression}</code></td>
                  <td style={tdStyle}>
                    <button style={dangerBtnSmall} onClick={() => setConfirmDeleteMatcher(m.messageType)}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}

        {showAddMatcher && (
          <form
            onSubmit={e => { e.preventDefault(); void matcherForm.handleSubmit() }}
            style={addMatcherFormStyle}
          >
            <matcherForm.Field name="messageType">
              {(f) => (
                <div>
                  <label style={{ ...labelStyle, marginBottom: 3 }}>Message Type *</label>
                  <input
                    style={{ ...inputStyle, width: 150 }}
                    placeholder="e.g. orderCreated"
                    value={f.state.value}
                    onChange={e => f.handleChange(e.target.value)}
                    required
                  />
                </div>
              )}
            </matcherForm.Field>
            <matcherForm.Field name="idSource">
              {(f) => (
                <div>
                  <label style={{ ...labelStyle, marginBottom: 3 }}>ID Source</label>
                  <select style={{ ...inputStyle, width: 100 }} value={f.state.value} onChange={e => f.handleChange(e.target.value)}>
                    <option value="value">value</option>
                    <option value="key">key</option>
                    <option value="header">header</option>
                  </select>
                </div>
              )}
            </matcherForm.Field>
            <matcherForm.Field name="idExpression">
              {(f) => (
                <div>
                  <label style={{ ...labelStyle, marginBottom: 3 }}>ID Expression *</label>
                  <input
                    style={{ ...inputStyle, width: 180 }}
                    placeholder="e.g. $.order_id"
                    value={f.state.value}
                    onChange={e => f.handleChange(e.target.value)}
                    required
                  />
                </div>
              )}
            </matcherForm.Field>
            <div style={{ display: 'flex', gap: 6, alignSelf: 'flex-end' }}>
              <button type="submit" disabled={addMatcherMutation.isPending} style={{ ...primaryBtnStyle, padding: '0.4rem 0.8rem' }}>
                {addMatcherMutation.isPending ? 'Adding…' : 'Add'}
              </button>
              <button type="button" style={{ ...cancelBtnStyle, padding: '0.4rem 0.8rem' }} onClick={() => { setShowAddMatcher(false); matcherForm.reset() }}>
                Cancel
              </button>
            </div>
          </form>
        )}
      </div>

      {confirmDeleteMatcher && (
        <ConfirmDialog
          message={`Delete matcher "${confirmDeleteMatcher}" from ${source.topic}?`}
          onConfirm={() => { deleteMatcherMutation.mutate(confirmDeleteMatcher); setConfirmDeleteMatcher(null) }}
          onCancel={() => setConfirmDeleteMatcher(null)}
        />
      )}
    </div>
  )
}

// ---- Main page ----

function EntityTypeDetailPage() {
  const { entityType } = Route.useParams()
  const navigate = useNavigate()
  const qc = useQueryClient()
  const { addToast } = useToast()
  const [showAddSource, setShowAddSource] = useState(false)
  const [confirmDeleteSource, setConfirmDeleteSource] = useState<string | null>(null)
  const [showTruncateDialog, setShowTruncateDialog] = useState(false)
  const [searchRaw, setSearchRaw] = useState('')
  const search = useDebounce(searchRaw, 300)
  const [cursor, setCursor] = useState<string | undefined>()
  const [cursors, setCursors] = useState<string[]>([])

  const entityQuery = useQuery({ queryKey: ['entities', entityType], queryFn: () => entitiesApi.get(entityType) })
  const sourcesQuery = useQuery({ queryKey: ['entities', entityType, 'sources'], queryFn: () => entitiesApi.getSources(entityType) })

  const entitiesQuery = useQuery({
    queryKey: ['cassettes', 'entities', entityType, 'list', search, cursor],
    queryFn: () =>
      search
        ? cassettesApi.searchEntities(entityType, { q: search, limit: 50, cursor })
        : cassettesApi.listEntities(entityType, { limit: 50, cursor }),
  })

  const updateMutation = useMutation({
    mutationFn: (buckets: number) => entitiesApi.update(entityType, { buckets }),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['entities', entityType] }); addToast('Entity type updated', 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const retentionMutation = useMutation({
    mutationFn: (days: number) => entitiesApi.updateRetention(entityType, days),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['entities', entityType] }); addToast('Retention updated', 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const deleteSourceMutation = useMutation({
    mutationFn: (topic: string) => entitiesApi.deleteSource(entityType, topic),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['entities', entityType, 'sources'] }); addToast('Source deleted', 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const truncateMutation = useMutation({
    mutationFn: (before: string) => cassettesApi.truncateEntityType(entityType, before),
    onSuccess: (d) => { void qc.invalidateQueries({ queryKey: ['cassettes', 'entities', entityType] }); addToast(`Deleted ${d.deleted} records`, 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const form = useForm({
    defaultValues: { buckets: String(entityQuery.data?.buckets ?? '') },
    onSubmit: async ({ value }) => updateMutation.mutate(Number(value.buckets)),
  })

  const retentionForm = useForm({
    defaultValues: { retentionDays: entityQuery.data?.retentionDays ?? 0 },
    onSubmit: async ({ value }) => retentionMutation.mutate(value.retentionDays),
  })

  const entityColumns = [
    entityColHelper.accessor('entityId', { header: 'Entity ID' }),
    entityColHelper.accessor('firstSeen', { header: 'First Seen', cell: i => i.getValue().slice(0, 19).replace('T', ' ') }),
    entityColHelper.accessor('lastSeen', { header: 'Last Seen', cell: i => i.getValue().slice(0, 19).replace('T', ' ') }),
  ]

  const entityTable = useReactTable({ data: entitiesQuery.data?.data ?? [], columns: entityColumns, getCoreRowModel: getCoreRowModel() })

  function nextPage() {
    if (entitiesQuery.data?.nextCursor) {
      setCursors(prev => [...prev, cursor ?? ''])
      setCursor(entitiesQuery.data!.nextCursor)
    }
  }
  function prevPage() {
    const prev = cursors[cursors.length - 1]
    setCursors(c => c.slice(0, -1))
    setCursor(prev || undefined)
  }

  return (
    <Layout>
      {entityQuery.isLoading && <LoadingSpinner />}
      {entityQuery.error && <ErrorMessage message={(entityQuery.error as Error).message} />}
      {entityQuery.data && (
        <>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }}>
            <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>{entityType}</h1>
            <button style={{ ...primaryBtnStyle, background: '#e53e3e' }} onClick={() => setShowTruncateDialog(true)}>
              Truncate
            </button>
          </div>

          {/* Edit form */}
          <div style={cardStyle}>
            <h3 style={cardTitle}>Edit Entity Type</h3>
            <form onSubmit={e => { e.preventDefault(); void form.handleSubmit() }} style={{ display: 'flex', gap: 12, alignItems: 'flex-end' }}>
              <form.Field name="buckets">
                {(f) => (
                  <div>
                    <label style={labelStyle}>Buckets</label>
                    <input type="number" style={{ ...inputStyle, width: 120 }} value={f.state.value} onChange={e => f.handleChange(e.target.value)} />
                  </div>
                )}
              </form.Field>
              <button type="submit" disabled={updateMutation.isPending} style={primaryBtnStyle}>Save</button>
            </form>
            <div style={{ borderTop: '1px solid #e2e8f0', marginTop: '0.75rem', paddingTop: '0.75rem' }}>
              <p style={{ margin: '0 0 0.5rem', fontSize: 13, color: '#718096' }}>
                {entityQuery.data.retentionDays
                  ? `Data retained for ${entityQuery.data.retentionDays} days`
                  : 'No retention limit (unlimited)'}
              </p>
              <form onSubmit={e => { e.preventDefault(); void retentionForm.handleSubmit() }} style={{ display: 'flex', gap: 12, alignItems: 'flex-end' }}>
                <retentionForm.Field name="retentionDays">
                  {(f) => (
                    <div>
                      <label style={labelStyle}>Retention (days, 0 = unlimited)</label>
                      <input
                        type="number" min="0"
                        style={{ ...inputStyle, width: 120 }}
                        value={f.state.value}
                        onChange={e => f.handleChange(Number(e.target.value))}
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

          {/* Sources */}
          <div style={{ ...cardStyle, marginTop: '1.5rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.75rem' }}>
              <h3 style={{ margin: 0, fontSize: 15 }}>Sources</h3>
              <button style={primaryBtnStyle} onClick={() => setShowAddSource(true)}>+ Add Source</button>
            </div>
            {sourcesQuery.isLoading && <LoadingSpinner />}
            {!sourcesQuery.isLoading && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {(sourcesQuery.data ?? []).map(src => (
                  <SourceCard
                    key={src.topic}
                    entityType={entityType}
                    source={src}
                    onDeleteSource={setConfirmDeleteSource}
                  />
                ))}
                {(sourcesQuery.data ?? []).length === 0 && (
                  <p style={{ fontSize: 13, color: '#a0aec0', margin: 0 }}>No sources configured.</p>
                )}
              </div>
            )}
          </div>

          {/* Known entities */}
          <div style={{ ...cardStyle, marginTop: '1.5rem' }}>
            <h3 style={{ ...cardTitle, marginBottom: '0.75rem' }}>Known Entities</h3>
            <input
              style={{ ...inputStyle, width: 280, marginBottom: '0.75rem' }}
              placeholder="Search entity ID…"
              value={searchRaw}
              onChange={e => { setSearchRaw(e.target.value); setCursor(undefined); setCursors([]) }}
            />
            {entitiesQuery.isLoading && <LoadingSpinner />}
            {entitiesQuery.error && <ErrorMessage message={(entitiesQuery.error as Error).message} />}
            {!entitiesQuery.isLoading && (
              <>
                <table style={tableStyle}>
                  <thead>
                    {entityTable.getHeaderGroups().map(hg => (
                      <tr key={hg.id}>{hg.headers.map(h => <th key={h.id} style={thStyle}>{flexRender(h.column.columnDef.header, h.getContext())}</th>)}</tr>
                    ))}
                  </thead>
                  <tbody>
                    {entityTable.getRowModel().rows.map(row => (
                      <tr
                        key={row.id}
                        style={{ cursor: 'pointer' }}
                        onClick={() => void navigate({ to: '/entities/$entityType/$entityId', params: { entityType, entityId: row.original.entityId } })}
                        onMouseEnter={e => (e.currentTarget.style.background = '#ebf8ff')}
                        onMouseLeave={e => (e.currentTarget.style.background = '')}
                      >
                        {row.getVisibleCells().map(cell => <td key={cell.id} style={tdStyle}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}
                      </tr>
                    ))}
                  </tbody>
                </table>
                <div style={{ display: 'flex', gap: 8, marginTop: '0.75rem', alignItems: 'center' }}>
                  <button style={secondaryBtnStyle} disabled={cursors.length === 0} onClick={prevPage}>← Prev</button>
                  <button style={secondaryBtnStyle} disabled={!entitiesQuery.data?.hasMore} onClick={nextPage}>Next →</button>
                  <span style={{ fontSize: 13, color: '#718096' }}>{entitiesQuery.data?.data.length ?? 0} results</span>
                </div>
              </>
            )}
          </div>
        </>
      )}
      {showAddSource && <AddSourceModal entityType={entityType} onClose={() => setShowAddSource(false)} />}
      {confirmDeleteSource && (
        <ConfirmDialog
          message={`Delete source topic "${confirmDeleteSource}"?`}
          onConfirm={() => { deleteSourceMutation.mutate(confirmDeleteSource); setConfirmDeleteSource(null) }}
          onCancel={() => setConfirmDeleteSource(null)}
        />
      )}
      {showTruncateDialog && (
        <TruncateDialog
          label={`entity type "${entityType}"`}
          onConfirm={(before) => { truncateMutation.mutate(before); setShowTruncateDialog(false) }}
          onCancel={() => setShowTruncateDialog(false)}
        />
      )}
    </Layout>
  )
}

// ---- Styles ----

function modeBadgeStyle(mode: string): React.CSSProperties {
  return {
    fontSize: 11,
    padding: '0.15rem 0.45rem',
    borderRadius: 10,
    fontWeight: 600,
    background: mode === 'both' ? '#ebf8ff' : '#f0fff4',
    color: mode === 'both' ? '#2b6cb0' : '#276749',
    border: `1px solid ${mode === 'both' ? '#bee3f8' : '#c6f6d5'}`,
  }
}

const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem 2rem', minWidth: 420, boxShadow: '0 10px 30px rgba(0,0,0,0.2)' }
const cardStyle: React.CSSProperties = { background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem' }
const cardTitle: React.CSSProperties = { margin: '0 0 0.75rem', fontSize: 15 }
const sourceCardStyle: React.CSSProperties = { border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.75rem 1rem', background: '#fafafa' }
const addMatcherFormStyle: React.CSSProperties = { display: 'flex', gap: 8, alignItems: 'flex-end', flexWrap: 'wrap', padding: '0.5rem 0.75rem', background: '#fff', borderRadius: 4, border: '1px solid #e2e8f0', marginTop: '0.35rem' }
const fieldWrap: React.CSSProperties = { marginBottom: '0.75rem' }
const labelStyle: React.CSSProperties = { display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#4a5568' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14 }
const inputStyleFull: React.CSSProperties = { width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14, boxSizing: 'border-box' }
const primaryBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const cancelBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const secondaryBtnStyle: React.CSSProperties = { padding: '0.35rem 0.8rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 13 }
const secondaryBtnSmall: React.CSSProperties = { padding: '0.2rem 0.55rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const dangerBtnSmall: React.CSSProperties = { padding: '0.2rem 0.6rem', background: '#e53e3e', color: '#fff', border: 'none', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', background: '#fff', fontSize: 13 }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.5rem 0.6rem', background: '#edf2f7', fontWeight: 600, color: '#4a5568', borderBottom: '1px solid #e2e8f0' }
const tdStyle: React.CSSProperties = { padding: '0.45rem 0.6rem', borderBottom: '1px solid #e2e8f0' }
const monoStyle: React.CSSProperties = { fontFamily: 'monospace', fontSize: 12, background: '#edf2f7', padding: '0.1rem 0.3rem', borderRadius: 3 }

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
  type EntityInfo,
  type AddSourceRequest,
} from '../../../api/client'
import { Layout } from '../../../components/Layout'
import { LoadingSpinner } from '../../../components/LoadingSpinner'
import { ErrorMessage } from '../../../components/ErrorMessage'
import { ConfirmDialog } from '../../../components/ConfirmDialog'
import { useToast } from '../../../components/Toast'
import { useDebounce } from '../../../hooks/useDebounce'

export const Route = createFileRoute('/entities/$entityType/')({
  component: EntityTypeDetailPage,
})

const srcColHelper = createColumnHelper<EntitySourceConfig>()
const entityColHelper = createColumnHelper<EntityInfo>()

function AddSourceModal({ entityType, onClose }: { entityType: string; onClose: () => void }) {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const mutation = useMutation({
    mutationFn: (d: AddSourceRequest) => entitiesApi.addSource(entityType, d),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['entities', entityType, 'sources'] }); addToast('Source added', 'success'); onClose() },
    onError: (e: Error) => addToast(e.message, 'error'),
  })
  const form = useForm({
    defaultValues: { topic: '', idSource: 'value', idExpression: '' },
    onSubmit: async ({ value }) => mutation.mutate({ topic: value.topic, idSource: value.idSource, idExpression: value.idExpression }),
  })
  return (
    <div style={overlayStyle} onClick={onClose}>
      <div style={modalStyle} onClick={e => e.stopPropagation()}>
        <h2 style={{ margin: '0 0 1.25rem', fontSize: 18 }}>Add Source</h2>
        <form onSubmit={e => { e.preventDefault(); void form.handleSubmit() }}>
          <form.Field name="topic">
            {(f) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>Topic *</label>
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
                <input style={inputStyleFull} value={f.state.value} onChange={e => f.handleChange(e.target.value)} required />
              </div>
            )}
          </form.Field>
          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '0.5rem' }}>
            <button type="button" onClick={onClose} style={cancelBtnStyle}>Cancel</button>
            <button type="submit" disabled={mutation.isPending} style={primaryBtnStyle}>{mutation.isPending ? 'Adding…' : 'Add'}</button>
          </div>
        </form>
      </div>
    </div>
  )
}

function EntityTypeDetailPage() {
  const { entityType } = Route.useParams()
  const navigate = useNavigate()
  const qc = useQueryClient()
  const { addToast } = useToast()
  const [showAddSource, setShowAddSource] = useState(false)
  const [confirmDeleteSource, setConfirmDeleteSource] = useState<string | null>(null)
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

  const deleteSourceMutation = useMutation({
    mutationFn: (topic: string) => entitiesApi.deleteSource(entityType, topic),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['entities', entityType, 'sources'] }); addToast('Source deleted', 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const form = useForm({
    defaultValues: { buckets: String(entityQuery.data?.buckets ?? '') },
    onSubmit: async ({ value }) => updateMutation.mutate(Number(value.buckets)),
  })

  const srcColumns = [
    srcColHelper.accessor('topic', { header: 'Topic' }),
    srcColHelper.accessor('idSource', { header: 'ID Source' }),
    srcColHelper.accessor('idExpression', { header: 'ID Expression' }),
    srcColHelper.display({
      id: 'actions', header: 'Actions',
      cell: ({ row }) => (
        <button style={dangerBtnSmall} onClick={() => setConfirmDeleteSource(row.original.topic)}>Delete</button>
      ),
    }),
  ]

  const entityColumns = [
    entityColHelper.accessor('entityId', { header: 'Entity ID' }),
    entityColHelper.accessor('firstSeen', { header: 'First Seen', cell: i => i.getValue().slice(0, 19).replace('T', ' ') }),
    entityColHelper.accessor('lastSeen', { header: 'Last Seen', cell: i => i.getValue().slice(0, 19).replace('T', ' ') }),
  ]

  const srcTable = useReactTable({ data: sourcesQuery.data ?? [], columns: srcColumns, getCoreRowModel: getCoreRowModel() })
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
          <h1 style={{ margin: '0 0 1.5rem', fontSize: 22, fontWeight: 700 }}>{entityType}</h1>

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
          </div>

          {/* Sources */}
          <div style={{ ...cardStyle, marginTop: '1.5rem' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.75rem' }}>
              <h3 style={{ margin: 0, fontSize: 15 }}>Sources</h3>
              <button style={primaryBtnStyle} onClick={() => setShowAddSource(true)}>+ Add Source</button>
            </div>
            {sourcesQuery.isLoading && <LoadingSpinner />}
            {!sourcesQuery.isLoading && (
              <table style={tableStyle}>
                <thead>
                  {srcTable.getHeaderGroups().map(hg => (
                    <tr key={hg.id}>{hg.headers.map(h => <th key={h.id} style={thStyle}>{flexRender(h.column.columnDef.header, h.getContext())}</th>)}</tr>
                  ))}
                </thead>
                <tbody>
                  {srcTable.getRowModel().rows.map(row => (
                    <tr key={row.id}>
                      {row.getVisibleCells().map(cell => <td key={cell.id} style={tdStyle}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}
                    </tr>
                  ))}
                </tbody>
              </table>
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
    </Layout>
  )
}

const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem 2rem', minWidth: 380, boxShadow: '0 10px 30px rgba(0,0,0,0.2)' }
const cardStyle: React.CSSProperties = { background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem' }
const cardTitle: React.CSSProperties = { margin: '0 0 0.75rem', fontSize: 15 }
const fieldWrap: React.CSSProperties = { marginBottom: '0.75rem' }
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

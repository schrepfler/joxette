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
import { entitiesApi, cassettesApi, type EntityTypeConfig, type CreateEntityRequest } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { ConfirmDialog } from '../../components/ConfirmDialog'
import { useToast } from '../../components/Toast'

export const Route = createFileRoute('/entities/')({
  component: EntitiesPage,
})

const colHelper = createColumnHelper<EntityTypeConfig>()

function AddEntityModal({ onClose }: { onClose: () => void }) {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const mutation = useMutation({
    mutationFn: (d: CreateEntityRequest) => entitiesApi.create(d),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['entities'] }); addToast('Entity type created', 'success'); onClose() },
    onError: (e: Error) => addToast(e.message, 'error'),
  })
  const form = useForm({
    defaultValues: { type: '', buckets: '', retentionDays: '' },
    onSubmit: async ({ value }) => {
      mutation.mutate({
        type: value.type,
        buckets: value.buckets ? Number(value.buckets) : undefined,
        retentionDays: value.retentionDays ? Number(value.retentionDays) : undefined,
      })
    },
  })

  return (
    <div style={overlayStyle} onClick={onClose}>
      <div style={modalStyle} onClick={e => e.stopPropagation()}>
        <h2 style={{ margin: '0 0 1.25rem', fontSize: 18 }}>Add Entity Type</h2>
        <form onSubmit={(e) => { e.preventDefault(); void form.handleSubmit() }}>
          <form.Field name="type">
            {(f) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>Type *</label>
                <input style={inputStyleFull} value={f.state.value} onChange={e => f.handleChange(e.target.value)} required />
              </div>
            )}
          </form.Field>
          <form.Field name="buckets">
            {(f) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>Buckets</label>
                <input type="number" style={inputStyleFull} value={f.state.value} onChange={e => f.handleChange(e.target.value)} />
              </div>
            )}
          </form.Field>
          <form.Field name="retentionDays">
            {(f) => (
              <div style={fieldWrap}>
                <label style={labelStyle}>Retention Days</label>
                <input type="number" style={inputStyleFull} value={f.state.value} onChange={e => f.handleChange(e.target.value)} />
              </div>
            )}
          </form.Field>
          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '0.5rem' }}>
            <button type="button" onClick={onClose} style={cancelBtnStyle}>Cancel</button>
            <button type="submit" disabled={mutation.isPending} style={primaryBtnStyle}>{mutation.isPending ? 'Creating…' : 'Create'}</button>
          </div>
        </form>
      </div>
    </div>
  )
}

function EntitiesPage() {
  const navigate = useNavigate()
  const qc = useQueryClient()
  const { addToast } = useToast()
  const [showAdd, setShowAdd] = useState(false)
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null)
  const [confirmRebuild, setConfirmRebuild] = useState(false)

  const { data, isLoading, error } = useQuery({ queryKey: ['entities'], queryFn: entitiesApi.list })

  const deleteMutation = useMutation({
    mutationFn: (type: string) => entitiesApi.delete(type),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['entities'] }); addToast('Entity type deleted', 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const rebuildMutation = useMutation({
    mutationFn: () => cassettesApi.rebuildKnownEntities(),
    onSuccess: (d) => addToast(`Rebuilt ${d.rebuilt.toLocaleString()} entity rows`, 'success'),
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const columns = [
    colHelper.accessor('entityType', { header: 'Entity Type' }),
    colHelper.accessor('buckets', { header: 'Buckets' }),
    colHelper.accessor('retentionDays', { header: 'Retention Days', cell: i => i.getValue() ?? '—' }),
    colHelper.display({
      id: 'actions',
      header: 'Actions',
      cell: ({ row }) => (
        <button
          style={dangerBtnSmall}
          onClick={e => { e.stopPropagation(); setConfirmDelete(row.original.entityType) }}
        >
          Delete
        </button>
      ),
    }),
  ]

  const table = useReactTable({ data: data ?? [], columns, getCoreRowModel: getCoreRowModel() })

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
        <h1 style={pageTitle}>Entities</h1>
        <div style={{ display: 'flex', gap: 8 }}>
          <button
            style={{ ...primaryBtnStyle, background: '#dd6b20' }}
            onClick={() => setConfirmRebuild(true)}
            disabled={rebuildMutation.isPending}
            title="Rebuild known_entities registry from cassette data on object storage"
          >
            {rebuildMutation.isPending ? 'Rebuilding…' : '⟳ Rebuild Known Entities'}
          </button>
          <button style={primaryBtnStyle} onClick={() => setShowAdd(true)}>+ Add Entity Type</button>
        </div>
      </div>
      {rebuildMutation.isPending && (
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, background: '#fffaf0', border: '1px solid #fbd38d', borderRadius: 6, padding: '0.6rem 1rem', marginBottom: '1rem' }}>
          <div style={{ width: 16, height: 16, border: '2px solid #fbd38d', borderTop: '2px solid #dd6b20', borderRadius: '50%', animation: 'spin 0.8s linear infinite', flexShrink: 0 }} />
          <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
          <span style={{ fontSize: 14, color: '#c05621', fontWeight: 500 }}>Rebuilding known entities registry — scanning all cassette tables, this may take a while…</span>
        </div>
      )}
      {isLoading && <LoadingSpinner />}
      {error && <ErrorMessage message={(error as Error).message} />}
      {!isLoading && !error && (
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
              <tr
                key={row.id}
                style={{ cursor: 'pointer' }}
                onClick={() => void navigate({ to: '/entities/$entityType', params: { entityType: row.original.entityType } })}
                onMouseEnter={e => (e.currentTarget.style.background = '#ebf8ff')}
                onMouseLeave={e => (e.currentTarget.style.background = '')}
              >
                {row.getVisibleCells().map(cell => <td key={cell.id} style={tdStyle}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {showAdd && <AddEntityModal onClose={() => setShowAdd(false)} />}
      {confirmRebuild && (
        <ConfirmDialog
          message="Rebuild the known_entities registry from all entity cassette tables? This will delete all existing registry rows and re-scan the cassette data. Use this to recover after losing the catalog file."
          onConfirm={() => { rebuildMutation.mutate(); setConfirmRebuild(false) }}
          onCancel={() => setConfirmRebuild(false)}
        />
      )}
      {confirmDelete && (
        <ConfirmDialog
          message={`Delete entity type "${confirmDelete}"?`}
          onConfirm={() => { deleteMutation.mutate(confirmDelete); setConfirmDelete(null) }}
          onCancel={() => setConfirmDelete(null)}
        />
      )}
    </Layout>
  )
}

const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem 2rem', minWidth: 380, boxShadow: '0 10px 30px rgba(0,0,0,0.2)' }
const fieldWrap: React.CSSProperties = { marginBottom: '0.75rem' }
const labelStyle: React.CSSProperties = { display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#4a5568' }
const inputStyleFull: React.CSSProperties = { width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14, boxSizing: 'border-box' }
const primaryBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const cancelBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const dangerBtnSmall: React.CSSProperties = { padding: '0.2rem 0.6rem', background: '#e53e3e', color: '#fff', border: 'none', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', background: '#fff', borderRadius: 8, overflow: 'hidden', boxShadow: '0 1px 4px rgba(0,0,0,0.1)' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 0.75rem', background: '#edf2f7', fontSize: 13, fontWeight: 600, color: '#4a5568', borderBottom: '1px solid #e2e8f0' }
const tdStyle: React.CSSProperties = { padding: '0.55rem 0.75rem', borderBottom: '1px solid #e2e8f0', fontSize: 14 }
const pageTitle: React.CSSProperties = { margin: 0, fontSize: 22, fontWeight: 700 }

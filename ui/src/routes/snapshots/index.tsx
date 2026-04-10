import { createFileRoute } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { useForm } from '@tanstack/react-form'
import { useState } from 'react'
import { cassettesApi, type SnapshotInfo } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { ConfirmDialog } from '../../components/ConfirmDialog'
import { useToast } from '../../components/Toast'

export const Route = createFileRoute('/snapshots/')({
  component: SnapshotsPage,
})

const colHelper = createColumnHelper<SnapshotInfo>()

function formatBytes(b: number) {
  if (b < 1024) return `${b} B`
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`
  return `${(b / 1024 / 1024).toFixed(1)} MB`
}

function CreateSnapshotModal({ onClose }: { onClose: () => void }) {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const mutation = useMutation({
    mutationFn: (name?: string) => cassettesApi.createSnapshot(name ? { name } : {}),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['snapshots'] }); addToast('Snapshot created', 'success'); onClose() },
    onError: (e: Error) => addToast(e.message, 'error'),
  })
  const form = useForm({
    defaultValues: { name: '' },
    onSubmit: async ({ value }) => mutation.mutate(value.name || undefined),
  })
  return (
    <div style={overlayStyle} onClick={onClose}>
      <div style={modalStyle} onClick={e => e.stopPropagation()}>
        <h2 style={{ margin: '0 0 1.25rem', fontSize: 18 }}>Create Snapshot</h2>
        <form onSubmit={e => { e.preventDefault(); void form.handleSubmit() }}>
          <form.Field name="name">
            {(f) => (
              <div style={{ marginBottom: '1rem' }}>
                <label style={labelStyle}>Name (optional)</label>
                <input style={inputStyleFull} value={f.state.value} onChange={e => f.handleChange(e.target.value)} placeholder="Auto-generated if empty" />
              </div>
            )}
          </form.Field>
          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
            <button type="button" onClick={onClose} style={cancelBtnStyle}>Cancel</button>
            <button type="submit" disabled={mutation.isPending} style={primaryBtnStyle}>{mutation.isPending ? 'Creating…' : 'Create'}</button>
          </div>
        </form>
      </div>
    </div>
  )
}

function ExportToObjectStoreModal({ onClose }: { onClose: () => void }) {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const mutation = useMutation({
    mutationFn: (name?: string) => cassettesApi.exportSnapshotToObjectStore(name ? { name } : {}),
    onSuccess: (d) => {
      void qc.invalidateQueries({ queryKey: ['snapshots'] })
      addToast(`Snapshot '${d.name}' exported to object storage`, 'success')
      onClose()
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })
  const form = useForm({
    defaultValues: { name: '' },
    onSubmit: async ({ value }) => mutation.mutate(value.name || undefined),
  })
  return (
    <div style={overlayStyle} onClick={onClose}>
      <div style={modalStyle} onClick={e => e.stopPropagation()}>
        <h2 style={{ margin: '0 0 0.75rem', fontSize: 18 }}>Export to Object Storage</h2>
        <p style={{ margin: '0 0 1rem', fontSize: 13, color: '#718096', lineHeight: 1.5 }}>
          Exports the full catalog (config tables, known_entities, DuckLake metadata) directly to the
          configured S3 bucket. No local disk space is used. The snapshot is placed under
          <code style={{ background: '#edf2f7', padding: '1px 4px', borderRadius: 3 }}>&lt;bucket&gt;/snapshots/&lt;name&gt;/</code>.
        </p>
        <form onSubmit={e => { e.preventDefault(); void form.handleSubmit() }}>
          <form.Field name="name">
            {(f) => (
              <div style={{ marginBottom: '1rem' }}>
                <label style={labelStyle}>Name (optional)</label>
                <input style={inputStyleFull} value={f.state.value} onChange={e => f.handleChange(e.target.value)} placeholder="Auto-generated if empty" />
              </div>
            )}
          </form.Field>
          {mutation.isPending && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, background: '#f0fff4', border: '1px solid #9ae6b4', borderRadius: 6, padding: '0.6rem 1rem', marginBottom: '1rem' }}>
              <div style={{ width: 16, height: 16, border: '2px solid #9ae6b4', borderTop: '2px solid #38a169', borderRadius: '50%', animation: 'spin 0.8s linear infinite', flexShrink: 0 }} />
              <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
              <span style={{ fontSize: 14, color: '#276749', fontWeight: 500 }}>Uploading snapshot to object storage…</span>
            </div>
          )}
          <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
            <button type="button" onClick={onClose} disabled={mutation.isPending} style={cancelBtnStyle}>Cancel</button>
            <button type="submit" disabled={mutation.isPending} style={{ ...primaryBtnStyle, background: '#38a169' }}>
              {mutation.isPending ? 'Exporting…' : '☁ Export to Object Store'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

function SnapshotsPage() {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const [showCreate, setShowCreate] = useState(false)
  const [showExport, setShowExport] = useState(false)
  const [confirmRestore, setConfirmRestore] = useState<string | null>(null)

  const { data, isLoading, error } = useQuery({
    queryKey: ['snapshots'],
    queryFn: cassettesApi.listSnapshots,
  })

  const restoreMutation = useMutation({
    mutationFn: (name: string) => cassettesApi.restoreSnapshot(name),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['snapshots'] }); addToast('Snapshot restore started', 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const columns = [
    colHelper.accessor('name', { header: 'Name' }),
    colHelper.accessor('createdAt', { header: 'Created At', cell: i => i.getValue().slice(0, 19).replace('T', ' ') }),
    colHelper.accessor('sizeBytes', { header: 'Size', cell: i => formatBytes(i.getValue()) }),
    colHelper.display({
      id: 'actions',
      header: 'Actions',
      cell: ({ row }) => (
        <button
          style={primaryBtnSmall}
          onClick={() => setConfirmRestore(row.original.name)}
        >
          Restore
        </button>
      ),
    }),
  ]

  const table = useReactTable({ data: data ?? [], columns, getCoreRowModel: getCoreRowModel() })

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>Snapshots</h1>
        <div style={{ display: 'flex', gap: 8 }}>
          <button style={{ ...primaryBtnStyle, background: '#38a169' }} onClick={() => setShowExport(true)}>
            ☁ Export to Object Store
          </button>
          <button style={primaryBtnStyle} onClick={() => setShowCreate(true)}>+ Create Local Snapshot</button>
        </div>
      </div>

      {isLoading && <LoadingSpinner />}
      {error && <ErrorMessage message={(error as Error).message} />}

      {!isLoading && !error && (
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
      )}

      {showCreate && <CreateSnapshotModal onClose={() => setShowCreate(false)} />}
      {showExport && <ExportToObjectStoreModal onClose={() => setShowExport(false)} />}
      {confirmRestore && (
        <ConfirmDialog
          message={`Restore snapshot "${confirmRestore}"? This will overwrite current data.`}
          onConfirm={() => { restoreMutation.mutate(confirmRestore); setConfirmRestore(null) }}
          onCancel={() => setConfirmRestore(null)}
        />
      )}
    </Layout>
  )
}

const overlayStyle: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }
const modalStyle: React.CSSProperties = { background: '#fff', borderRadius: 8, padding: '1.5rem 2rem', minWidth: 380, boxShadow: '0 10px 30px rgba(0,0,0,0.2)' }
const labelStyle: React.CSSProperties = { display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#4a5568' }
const inputStyleFull: React.CSSProperties = { width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14, boxSizing: 'border-box' }
const primaryBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const cancelBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const primaryBtnSmall: React.CSSProperties = { padding: '0.2rem 0.6rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', background: '#fff', borderRadius: 8, overflow: 'hidden', boxShadow: '0 1px 4px rgba(0,0,0,0.1)' }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.6rem 0.75rem', background: '#edf2f7', fontSize: 13, fontWeight: 600, color: '#4a5568', borderBottom: '1px solid #e2e8f0' }
const tdStyle: React.CSSProperties = { padding: '0.55rem 0.75rem', borderBottom: '1px solid #e2e8f0', fontSize: 14 }

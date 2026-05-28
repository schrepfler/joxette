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
import { ModalDialog } from '../../components/ModalDialog'
import { useToast } from '../../components/Toast'
import {
  pageTitle, primaryBtnStyle, primaryBtnSmall, cancelBtnStyle, labelStyle,
  tableStyle, thStyle, tdStyle,
} from '../../styles/shared'

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
    <ModalDialog title="Create Snapshot" onClose={onClose} style={{ minWidth: 380 }}>
      <form onSubmit={e => { e.preventDefault(); void form.handleSubmit() }}>
        <form.Field name="name">
          {(f) => (
            <div style={{ marginBottom: 20 }}>
              <label style={labelStyle}>Name (optional)</label>
              <input className="jx-input-box" value={f.state.value} onChange={e => f.handleChange(e.target.value)} placeholder="Auto-generated if empty" />
            </div>
          )}
        </form.Field>
        <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
          <button type="button" onClick={onClose} style={cancelBtnStyle}>Cancel</button>
          <button type="submit" disabled={mutation.isPending} style={primaryBtnStyle}>{mutation.isPending ? 'Creating…' : 'Create'}</button>
        </div>
      </form>
    </ModalDialog>
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
    <ModalDialog title="Export to Object Storage" onClose={onClose} style={{ minWidth: 380 }}>
      <p style={{ margin: '0 0 16px', fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-secondary)', lineHeight: 1.6 }}>
        Exports the full catalog (config tables, known_entities, DuckLake metadata) directly to the
        configured S3 bucket. No local disk space is used. The snapshot is placed under{' '}
        <code>&lt;bucket&gt;/snapshots/&lt;name&gt;/</code>.
      </p>
      <form onSubmit={e => { e.preventDefault(); void form.handleSubmit() }}>
        <form.Field name="name">
          {(f) => (
            <div style={{ marginBottom: 16 }}>
              <label style={labelStyle}>Name (optional)</label>
              <input className="jx-input-box" value={f.state.value} onChange={e => f.handleChange(e.target.value)} placeholder="Auto-generated if empty" />
            </div>
          )}
        </form.Field>
        {mutation.isPending && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, background: '#dcfce7', border: '1px solid var(--signal-live)', borderRadius: 'var(--radius-sm)', padding: '10px 14px', marginBottom: 16 }}>
            <span aria-hidden="true" className="jx-spin" style={{ display: 'inline-block', width: 16, height: 16, border: '2px solid var(--signal-live)', borderTopColor: 'transparent', borderRadius: '50%', flexShrink: 0 }} />
            <span style={{ fontSize: 'var(--type-body-sm-size)', color: 'var(--signal-live-ink)', fontWeight: 500 }}>Uploading snapshot to object storage…</span>
          </div>
        )}
        <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
          <button type="button" onClick={onClose} disabled={mutation.isPending} style={cancelBtnStyle}>Cancel</button>
          <button type="submit" disabled={mutation.isPending} style={{ ...primaryBtnStyle, background: 'var(--signal-live)', borderColor: 'var(--signal-live)' }}>
            {mutation.isPending ? 'Exporting…' : 'Export to Object Store'}
          </button>
        </div>
      </form>
    </ModalDialog>
  )
}

function SnapshotsPage() {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const [showCreate, setShowCreate] = useState(false)
  const [showExport, setShowExport] = useState(false)
  const [confirmRestore, setConfirmRestore] = useState<string | null>(null)

  const { data, isLoading, error } = useQuery({ queryKey: ['snapshots'], queryFn: cassettesApi.listSnapshots })

  const restoreMutation = useMutation({
    mutationFn: (name: string) => cassettesApi.restoreSnapshot(name),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['snapshots'] }); addToast('Snapshot restore started', 'success') },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const columns = [
    colHelper.accessor('name', { header: 'Name' }),
    colHelper.accessor('createdAt', { header: 'Created At', cell: i => <span style={monoCell}>{i.getValue().slice(0, 19).replace('T', ' ')}</span> }),
    colHelper.accessor('sizeBytes', { header: 'Size', cell: i => <span style={monoCell}>{formatBytes(i.getValue())}</span> }),
    colHelper.display({
      id: 'actions',
      header: 'Actions',
      cell: ({ row }) => (
        <button style={primaryBtnSmall} onClick={() => setConfirmRestore(row.original.name)}>Restore</button>
      ),
    }),
  ]

  const table = useReactTable({ data: data ?? [], columns, getCoreRowModel: getCoreRowModel() })

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 24 }}>
        <h1 style={pageTitle}>Snapshots</h1>
        <div style={{ display: 'flex', gap: 8 }}>
          <button style={{ ...primaryBtnStyle, background: 'var(--signal-live)', borderColor: 'var(--signal-live)' }} onClick={() => setShowExport(true)}>
            Export to Object Store
          </button>
          <button style={primaryBtnStyle} onClick={() => setShowCreate(true)}>+ Create Local Snapshot</button>
        </div>
      </div>

      {isLoading && <LoadingSpinner />}
      {error && <ErrorMessage message={(error as Error).message} />}

      {!isLoading && !error && (
        <div style={tableStyle}>
          <table aria-label="DuckLake snapshots" style={{ width: '100%', borderCollapse: 'collapse' }}>
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
        </div>
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

const monoCell: React.CSSProperties = {
  fontFamily: 'var(--font-mono)',
  fontSize: 'var(--type-mono-size)',
  fontVariantNumeric: 'tabular-nums',
}

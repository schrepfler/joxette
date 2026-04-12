import { createFileRoute } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { useState, useEffect, useRef } from 'react'
import { compactionApi, type CompactionRun, type CompactionConfig } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { useToast } from '../../components/Toast'

export const Route = createFileRoute('/compaction/')({
  component: CompactionPage,
})

const colHelper = createColumnHelper<CompactionRun>()

function CompactionPage() {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const [targetsInput, setTargetsInput] = useState('')

  const statusQuery = useQuery({
    queryKey: ['compaction', 'status'],
    queryFn: compactionApi.getStatus,
    refetchInterval: (query) => query.state.data?.running ? 2_000 : 10_000,
  })

  const historyQuery = useQuery({
    queryKey: ['compaction', 'history'],
    queryFn: () => compactionApi.getHistory(20),
  })

  const configQuery = useQuery({
    queryKey: ['compaction', 'config'],
    queryFn: compactionApi.getConfig,
    staleTime: Infinity,
  })

  const triggerMutation = useMutation({
    mutationFn: () => {
      const targets = targetsInput.trim()
        ? targetsInput.split(',').map(s => s.trim()).filter(Boolean)
        : undefined
      return compactionApi.trigger({ targets })
    },
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['compaction'] })
      addToast('Compaction triggered', 'success')
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const columns = [
    colHelper.accessor('id', { header: 'ID' }),
    colHelper.accessor('startedAt', { header: 'Started At', cell: i => i.getValue().slice(0, 19).replace('T', ' ') }),
    colHelper.accessor('completedAt', { header: 'Completed At', cell: i => i.getValue()?.slice(0, 19).replace('T', ' ') ?? '—' }),
    colHelper.accessor('status', {
      header: 'Status',
      cell: i => {
        const s = i.getValue()
        const color = s === 'completed' ? '#276749' : s === 'running' ? '#2b6cb0' : '#9b2c2c'
        const bg = s === 'completed' ? '#c6f6d5' : s === 'running' ? '#bee3f8' : '#fed7d7'
        return <span style={{ background: bg, color, padding: '2px 8px', borderRadius: 12, fontSize: 12 }}>{s}</span>
      },
    }),
    colHelper.accessor('triggeredBy', { header: 'Triggered By' }),
    colHelper.accessor('targets', { header: 'Targets', cell: i => i.getValue()?.join(', ') ?? '—' }),
    colHelper.accessor('entityBucketsCompacted', { header: 'Entity Buckets' }),
    colHelper.accessor('generalPartitionsCompacted', { header: 'Partitions' }),
  ]

  const table = useReactTable({ data: historyQuery.data ?? [], columns, getCoreRowModel: getCoreRowModel() })
  const status = statusQuery.data

  const prevRunningRef = useRef<boolean | undefined>(undefined)
  useEffect(() => {
    const running = status?.running
    if (prevRunningRef.current === true && running === false) {
      if (status?.lastRun?.status === 'completed') {
        addToast('Compaction completed successfully', 'success')
      } else if (status?.lastRun?.status === 'failed') {
        addToast(`Compaction failed: ${status.lastRun.errorMessage ?? 'unknown error'}`, 'error')
      }
      void qc.invalidateQueries({ queryKey: ['compaction', 'history'] })
    }
    prevRunningRef.current = running
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [status?.running])

  return (
    <Layout>
      <h1 style={{ margin: '0 0 1.5rem', fontSize: 22, fontWeight: 700 }}>Compaction</h1>

      {statusQuery.isLoading && <LoadingSpinner />}
      {statusQuery.error && <ErrorMessage message={(statusQuery.error as Error).message} />}

      {status && (
        <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem' }}>
          <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Status</h3>
          {status?.running && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, background: '#ebf8ff', border: '1px solid #bee3f8', borderRadius: 6, padding: '0.6rem 1rem', marginBottom: '0.75rem' }}>
              <div style={{ width: 16, height: 16, border: '2px solid #bee3f8', borderTop: '2px solid #2b6cb0', borderRadius: '50%', animation: 'spin 0.8s linear infinite', flexShrink: 0 }} />
              <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
              <span style={{ fontSize: 14, color: '#2b6cb0', fontWeight: 500 }}>Compaction is running — polling every 2 s…</span>
            </div>
          )}
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: '1rem' }}>
            <div style={statCard}>
              <div style={statLabel}>Running</div>
              <div style={{ ...statValue, color: status.running ? '#2b6cb0' : '#276749' }}>
                {status.running ? 'Yes' : 'No'}
              </div>
            </div>
            {status.nextScheduledRun && (
              <div style={statCard}>
                <div style={statLabel}>Next Run</div>
                <div style={statValue}>{status.nextScheduledRun.slice(0, 19).replace('T', ' ')}</div>
              </div>
            )}
            {status.lastRun && (
              <>
                <div style={statCard}>
                  <div style={statLabel}>Last Run Status</div>
                  <div style={statValue}>{status.lastRun.status}</div>
                </div>
                <div style={statCard}>
                  <div style={statLabel}>Last Run Completed</div>
                  <div style={statValue}>{status.lastRun.completedAt?.slice(0, 19).replace('T', ' ') ?? '—'}</div>
                </div>
              </>
            )}
          </div>

          {/* Trigger */}
          <div style={{ display: 'flex', gap: 10, alignItems: 'flex-end', flexWrap: 'wrap' }}>
            <div>
              <label style={labelStyle}>Targets (comma-separated, optional)</label>
              <input
                style={{ padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14, width: 280 }}
                placeholder="topic1, topic2"
                value={targetsInput}
                onChange={e => setTargetsInput(e.target.value)}
              />
            </div>
            <button
              style={{ padding: '0.45rem 1rem', background: '#805ad5', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }}
              disabled={triggerMutation.isPending}
              onClick={() => triggerMutation.mutate()}
            >
              {triggerMutation.isPending ? 'Triggering…' : 'Trigger Compaction'}
            </button>
          </div>
        </div>
      )}

      {configQuery.data && <CompactionConfigCard config={configQuery.data} />}

      <h2 style={{ margin: '0 0 0.75rem', fontSize: 16, fontWeight: 600 }}>History (last 20)</h2>
      {historyQuery.isLoading && <LoadingSpinner />}
      {historyQuery.error && <ErrorMessage message={(historyQuery.error as Error).message} />}
      {!historyQuery.isLoading && (
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
    </Layout>
  )
}

const statCard: React.CSSProperties = { background: '#f7fafc', border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.5rem 0.85rem', minWidth: 130 }
const statLabel: React.CSSProperties = { fontSize: 11, color: '#718096', marginBottom: 2 }
const statValue: React.CSSProperties = { fontSize: 15, fontWeight: 600 }
const labelStyle: React.CSSProperties = { display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#4a5568' }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', background: '#fff', borderRadius: 8, overflow: 'hidden', boxShadow: '0 1px 4px rgba(0,0,0,0.1)', fontSize: 13 }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.55rem 0.75rem', background: '#edf2f7', fontWeight: 600, color: '#4a5568', borderBottom: '1px solid #e2e8f0' }
const tdStyle: React.CSSProperties = { padding: '0.5rem 0.75rem', borderBottom: '1px solid #e2e8f0' }

function CompactionConfigCard({ config }: { config: CompactionConfig }) {
  return (
    <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem' }}>
      <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Active Configuration</h3>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: 10 }}>
        <ConfigField label="Schedule (cron)" value={config.schedule} mono />
        <ConfigField label="Entity · Lookback" value={`${config.entity.lookbackDays} days`} />
        <ConfigField label="Entity · Min Files / Bucket" value={String(config.entity.minFilesPerBucket)} />
        <ConfigField label="Entity · Target File Size" value={`${config.entity.targetFileSizeMb} MB`} />
        <ConfigField label="General · Enabled" value={config.general.enabled ? 'Yes' : 'No'} />
        <ConfigField label="General · Lookback" value={`${config.general.lookbackDays} days`} />
        <ConfigField label="General · Min Files / Partition" value={String(config.general.minFilesPerPartition)} />
        <ConfigField label="General · Target File Size" value={`${config.general.targetFileSizeMb} MB`} />
      </div>
    </div>
  )
}

function ConfigField({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div style={{ background: '#f7fafc', border: '1px solid #e2e8f0', borderRadius: 6, padding: '0.5rem 0.75rem' }}>
      <div style={{ fontSize: 11, color: '#718096', marginBottom: 3 }}>{label}</div>
      <div style={{ fontSize: 13, fontWeight: 600, fontFamily: mono ? 'monospace' : undefined }}>{value}</div>
    </div>
  )
}

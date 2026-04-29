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
import {
  pageTitle, primaryBtnStyle, cardStyle, labelStyle, tableStyle, thStyle, tdStyle,
} from '../../styles/shared'

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
    colHelper.accessor('startedAt', { header: 'Started', cell: i => <span style={monoCell}>{i.getValue().slice(0, 19).replace('T', ' ')}</span> }),
    colHelper.accessor('completedAt', { header: 'Completed', cell: i => <span style={monoCell}>{i.getValue()?.slice(0, 19).replace('T', ' ') ?? '—'}</span> }),
    colHelper.accessor('status', {
      header: 'Status',
      cell: i => {
        const s = i.getValue()
        const cls = s === 'completed' ? 'jx-badge-success' : s === 'running' ? 'jx-badge-accent' : 'jx-badge-error'
        return <span className={`jx-badge ${cls}`}>{s}</span>
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
      <h1 style={{ ...pageTitle, marginBottom: 24 }}>Compaction</h1>

      {statusQuery.isLoading && <LoadingSpinner />}
      {statusQuery.error && <ErrorMessage message={(statusQuery.error as Error).message} />}

      {status && (
        <div style={{ ...cardStyle, marginBottom: 24 }}>
          <h3 style={subheading}>Status</h3>

          {status.running && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, background: 'var(--accent-wash)', border: '1px solid var(--accent)', borderRadius: 'var(--radius-sm)', padding: '10px 14px', marginBottom: 12 }}>
              <span className="jx-spin" style={{ display: 'inline-block', width: 16, height: 16, border: '2px solid var(--accent)', borderTopColor: 'transparent', borderRadius: '50%', flexShrink: 0 }} />
              <span style={{ fontSize: 'var(--type-body-sm-size)', color: 'var(--accent-muted)', fontWeight: 500 }}>Compaction is running — polling every 2 s…</span>
            </div>
          )}

          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 20 }}>
            <StatCard label="Running" value={status.running ? 'Yes' : 'No'} valueColor={status.running ? 'var(--signal-live)' : 'var(--ink-secondary)'} />
            {status.nextScheduledRun && (
              <StatCard label="Next Run" value={status.nextScheduledRun.slice(0, 19).replace('T', ' ')} mono />
            )}
            {status.lastRun && (
              <>
                <StatCard label="Last Run Status" value={status.lastRun.status} />
                <StatCard label="Last Run Completed" value={status.lastRun.completedAt?.slice(0, 19).replace('T', ' ') ?? '—'} mono />
              </>
            )}
          </div>

          <div style={{ display: 'flex', gap: 10, alignItems: 'flex-end', flexWrap: 'wrap' }}>
            <div>
              <label style={labelStyle}>Targets (comma-separated, optional)</label>
              <input
                className="jx-input-box"
                style={{ width: 280 }}
                placeholder="topic1, topic2"
                value={targetsInput}
                onChange={e => setTargetsInput(e.target.value)}
              />
            </div>
            <button
              style={primaryBtnStyle}
              disabled={triggerMutation.isPending}
              onClick={() => triggerMutation.mutate()}
            >
              {triggerMutation.isPending ? 'Triggering…' : 'Trigger Compaction'}
            </button>
          </div>
        </div>
      )}

      {configQuery.data && <CompactionConfigCard config={configQuery.data} />}

      <h2 style={{ ...subheading, margin: '0 0 12px' }}>History (last 20)</h2>
      {historyQuery.isLoading && <LoadingSpinner />}
      {historyQuery.error && <ErrorMessage message={(historyQuery.error as Error).message} />}
      {!historyQuery.isLoading && (
        <div style={tableStyle}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
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
    </Layout>
  )
}

function StatCard({ label, value, mono, valueColor }: { label: string; value: string; mono?: boolean; valueColor?: string }) {
  return (
    <div style={{ ...cardStyle, minWidth: 130, padding: '10px 14px' }}>
      <div style={{ fontSize: 'var(--type-micro-size)', fontWeight: 'var(--type-micro-weight)' as unknown as number, letterSpacing: 'var(--type-micro-tracking)', textTransform: 'uppercase', color: 'var(--ink-tertiary)', marginBottom: 4 }}>
        {label}
      </div>
      <div style={{ fontSize: 'var(--type-body-sm-size)', fontWeight: 600, fontFamily: mono ? 'var(--font-mono)' : undefined, color: valueColor ?? 'var(--ink-primary)' }}>
        {value}
      </div>
    </div>
  )
}

function CompactionConfigCard({ config }: { config: CompactionConfig }) {
  return (
    <div style={{ ...cardStyle, marginBottom: 24 }}>
      <h3 style={subheading}>Active Configuration</h3>
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
    <div style={{ background: 'var(--surface-raised)', border: '1px solid var(--rule)', borderRadius: 'var(--radius-xs)', padding: '8px 12px' }}>
      <div style={{ fontSize: 'var(--type-micro-size)', color: 'var(--ink-tertiary)', textTransform: 'uppercase', letterSpacing: 'var(--type-micro-tracking)', fontWeight: 600, marginBottom: 3 }}>{label}</div>
      <div style={{ fontSize: 'var(--type-body-sm-size)', fontWeight: 600, fontFamily: mono ? 'var(--font-mono)' : undefined, color: 'var(--ink-primary)' }}>{value}</div>
    </div>
  )
}

const subheading: React.CSSProperties = {
  margin: '0 0 12px',
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-h4-size)',
  fontWeight: 600,
  color: 'var(--ink-primary)',
}

const monoCell: React.CSSProperties = {
  fontFamily: 'var(--font-mono)',
  fontSize: 'var(--type-mono-size)',
  fontVariantNumeric: 'tabular-nums',
}

import { createFileRoute } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { useEffect, useRef } from 'react'
import { retentionApi, type RetentionRun } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { useToast } from '../../components/Toast'
import {
  pageTitle, primaryBtnStyle, cardStyle, tableStyle, thStyle, tdStyle,
} from '../../styles/shared'

export const Route = createFileRoute('/retention/')({
  component: RetentionPage,
})

const colHelper = createColumnHelper<RetentionRun>()

function RetentionPage() {
  const qc = useQueryClient()
  const { addToast } = useToast()

  const statusQuery = useQuery({
    queryKey: ['retention', 'status'],
    queryFn: retentionApi.getStatus,
    refetchInterval: (query) => query.state.data?.running ? 2_000 : 30_000,
  })

  const historyQuery = useQuery({
    queryKey: ['retention', 'history'],
    queryFn: () => retentionApi.getHistory(20),
  })

  const triggerMutation = useMutation({
    mutationFn: retentionApi.trigger,
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['retention'] })
      addToast('Retention run started', 'success')
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  // Toast when a running retention run transitions to completed/failed
  const prevRunningRef = useRef<boolean | undefined>(undefined)
  const status = statusQuery.data
  useEffect(() => {
    const running = status?.running
    if (prevRunningRef.current === true && running === false) {
      if (status?.lastRun?.status === 'completed') {
        addToast('Retention run completed', 'success')
      } else if (status?.lastRun?.status === 'failed') {
        addToast(`Retention run failed: ${status.lastRun.errorMessage ?? 'unknown error'}`, 'error')
      }
      void qc.invalidateQueries({ queryKey: ['retention', 'history'] })
    }
    prevRunningRef.current = running
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [status?.running])

  const columns = [
    colHelper.accessor('id', { header: 'ID' }),
    colHelper.accessor('startedAt', {
      header: 'Started',
      cell: i => <span style={monoCell}>{i.getValue().slice(0, 19).replace('T', ' ')}</span>,
    }),
    colHelper.accessor('completedAt', {
      header: 'Completed',
      cell: i => <span style={monoCell}>{i.getValue()?.slice(0, 19).replace('T', ' ') ?? '—'}</span>,
    }),
    colHelper.accessor('status', {
      header: 'Status',
      cell: i => {
        const s = i.getValue()
        const cls = s === 'completed' ? 'jx-badge-success' : s === 'running' ? 'jx-badge-accent' : 'jx-badge-error'
        return <span className={`jx-badge ${cls}`}>{s}</span>
      },
    }),
    colHelper.accessor('triggeredBy', { header: 'Triggered By' }),
    colHelper.accessor('generalRowsDeleted', { header: 'General Rows' }),
    colHelper.accessor('entityRowsDeleted', { header: 'Entity Rows' }),
    colHelper.accessor('knownEntitiesDeleted', { header: 'Known Entities' }),
    colHelper.accessor('errorMessage', {
      header: 'Error',
      cell: i => i.getValue()
        ? <span style={{ color: 'var(--signal-error)', fontFamily: 'var(--font-mono)', fontSize: 12 }}>{i.getValue()}</span>
        : '—',
    }),
  ]

  const table = useReactTable({ data: historyQuery.data ?? [], columns, getCoreRowModel: getCoreRowModel() })

  return (
    <Layout>
      <h1 style={{ ...pageTitle, marginBottom: 24 }}>Retention</h1>

      {statusQuery.isLoading && <LoadingSpinner />}
      {statusQuery.error && <ErrorMessage message={(statusQuery.error as Error).message} />}

      {status && (
        <div style={{ ...cardStyle, marginBottom: 24 }}>
          <h3 style={subheading}>Status</h3>

          {status.running && (
            <div style={{
              display: 'flex', alignItems: 'center', gap: 10,
              background: 'var(--accent-wash)', border: '1px solid var(--accent)',
              borderRadius: 'var(--radius-sm)', padding: '10px 14px', marginBottom: 12,
            }}>
              <span className="jx-spin" style={{ display: 'inline-block', width: 16, height: 16, border: '2px solid var(--accent)', borderTopColor: 'transparent', borderRadius: '50%', flexShrink: 0 }} />
              <span style={{ fontSize: 'var(--type-body-sm-size)', color: 'var(--accent-muted)', fontWeight: 500 }}>
                Retention run is in progress — polling every 2 s…
              </span>
            </div>
          )}

          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 20 }}>
            <StatCard label="Running" value={status.running ? 'Yes' : 'No'} valueColor={status.running ? 'var(--signal-live)' : 'var(--ink-secondary)'} />
            {status.nextScheduledRun && (
              <StatCard label="Next Run" value={status.nextScheduledRun.slice(0, 19).replace('T', ' ')} mono />
            )}
            {status.lastRun && (
              <>
                <StatCard label="Last Status" value={status.lastRun.status} />
                <StatCard label="Last Completed" value={status.lastRun.completedAt?.slice(0, 19).replace('T', ' ') ?? '—'} mono />
                <StatCard label="General Rows" value={status.lastRun.generalRowsDeleted.toLocaleString()} />
                <StatCard label="Entity Rows" value={status.lastRun.entityRowsDeleted.toLocaleString()} />
                <StatCard label="Known Entities" value={status.lastRun.knownEntitiesDeleted.toLocaleString()} />
              </>
            )}
          </div>

          <button
            style={primaryBtnStyle}
            disabled={triggerMutation.isPending || status.running}
            onClick={() => triggerMutation.mutate()}
          >
            {triggerMutation.isPending || status.running ? 'Running…' : 'Trigger Retention'}
          </button>
        </div>
      )}

      <h2 style={{ ...subheading, margin: '0 0 12px' }}>History (last 20)</h2>
      {historyQuery.isLoading && <LoadingSpinner />}
      {historyQuery.error && <ErrorMessage message={(historyQuery.error as Error).message} />}
      {!historyQuery.isLoading && (
        <div style={tableStyle}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
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
          {(historyQuery.data ?? []).length === 0 && (
            <p style={{ padding: '12px 16px', fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-tertiary)' }}>
              No retention runs recorded yet.
            </p>
          )}
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

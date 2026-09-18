import { createFileRoute } from '@tanstack/react-router'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  useTable,
  tableFeatures,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { useEffect, useRef, useState } from 'react'
import { reconciliationApi, type ReconciliationRun } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { useToast } from '../../components/Toast'
import {
  pageTitle, primaryBtnStyle, cardStyle, tableStyle, thStyle, tdStyle,
} from '../../styles/shared'

export const Route = createFileRoute('/reconciliation/')({
  component: ReconciliationPage,
})

const features = tableFeatures({})
const colHelper = createColumnHelper<typeof features, ReconciliationRun>()

function formatBytes(b: number) {
  if (b < 0) return 'N/A'
  if (b < 1024) return `${b} B`
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`
  if (b < 1024 * 1024 * 1024) return `${(b / 1024 / 1024).toFixed(1)} MB`
  return `${(b / 1024 / 1024 / 1024).toFixed(2)} GB`
}

function ReconciliationPage() {
  const qc = useQueryClient()
  const { addToast } = useToast()
  const [recoverOrphanedFiles, setRecoverOrphanedFiles] = useState(false)

  const statusQuery = useQuery({
    queryKey: ['reconciliation', 'status'],
    queryFn: reconciliationApi.getStatus,
    refetchInterval: (query) => query.state.data?.running ? 2_000 : 30_000,
  })

  const historyQuery = useQuery({
    queryKey: ['reconciliation', 'history'],
    queryFn: () => reconciliationApi.getHistory(20),
  })

  const triggerMutation = useMutation({
    mutationFn: () => reconciliationApi.trigger({ recoverOrphanedFiles }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['reconciliation'] })
      addToast('Reconciliation triggered', 'success')
    },
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  const status = statusQuery.data
  const lastRun = status?.lastRun

  // Toast when a running reconciliation run transitions to completed/failed
  const prevRunningRef = useRef<boolean | undefined>(undefined)
  useEffect(() => {
    const running = status?.running
    if (prevRunningRef.current === true && running === false) {
      if (status?.lastRun?.status === 'completed') {
        addToast('Reconciliation completed', 'success')
      } else if (status?.lastRun?.status === 'failed') {
        addToast(`Reconciliation failed: ${status.lastRun.errorMessage ?? 'unknown error'}`, 'error')
      }
      void qc.invalidateQueries({ queryKey: ['reconciliation', 'history'] })
    }
    prevRunningRef.current = running
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [status?.running])

  const columns = colHelper.columns([
    colHelper.accessor('id', { header: 'ID' }),
    colHelper.accessor('startedAt', { header: 'Started', cell: i => <span style={monoCell}>{i.getValue().slice(0, 19).replace('T', ' ')}</span> }),
    colHelper.accessor('status', {
      header: 'Status',
      cell: i => {
        const s = i.getValue()
        const cls = s === 'completed' ? 'jx-badge-success' : s === 'running' ? 'jx-badge-accent' : 'jx-badge-error'
        return <span className={`jx-badge ${cls}`}>{s}</span>
      },
    }),
    colHelper.accessor('tablesScanned', { header: 'Tables' }),
    colHelper.accessor('orphanedFiles', { header: 'Orphaned' }),
    colHelper.accessor('missingFiles', { header: 'Missing' }),
    colHelper.accessor('recoveredFiles', { header: 'Recovered', cell: i => i.getValue() || '—' }),
  ])

  const table = useTable({ features, columns, data: historyQuery.data ?? [] })

  return (
    <Layout>
      <h1 style={{ ...pageTitle, marginBottom: 24 }}>Reconciliation</h1>

      {statusQuery.isLoading && <LoadingSpinner />}
      {statusQuery.error && <ErrorMessage message={(statusQuery.error as Error).message} />}

      {lastRun && (lastRun.missingFiles > 0 || lastRun.orphanedFiles > 0) && (
        <SeverityBanner run={lastRun} />
      )}

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
                Reconciliation is running — polling every 2 s…
              </span>
            </div>
          )}

          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 20 }}>
            <StatCard label="Status" value={lastRun?.status ?? '—'} valueColor={
              lastRun?.status === 'failed' ? 'var(--signal-error)' : lastRun?.status === 'completed' ? 'var(--signal-live)' : undefined
            } />
            {status.nextScheduledRun && (
              <StatCard label="Next Run" value={status.nextScheduledRun.slice(0, 19).replace('T', ' ')} mono />
            )}
            {lastRun && (
              <>
                <StatCard label="Tables Scanned" value={String(lastRun.tablesScanned)} />
                <StatCard
                  label="Orphaned Files"
                  value={`${lastRun.orphanedFiles} / ${formatBytes(lastRun.orphanedBytes)}`}
                  valueColor={lastRun.orphanedFiles > 0 ? 'var(--signal-warn-ink)' : undefined}
                />
                <StatCard
                  label="Missing Files"
                  value={`${lastRun.missingFiles} / ${formatBytes(lastRun.missingBytes)}`}
                  valueColor={lastRun.missingFiles > 0 ? 'var(--signal-error-ink)' : undefined}
                />
              </>
            )}
          </div>

          <div style={{ display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' }}>
            <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-secondary)', cursor: 'pointer' }}>
              <input
                type="checkbox"
                checked={recoverOrphanedFiles}
                onChange={e => setRecoverOrphanedFiles(e.target.checked)}
              />
              Recover orphaned files
            </label>
            <button
              style={{
                ...primaryBtnStyle,
                ...(recoverOrphanedFiles ? { background: 'var(--signal-warn)', color: '#1f1300' } : {}),
              }}
              disabled={triggerMutation.isPending || status.running}
              onClick={() => triggerMutation.mutate()}
            >
              {triggerMutation.isPending || status.running
                ? 'Running…'
                : recoverOrphanedFiles ? 'Run + recover orphaned files' : 'Run reconciliation'}
            </button>
          </div>
        </div>
      )}

      <h2 style={{ ...subheading, margin: '0 0 12px' }}>History (last 20)</h2>
      {historyQuery.isLoading && <LoadingSpinner />}
      {historyQuery.error && <ErrorMessage message={(historyQuery.error as Error).message} />}
      {!historyQuery.isLoading && (
        <div style={tableStyle}>
          <table aria-label="Reconciliation run history" style={{ width: '100%', borderCollapse: 'collapse' }}>
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
                  {row.getAllCells().map(cell => <td key={cell.id} style={tdStyle}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}
                </tr>
              ))}
            </tbody>
          </table>
          {(historyQuery.data ?? []).length === 0 && (
            <p style={{ padding: '12px 16px', fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-tertiary)' }}>
              No reconciliation runs recorded yet.
            </p>
          )}
        </div>
      )}
    </Layout>
  )
}

function SeverityBanner({ run }: { run: ReconciliationRun }) {
  const error = run.missingFiles > 0
  const tone = error
    ? { bg: '#fef2f2', border: 'var(--signal-error)', text: 'var(--signal-error-ink)' }
    : { bg: '#fffbeb', border: 'var(--signal-warn)', text: 'var(--signal-warn-ink)' }
  const checkedAt = run.completedAt?.slice(0, 19).replace('T', ' ') ?? run.startedAt.slice(0, 19).replace('T', ' ')
  const message = error
    ? `${run.missingFiles} file${run.missingFiles === 1 ? '' : 's'} referenced by the catalog ${run.missingFiles === 1 ? 'is' : 'are'} missing from object storage — possible data loss. Last checked ${checkedAt}.`
    : `${run.orphanedFiles} orphaned file${run.orphanedFiles === 1 ? '' : 's'} found in object storage, not tracked by the catalog. Last checked ${checkedAt}.`
  return (
    <div
      role="alert"
      style={{
        display: 'flex', alignItems: 'center', gap: 10,
        background: tone.bg, border: `1px solid ${tone.border}`,
        borderRadius: 'var(--radius-sm)', padding: '10px 14px', marginBottom: 16,
      }}
    >
      <span className={`jx-badge ${error ? 'jx-badge-error' : 'jx-badge-warn'}`}>
        {error ? '⚠ drift detected' : '⚠ orphaned files'}
      </span>
      <span style={{ fontSize: 'var(--type-body-sm-size)', color: tone.text }}>{message}</span>
    </div>
  )
}

function StatCard({ label, value, mono, valueColor }: { label: string; value: string; mono?: boolean; valueColor?: string }) {
  return (
    <div style={{ ...cardStyle, minWidth: 150, padding: '10px 14px' }}>
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

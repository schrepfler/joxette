import { createFileRoute } from '@tanstack/react-router'
import { useQuery } from '@tanstack/react-query'
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
} from '@tanstack/react-table'
import { healthApi, type TopicLag } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'

export const Route = createFileRoute('/health/')({
  component: HealthPage,
})

const colHelper = createColumnHelper<TopicLag>()

function formatBytes(b: number) {
  if (b < 1024) return `${b} B`
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`
  if (b < 1024 * 1024 * 1024) return `${(b / 1024 / 1024).toFixed(1)} MB`
  return `${(b / 1024 / 1024 / 1024).toFixed(2)} GB`
}

function HealthPage() {
  const { data, isLoading, error } = useQuery({
    queryKey: ['health'],
    queryFn: healthApi.get,
    refetchInterval: 30_000,
  })

  const columns = [
    colHelper.accessor('topic', { header: 'Topic' }),
    colHelper.accessor('totalLag', { header: 'Total Lag' }),
    colHelper.display({
      id: 'partitions',
      header: 'Partition Lag',
      cell: ({ row }) => {
        const entries = Object.entries(row.original.lagByPartition)
        return (
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
            {entries.map(([p, lag]) => (
              <span key={p} style={{ background: '#edf2f7', padding: '1px 6px', borderRadius: 4, fontSize: 12 }}>
                p{p}: {lag}
              </span>
            ))}
          </div>
        )
      },
    }),
  ]

  const table = useReactTable({ data: data?.consumerLag ?? [], columns, getCoreRowModel: getCoreRowModel() })

  const statusColor = data?.status === 'UP' || data?.status === 'healthy' ? '#276749' : '#9b2c2c'
  const statusBg = data?.status === 'UP' || data?.status === 'healthy' ? '#c6f6d5' : '#fed7d7'

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: '1.5rem' }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>Health</h1>
        {data && (
          <span style={{ background: statusBg, color: statusColor, padding: '3px 12px', borderRadius: 12, fontSize: 13, fontWeight: 600 }}>
            {data.status}
          </span>
        )}
        <span style={{ fontSize: 12, color: '#a0aec0' }}>auto-refresh every 30s</span>
      </div>

      {isLoading && <LoadingSpinner />}
      {error && <ErrorMessage message={(error as Error).message} />}

      {data && (
        <>
          {/* Overview cards */}
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: '1.5rem' }}>
            {[
              ['Catalog Size', formatBytes(data.catalogSizeBytes)],
              ['Inlined Data Size', formatBytes(data.inlinedDataSizeBytes)],
              ['Catalog Path', data.catalogPath],
            ].map(([k, v]) => (
              <div key={k} style={statCard}>
                <div style={statLabel}>{k}</div>
                <div style={{ ...statValue, wordBreak: 'break-all' }}>{v}</div>
              </div>
            ))}
          </div>

          {/* Active Recorders */}
          <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem' }}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Active Recorders</h3>
            {data.activeRecorders.length === 0 ? (
              <p style={{ margin: 0, color: '#718096', fontSize: 14 }}>No active recorders</p>
            ) : (
              <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                {data.activeRecorders.map(r => (
                  <span key={r} style={{ background: '#c6f6d5', color: '#276749', padding: '3px 10px', borderRadius: 12, fontSize: 13 }}>{r}</span>
                ))}
              </div>
            )}
          </div>

          {/* Consumer Lag */}
          <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem' }}>
            <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Consumer Lag</h3>
            {data.consumerLag.length === 0 ? (
              <p style={{ margin: 0, color: '#718096', fontSize: 14 }}>No consumer lag data</p>
            ) : (
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
          </div>
        </>
      )}
    </Layout>
  )
}

const statCard: React.CSSProperties = { background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '0.75rem 1rem', minWidth: 160, maxWidth: 400 }
const statLabel: React.CSSProperties = { fontSize: 11, color: '#718096', marginBottom: 3 }
const statValue: React.CSSProperties = { fontSize: 15, fontWeight: 600 }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', fontSize: 13 }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.5rem 0.6rem', background: '#edf2f7', fontWeight: 600, color: '#4a5568', borderBottom: '1px solid #e2e8f0' }
const tdStyle: React.CSSProperties = { padding: '0.45rem 0.6rem', borderBottom: '1px solid #e2e8f0' }

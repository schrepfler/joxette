import { createFileRoute } from '@tanstack/react-router'
import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { instancesApi, topicsApi, type RecorderStatus, type InstanceRecord, type MemberView } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { ClusterFlowMap } from '../../components/ClusterFlowMap'
import { pageTitle, cardStyle } from '../../styles/shared'

export const Route = createFileRoute('/cluster/')({
  component: ClusterPage,
})

type Tab = 'map' | 'detail'

function pekkoStatusTone(status: string | null): { bg: string; text: string } {
  if (status === 'up') return { bg: '#dcfce7', text: 'var(--signal-live-ink)' }
  if (status === 'leaving' || status === 'exiting') return { bg: '#fef9c3', text: 'var(--signal-warn-ink)' }
  return { bg: '#fee2e2', text: 'var(--signal-error-ink)' }
}

function timeAgo(ts: string | null): string {
  if (!ts) return '—'
  const diff = Math.floor((Date.now() - new Date(ts).getTime()) / 1000)
  if (diff < 60) return `${diff} s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)} m ago`
  return `${Math.floor(diff / 3600)} h ago`
}

function ClusterPage() {
  const queryClient = useQueryClient()
  const [tab, setTab] = useState<Tab>('map')

  const { data, isLoading, error } = useQuery({
    queryKey: ['cluster-state'],
    queryFn: instancesApi.clusterState,
    refetchInterval: 10_000,
  })

  const pauseMutation = useMutation({
    mutationFn: (topic: string) => topicsApi.pause(topic),
    onSuccess: () => { void queryClient.invalidateQueries({ queryKey: ['cluster-state'] }) },
  })

  const resumeMutation = useMutation({
    mutationFn: (topic: string) => topicsApi.resume(topic),
    onSuccess: () => { void queryClient.invalidateQueries({ queryKey: ['cluster-state'] }) },
  })

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 20 }}>
        <h1 style={pageTitle}>Cluster</h1>
        <div style={{ display: 'flex', gap: 2, marginLeft: 8 }}>
          {(['map', 'detail'] as Tab[]).map(t => (
            <button
              key={t}
              onClick={() => setTab(t)}
              style={{
                background: tab === t ? 'var(--accent)' : 'none',
                color: tab === t ? '#fff' : 'var(--ink-secondary)',
                border: '1px solid',
                borderColor: tab === t ? 'var(--accent)' : 'var(--rule)',
                borderRadius: 'var(--radius-sm)',
                padding: '4px 14px',
                fontSize: 'var(--type-caption-size)',
                cursor: 'pointer',
                fontFamily: 'var(--font-body)',
                fontWeight: tab === t ? 600 : 400,
                transition: 'all var(--duration-quick)',
              }}
            >
              {t === 'map' ? 'Flow Map' : 'Detail'}
            </button>
          ))}
        </div>
      </div>

      {tab === 'map' && <ClusterFlowMap />}

      {tab === 'detail' && (
        <>
      {isLoading && <LoadingSpinner />}
      {error && <ErrorMessage message={(error as Error).message} />}

      {data && (
        <>
          {/* Self node */}
          <div style={{ ...cardStyle, marginBottom: 24 }}>
            <div style={{ display: 'flex', alignItems: 'baseline', gap: 12, marginBottom: 12, flexWrap: 'wrap' }}>
              <h2 style={{ margin: 0, fontSize: 'var(--type-h4-size)', fontWeight: 600 }}>This Node</h2>
              <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-mono-size)', color: 'var(--ink-secondary)' }}>
                {data.self.instanceId}
              </span>
            </div>

            {/* Capabilities */}
            <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center', marginBottom: 12 }}>
              <span className="jx-badge jx-badge-success">replay</span>
              {data.self.recordingEnabled && <span className="jx-badge jx-badge-success">recording</span>}
              {data.self.compactionEnabled && <span className="jx-badge jx-badge-info">compaction</span>}
            </div>

            {/* Pekko + heartbeat row */}
            <div style={{ display: 'flex', gap: 24, flexWrap: 'wrap', marginBottom: 8 }}>
              <div>
                <span style={overline}>Pekko address</span>
                <div style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-mono-size)', color: 'var(--ink-primary)', marginTop: 2 }}>
                  {data.self.pekkoAddress ?? '—'}
                </div>
              </div>
              <div>
                <span style={overline}>Pekko status</span>
                <div style={{ marginTop: 2 }}>
                  {data.self.pekkoStatus ? (
                    <span style={{ ...badgePill, ...pekkoStatusTone(data.self.pekkoStatus) }}>
                      {data.self.pekkoStatus}
                    </span>
                  ) : '—'}
                  {' '}
                  {data.self.pekkoReachable
                    ? <span style={{ color: 'var(--signal-live)', fontSize: 'var(--type-caption-size)' }}>✓ reachable</span>
                    : <span style={{ color: 'var(--signal-error)', fontSize: 'var(--type-caption-size)' }}>✗ unreachable</span>}
                </div>
              </div>
              <div>
                <span style={overline}>Heartbeat</span>
                <div style={{ marginTop: 2 }}>
                  <span className={`jx-badge ${data.self.heartbeatStatus === 'alive' ? 'jx-badge-success' : data.self.heartbeatStatus === 'stale' ? 'jx-badge-warn' : ''}`}>
                    {data.self.heartbeatStatus}
                  </span>
                  <span style={{ marginLeft: 8, fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
                    {timeAgo(data.self.lastHeartbeat)}
                  </span>
                </div>
              </div>
              <div>
                <span style={overline}>Started</span>
                <div style={{ fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-secondary)', marginTop: 2 }}>
                  {timeAgo(data.self.startedAt)}
                </div>
              </div>
              <div>
                <span style={overline}>Catalog</span>
                <div style={{ fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-secondary)', marginTop: 2 }}>
                  {data.self.catalogBackend}
                </div>
              </div>
            </div>

            {/* Recorders table */}
            {Object.keys(data.self.recorders).length > 0 && (
              <div style={{ marginTop: 16 }}>
                <div style={{ ...overline, marginBottom: 8 }}>Recorders</div>
                <table aria-label="Active topic recorders" style={tableStyle}>
                  <thead>
                    <tr>
                      {['Topic', 'Status', 'Lag', 'Partitions', 'Protocol', 'Last Batch', 'Actions'].map(h => (
                        <th key={h} style={thStyle}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(data.self.recorders).map(([topic, rec]) => (
                      <RecorderRow
                        key={topic}
                        topic={topic}
                        rec={rec}
                        onPause={() => pauseMutation.mutate(topic)}
                        onResume={() => resumeMutation.mutate(topic)}
                        isPending={pauseMutation.isPending || resumeMutation.isPending}
                      />
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* All instances */}
          <div style={{ ...cardStyle, marginBottom: 24 }}>
            <h2 style={{ margin: '0 0 16px', fontSize: 'var(--type-h4-size)', fontWeight: 600 }}>
              Instances <span style={{ fontWeight: 400, color: 'var(--ink-tertiary)', fontSize: 'var(--type-body-sm-size)' }}>({data.instances.length})</span>
            </h2>
            {data.instances.length === 0 ? (
              <p style={{ margin: 0, color: 'var(--ink-tertiary)', fontSize: 'var(--type-body-sm-size)' }}>No instances registered</p>
            ) : (
              <table aria-label="Cluster instances" style={tableStyle}>
                <thead>
                  <tr>
                    {['Instance ID', 'Capabilities', 'Catalog', 'Status', 'Last Heartbeat', 'Topics'].map(h => (
                      <th key={h} style={thStyle}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.instances.map(inst => (
                    <InstanceRow key={inst.instanceId} inst={inst} />
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* Pekko topology */}
          <div style={cardStyle}>
            <h2 style={{ margin: '0 0 16px', fontSize: 'var(--type-h4-size)', fontWeight: 600 }}>
              Pekko Topology <span style={{ fontWeight: 400, color: 'var(--ink-tertiary)', fontSize: 'var(--type-body-sm-size)' }}>({data.topology.length})</span>
            </h2>
            {data.topology.length === 0 ? (
              <p style={{ margin: 0, color: 'var(--ink-tertiary)', fontSize: 'var(--type-body-sm-size)' }}>No cluster members</p>
            ) : (
              <table aria-label="Pekko cluster topology" style={tableStyle}>
                <thead>
                  <tr>
                    {['Address', 'Status', 'Reachable', 'Pekko Roles'].map(h => (
                      <th key={h} style={thStyle}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.topology.map(member => (
                    <TopologyRow key={member.address} member={member} />
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </>
      )}
        </>
      )}
    </Layout>
  )
}

function RecorderRow({ topic, rec, onPause, onResume, isPending }: {
  topic: string
  rec: RecorderStatus
  onPause: () => void
  onResume: () => void
  isPending: boolean
}) {
  return (
    <tr style={trStyle}>
      <td style={tdStyle}>
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-mono-size)' }}>{topic}</span>
      </td>
      <td style={tdStyle}>
        <span className={`jx-badge ${rec.running ? 'jx-badge-success' : 'jx-badge-warn'}`}>
          {rec.running ? 'running' : 'paused'}
        </span>
      </td>
      <td style={{ ...tdStyle, fontVariantNumeric: 'tabular-nums', textAlign: 'right' }}>
        <span style={{ color: rec.consumerLag === 0 ? 'var(--signal-live)' : rec.consumerLag > 1000 ? 'var(--signal-error)' : 'var(--signal-warn)' }}>
          {rec.consumerLag.toLocaleString()}
        </span>
      </td>
      <td style={tdStyle}>
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)', color: 'var(--ink-secondary)' }}>
          {rec.assignedPartitions.length > 0 ? rec.assignedPartitions.join(', ') : '—'}
        </span>
      </td>
      <td style={tdStyle}>
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)' }}>{rec.protocol}</span>
      </td>
      <td style={{ ...tdStyle, color: 'var(--ink-tertiary)', fontSize: 'var(--type-caption-size)' }}>
        {timeAgo(rec.lastBatchAt)}
      </td>
      <td style={tdStyle}>
        {isPending ? (
          <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>…</span>
        ) : rec.running ? (
          <button onClick={onPause} style={actionBtnStyle}>■ Pause</button>
        ) : (
          <button onClick={onResume} style={{ ...actionBtnStyle, color: 'var(--signal-live)' }}>▶ Resume</button>
        )}
      </td>
    </tr>
  )
}

function InstanceRow({ inst }: { inst: InstanceRecord }) {
  const topics = Object.keys(inst.kafkaAssignments ?? {})
  return (
    <tr style={trStyle}>
      <td style={tdStyle}>
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-mono-size)' }}>{inst.instanceId}</span>
      </td>
      <td style={tdStyle}>
        <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
          <span className="jx-badge jx-badge-success">replay</span>
          {inst.recordingEnabled && <span className="jx-badge jx-badge-success">recording</span>}
          {inst.compactionEnabled && <span className="jx-badge jx-badge-info">compaction</span>}
        </div>
      </td>
      <td style={{ ...tdStyle, fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)' }}>{inst.catalogBackend}</td>
      <td style={tdStyle}>
        <span className={`jx-badge ${inst.status === 'alive' ? 'jx-badge-success' : 'jx-badge-warn'}`}>
          {inst.status}
        </span>
      </td>
      <td style={{ ...tdStyle, color: 'var(--ink-tertiary)', fontSize: 'var(--type-caption-size)' }}>
        {timeAgo(inst.lastHeartbeat)}
      </td>
      <td style={tdStyle}>
        {topics.length > 0 ? (
          <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
            {topics.map(t => (
              <span key={t} style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)', background: 'var(--surface-sunken)', padding: '1px 6px', borderRadius: 'var(--radius-xs)' }}>
                {t}
              </span>
            ))}
          </div>
        ) : <span style={{ color: 'var(--ink-tertiary)' }}>—</span>}
      </td>
    </tr>
  )
}

function TopologyRow({ member }: { member: MemberView }) {
  const tone = pekkoStatusTone(member.status)
  return (
    <tr style={trStyle}>
      <td style={tdStyle}>
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-mono-size)' }}>{member.address}</span>
      </td>
      <td style={tdStyle}>
        <span style={{ ...badgePill, ...tone }}>{member.status}</span>
      </td>
      <td style={tdStyle}>
        {member.reachable
          ? <span style={{ color: 'var(--signal-live)' }}>✓</span>
          : <span style={{ color: 'var(--signal-error)' }}>✗</span>}
      </td>
      <td style={tdStyle}>
        {member.roles.length > 0 ? (
          <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
            {member.roles.map(r => (
              <span key={r} style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-caption-size)', background: 'var(--surface-sunken)', padding: '1px 6px', borderRadius: 'var(--radius-xs)' }}>
                {r}
              </span>
            ))}
          </div>
        ) : <span style={{ color: 'var(--ink-tertiary)', fontSize: 'var(--type-caption-size)' }}>—</span>}
      </td>
    </tr>
  )
}

const overline: React.CSSProperties = {
  fontSize: 'var(--type-micro-size)',
  fontWeight: 'var(--type-micro-weight)' as unknown as number,
  letterSpacing: 'var(--type-micro-tracking)',
  textTransform: 'uppercase',
  color: 'var(--ink-tertiary)',
}

const badgePill: React.CSSProperties = {
  padding: '2px 10px',
  borderRadius: 'var(--radius-full)',
  fontSize: 'var(--type-caption-size)',
  fontWeight: 600,
  display: 'inline-block',
}

const tableStyle: React.CSSProperties = {
  width: '100%',
  borderCollapse: 'collapse',
  fontSize: 'var(--type-body-sm-size)',
}

const thStyle: React.CSSProperties = {
  textAlign: 'left',
  padding: '6px 12px',
  fontSize: 'var(--type-micro-size)',
  fontWeight: 600,
  letterSpacing: 'var(--type-micro-tracking)',
  textTransform: 'uppercase',
  color: 'var(--ink-tertiary)',
  borderBottom: '1px solid var(--rule)',
  whiteSpace: 'nowrap',
}

const trStyle: React.CSSProperties = {
  borderBottom: '1px solid var(--rule)',
}

const tdStyle: React.CSSProperties = {
  padding: '8px 12px',
  verticalAlign: 'middle',
}

const actionBtnStyle: React.CSSProperties = {
  background: 'none',
  border: '1px solid var(--rule)',
  borderRadius: 'var(--radius-sm)',
  padding: '3px 10px',
  fontSize: 'var(--type-caption-size)',
  cursor: 'pointer',
  color: 'var(--ink-secondary)',
  fontFamily: 'var(--font-body)',
}


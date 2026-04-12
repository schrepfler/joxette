import { createFileRoute, Link } from '@tanstack/react-router'
import { useQuery } from '@tanstack/react-query'
import { configApi, type DeploymentConfig, type DomainSummary } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'

export const Route = createFileRoute('/settings/')({
  component: SettingsPage,
})

function SettingsPage() {
  const { data, isLoading, error } = useQuery({
    queryKey: ['config', 'runtime'],
    queryFn: configApi.getRuntime,
  })

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: '1.75rem' }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>Settings</h1>
        <span style={{ fontSize: 12, color: '#a0aec0' }}>effective runtime configuration</span>
      </div>

      {isLoading && <LoadingSpinner />}
      {error && <ErrorMessage message={(error as Error).message} />}

      {data && (
        <>
          {/* ── Section 1: Deployment config ─────────────────────────── */}
          <section style={{ marginBottom: '2rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: '0.875rem' }}>
              <h2 style={{ margin: 0, fontSize: 16, fontWeight: 600 }}>Deployment Configuration</h2>
              <span style={immutableBadge}>Immutable — requires restart to change</span>
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(340px, 1fr))', gap: 14 }}>
              <DeploymentSection deployment={data.deployment} />
            </div>
          </section>

          {/* ── Section 2: Live domain summary ────────────────────────── */}
          <section>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: '0.875rem' }}>
              <h2 style={{ margin: 0, fontSize: 16, fontWeight: 600 }}>Live Domain Config</h2>
              <span style={liveBadge}>Live — from config tables</span>
            </div>

            <DomainSection domain={data.domain} />
          </section>
        </>
      )}
    </Layout>
  )
}

// ── Deployment config cards ──────────────────────────────────────────────────

function DeploymentSection({ deployment: d }: { deployment: DeploymentConfig }) {
  return (
    <>
      <ConfigCard title="Kafka">
        <KV label="Bootstrap Servers" value={d.kafkaBootstrapServers} mono />
      </ConfigCard>

      <ConfigCard title="Catalog & Object Storage">
        <KV label="Catalog Path" value={d.catalogPath} mono />
        <KV label="Object Storage Path" value={d.objectStoragePath ?? '—'} mono />
      </ConfigCard>

      <ConfigCard title="S3 / Object Storage Auth">
        <KV label="Endpoint" value={d.s3Endpoint || 'AWS default credential chain'} mono />
        <KV label="Region" value={d.s3Region} mono />
      </ConfigCard>

      <ConfigCard title="Inline Buffering">
        <KV label="Flush Threshold" value={`${d.inlineThresholdMb} MB`} />
        <KV label="Flush Threshold (records)" value={d.inlineThresholdRecords.toLocaleString()} />
      </ConfigCard>

      <ConfigCard title="Recording">
        <KV label="Batch Size" value={d.recordingBatchSize.toLocaleString()} />
        <KV label="Batch Timeout" value={`${d.recordingBatchTimeoutMs} ms`} />
      </ConfigCard>

      <ConfigCard title="Schedules (cron)">
        <KV label="Compaction" value={d.compactionSchedule} mono />
        <KV label="Retention" value={d.retentionSchedule} mono />
      </ConfigCard>

      <ConfigCard title="Entity Compaction">
        <KV label="Min Files Per Bucket" value={String(d.compactionEntityMinFilesPerBucket)} />
        <KV label="Target File Size" value={`${d.compactionEntityTargetFileSizeMb} MB`} />
        <KV label="Lookback" value={`${d.compactionEntityLookbackDays} days`} />
      </ConfigCard>

      <ConfigCard title="General Compaction">
        <KV
          label="Enabled"
          value={d.compactionGeneralEnabled ? 'Yes' : 'No'}
          valueColor={d.compactionGeneralEnabled ? '#276749' : '#718096'}
        />
        {d.compactionGeneralEnabled && (
          <>
            <KV label="Min Files Per Partition" value={String(d.compactionGeneralMinFilesPerPartition)} />
            <KV label="Target File Size" value={`${d.compactionGeneralTargetFileSizeMb} MB`} />
          </>
        )}
      </ConfigCard>
    </>
  )
}

// ── Domain summary cards ─────────────────────────────────────────────────────

function DomainSection({ domain: d }: { domain: DomainSummary }) {
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(210px, 1fr))', gap: 14 }}>
      <DomainCountCard
        label="Topics"
        count={d.topicCount}
        description="registered for recording"
        linkTo="/topics"
        linkLabel="Manage topics →"
        accentColor="#667eea"
      />
      <DomainCountCard
        label="Entity Types"
        count={d.entityTypeCount}
        description="entity types configured"
        linkTo="/entities"
        linkLabel="Manage entities →"
        accentColor="#38b2ac"
      />
      <DomainCountCard
        label="Source Mappings"
        count={d.sourceMappingCount}
        description="topic-to-entity mappings total"
        accentColor="#ed8936"
      />
    </div>
  )
}

// ── Shared sub-components ────────────────────────────────────────────────────

function ConfigCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div style={card}>
      <div style={cardTitle}>{title}</div>
      <dl style={{ margin: 0 }}>{children}</dl>
    </div>
  )
}

function KV({
  label,
  value,
  mono,
  valueColor,
}: {
  label: string
  value: string
  mono?: boolean
  valueColor?: string
}) {
  return (
    <div style={kvRow}>
      <dt style={kvLabel}>{label}</dt>
      <dd style={{
        ...kvValue,
        color: valueColor ?? '#2d3748',
        fontFamily: mono ? 'monospace' : undefined,
      }}>
        {value}
      </dd>
    </div>
  )
}

function DomainCountCard({
  label,
  count,
  description,
  linkTo,
  linkLabel,
  accentColor,
}: {
  label: string
  count: number
  description: string
  linkTo?: string
  linkLabel?: string
  accentColor: string
}) {
  return (
    <div style={card}>
      <div style={{ fontSize: 11, fontWeight: 700, color: '#718096', textTransform: 'uppercase' as const, letterSpacing: '0.06em', marginBottom: 4 }}>
        {label}
      </div>
      <div style={{ fontSize: 36, fontWeight: 700, color: '#2d3748', lineHeight: 1.1, marginBottom: 2 }}>
        {count}
      </div>
      <div style={{ fontSize: 12, color: '#a0aec0', marginBottom: linkTo ? 10 : 0 }}>
        {description}
      </div>
      {linkTo && linkLabel && (
        <Link
          to={linkTo}
          style={{ fontSize: 12, color: accentColor, textDecoration: 'none', fontWeight: 600 }}
        >
          {linkLabel}
        </Link>
      )}
      <div style={{ height: 3, background: accentColor, borderRadius: 2, marginTop: 10, opacity: 0.5 }} />
    </div>
  )
}

// ── Styles ───────────────────────────────────────────────────────────────────

const card: React.CSSProperties = {
  background: '#fff',
  border: '1px solid #e2e8f0',
  borderRadius: 8,
  padding: '0.875rem 1.125rem',
}

const cardTitle: React.CSSProperties = {
  fontSize: 12,
  fontWeight: 700,
  color: '#4a5568',
  textTransform: 'uppercase',
  letterSpacing: '0.06em',
  marginBottom: '0.6rem',
}

const kvRow: React.CSSProperties = {
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'baseline',
  padding: '4px 0',
  borderBottom: '1px solid #f7fafc',
  gap: 12,
}

const kvLabel: React.CSSProperties = {
  fontSize: 12,
  color: '#718096',
  flexShrink: 0,
}

const kvValue: React.CSSProperties = {
  margin: 0,
  fontSize: 13,
  fontWeight: 500,
  wordBreak: 'break-all',
  textAlign: 'right',
}

const immutableBadge: React.CSSProperties = {
  background: '#fefcbf',
  color: '#744210',
  padding: '2px 8px',
  borderRadius: 8,
  fontSize: 11,
  fontWeight: 600,
}

const liveBadge: React.CSSProperties = {
  background: '#c6f6d5',
  color: '#276749',
  padding: '2px 8px',
  borderRadius: 8,
  fontSize: 11,
  fontWeight: 600,
}

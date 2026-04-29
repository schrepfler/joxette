import { createFileRoute, Link } from '@tanstack/react-router'
import { useQuery } from '@tanstack/react-query'
import { configApi, type DeploymentConfig, type DomainSummary } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { pageTitle, cardStyle } from '../../styles/shared'

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
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 24 }}>
        <h1 style={pageTitle}>Settings</h1>
        <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>effective runtime configuration</span>
      </div>

      {isLoading && <LoadingSpinner />}
      {error && <ErrorMessage message={(error as Error).message} />}

      {data && (
        <>
          <section style={{ marginBottom: 32 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 14 }}>
              <h2 style={sectionHeading}>Deployment Configuration</h2>
              <span className="jx-badge jx-badge-warn">Immutable — requires restart to change</span>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(340px, 1fr))', gap: 14 }}>
              <DeploymentSection deployment={data.deployment} />
            </div>
          </section>

          <section>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 14 }}>
              <h2 style={sectionHeading}>Live Domain Config</h2>
              <span className="jx-badge jx-badge-success">Live — from config tables</span>
            </div>
            <DomainSection domain={data.domain} />
          </section>
        </>
      )}
    </Layout>
  )
}

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
        <KV label="Enabled" value={d.compactionGeneralEnabled ? 'Yes' : 'No'} valueColor={d.compactionGeneralEnabled ? 'var(--signal-live)' : 'var(--ink-tertiary)'} />
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

function DomainSection({ domain: d }: { domain: DomainSummary }) {
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(210px, 1fr))', gap: 14 }}>
      <DomainCountCard label="Topics" count={d.topicCount} description="registered for recording" linkTo="/topics" linkLabel="Manage topics" />
      <DomainCountCard label="Entity Types" count={d.entityTypeCount} description="entity types configured" linkTo="/entities" linkLabel="Manage entities" />
      <DomainCountCard label="Source Mappings" count={d.sourceMappingCount} description="topic-to-entity mappings total" />
    </div>
  )
}

function ConfigCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div style={cardStyle}>
      <div style={cardTitle}>{title}</div>
      <dl style={{ margin: 0 }}>{children}</dl>
    </div>
  )
}

function KV({ label, value, mono, valueColor }: { label: string; value: string; mono?: boolean; valueColor?: string }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', padding: '5px 0', borderBottom: '1px solid var(--surface-sunken)', gap: 12 }}>
      <dt style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', flexShrink: 0 }}>{label}</dt>
      <dd style={{ margin: 0, fontSize: 'var(--type-body-sm-size)', fontWeight: 500, fontFamily: mono ? 'var(--font-mono)' : undefined, wordBreak: 'break-all', textAlign: 'right', color: valueColor ?? 'var(--ink-primary)' }}>
        {value}
      </dd>
    </div>
  )
}

function DomainCountCard({ label, count, description, linkTo, linkLabel }: {
  label: string
  count: number
  description: string
  linkTo?: string
  linkLabel?: string
}) {
  return (
    <div style={cardStyle}>
      <div style={cardTitle}>{label}</div>
      <div style={{ fontFamily: 'var(--font-mono)', fontSize: '2rem', fontWeight: 700, color: 'var(--ink-primary)', lineHeight: 1.1, marginBottom: 4, fontVariantNumeric: 'tabular-nums' }}>
        {count}
      </div>
      <div style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', marginBottom: linkTo ? 10 : 0 }}>
        {description}
      </div>
      {linkTo && linkLabel && (
        <Link to={linkTo} style={{ fontSize: 'var(--type-caption-size)', color: 'var(--accent)', textDecoration: 'none', fontWeight: 600 }}>
          {linkLabel} →
        </Link>
      )}
      <div style={{ height: 2, background: 'var(--accent)', borderRadius: 'var(--radius-full)', marginTop: 10, opacity: 0.35 }} />
    </div>
  )
}

const sectionHeading: React.CSSProperties = {
  margin: 0,
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-h4-size)',
  fontWeight: 600,
  color: 'var(--ink-primary)',
}

const cardTitle: React.CSSProperties = {
  fontSize: 'var(--type-micro-size)',
  fontWeight: 'var(--type-micro-weight)' as unknown as number,
  letterSpacing: 'var(--type-micro-tracking)',
  textTransform: 'uppercase',
  color: 'var(--ink-tertiary)',
  marginBottom: 10,
}

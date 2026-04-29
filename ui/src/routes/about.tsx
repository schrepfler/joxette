import { createFileRoute } from '@tanstack/react-router'
import { useQuery } from '@tanstack/react-query'
import { Layout } from '../components/Layout'
import { pageTitle, cardStyle, thStyle, tdStyle } from '../styles/shared'

export const Route = createFileRoute('/about')({
  component: AboutPage,
})

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8080'

const TECH_STACK = [
  { component: 'Language', technology: 'Java', version: '25 (preview)' },
  { component: 'Framework', technology: 'Spring Boot', version: '4.0.5' },
  { component: 'Build', technology: 'Maven', version: 'latest' },
  { component: 'Database', technology: 'DuckDB JDBC', version: '1.5.1.0' },
  { component: 'Storage format', technology: 'DuckLake extension', version: 'latest' },
  { component: 'Concurrency', technology: 'Jox channels', version: '1.1.2' },
  { component: 'Flows', technology: 'Jox flows + structured', version: '0.5.3' },
  { component: 'SQL DSL', technology: 'jOOQ', version: '3.21.1' },
  { component: 'Messaging', technology: 'Apache Kafka clients', version: 'Boot BOM' },
  { component: 'API docs', technology: 'SpringDoc OpenAPI', version: '3.0.2' },
  { component: 'Object storage', technology: 'AWS SDK v2 (S3)', version: '2.42.32' },
]

const QUICK_LINKS = [
  { label: 'Swagger UI', desc: 'Explore and test the REST API interactively.', href: `${API_BASE}/swagger-ui.html` },
  { label: 'Health', desc: 'Liveness, consumer lag, catalog size, and inlined data.', href: `${API_BASE}/health` },
  { label: 'Prometheus metrics', desc: 'Micrometer metrics in Prometheus exposition format.', href: `${API_BASE}/actuator/prometheus` },
]

const SURVIVES = [
  'All data flushed to Parquet files on object storage (S3 / GCS / Azure)',
  'Catalog snapshots previously exported to object storage via the Snapshots page',
  'Kafka topic offsets committed before the crash (replay from Kafka is possible for retained topics)',
]

const LOST = [
  'Inlined (buffered) data not yet flushed to Parquet — typically up to the inline threshold (default 4 MB / 50 000 records)',
  'DuckLake catalog metadata — which Parquet files exist, their schemas, and snapshot history',
  'Service configuration — topic configs, entity types, source mappings (stored in plain DuckDB tables)',
]

const CONCEPTS = [
  {
    title: 'Cassettes',
    body: 'A cassette is a replayable archive of Kafka messages stored in DuckLake. General cassettes record the raw topic stream in order; entity cassettes group messages from multiple topics by a business key (for example, all events for a single order ID).',
  },
  {
    title: 'DuckLake data inlining',
    body: 'Incoming messages are first buffered in the DuckDB catalog file (inlined) before being flushed to Parquet on object storage. This eliminates the small-files problem, cuts S3 PUT costs, and allows replay queries to read all tiers transparently in a single statement.',
  },
  {
    title: 'Entity routing',
    body: 'Entity routing extracts a business key (such as an order ID or user ID) from each Kafka message using a JSONPath expression and routes it into an entity cassette. One entity type can consume messages from multiple topics, giving you a unified replay view across your entire event graph for any individual entity.',
  },
]

function AboutPage() {
  const { data: infoData } = useQuery<Record<string, unknown>>({
    queryKey: ['actuator-info'],
    queryFn: () => fetch(`${API_BASE}/actuator/info`).then(r => (r.ok ? r.json() : Promise.resolve({}))),
    retry: false,
    staleTime: Infinity,
  })

  const buildVersion = (infoData?.build as Record<string, string> | undefined)?.version ?? '0.1.0-SNAPSHOT'

  return (
    <Layout>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 24, flexWrap: 'wrap' }}>
        <h1 style={pageTitle}>About Joxette</h1>
        <span className="jx-badge jx-badge-default" style={{ fontFamily: 'var(--font-mono)' }}>{buildVersion}</span>
      </div>
      <p style={{ margin: '0 0 32px', fontSize: 'var(--type-body-size)', color: 'var(--ink-secondary)', maxWidth: 680, lineHeight: 1.65 }}>
        Joxette is a Kafka topic cassette recorder. It captures Kafka streams into replayable
        archives (cassettes) stored in DuckLake-backed object storage, with optional grouping by
        business entity across multiple topics.
      </p>

      <section style={{ ...cardStyle, marginBottom: 24 }}>
        <h2 style={sectionTitle}>Tech stack</h2>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={thStyle}>Component</th>
              <th style={thStyle}>Technology</th>
              <th style={thStyle}>Version</th>
            </tr>
          </thead>
          <tbody>
            {TECH_STACK.map(({ component, technology, version }) => (
              <tr key={component}>
                <td style={{ ...tdStyle, fontWeight: 500 }}>{component}</td>
                <td style={tdStyle}>{technology}</td>
                <td style={{ ...tdStyle, fontFamily: 'var(--font-mono)', fontSize: 'var(--type-mono-size)' }}>{version}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section style={{ ...cardStyle, marginBottom: 24 }}>
        <h2 style={sectionTitle}>Key concepts</h2>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          {CONCEPTS.map(({ title, body }) => (
            <div
              key={title}
              style={{ display: 'flex', gap: 12, padding: '12px 0', borderBottom: '1px solid var(--rule)' }}
            >
              <div style={{ width: 3, borderRadius: 2, background: 'var(--accent)', flexShrink: 0 }} />
              <div>
                <div style={{ fontWeight: 600, fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-primary)', marginBottom: 4 }}>{title}</div>
                <div style={{ fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-secondary)', lineHeight: 1.65 }}>{body}</div>
              </div>
            </div>
          ))}
        </div>
      </section>

      <section style={{ ...cardStyle, marginBottom: 24 }}>
        <h2 style={sectionTitle}>Operations links</h2>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
          {QUICK_LINKS.map(({ label, desc, href }) => (
            <a
              key={label}
              href={href}
              target="_blank"
              rel="noopener noreferrer"
              style={{
                display: 'block',
                flex: '1 1 200px',
                padding: '12px 16px',
                background: 'var(--surface-raised)',
                border: '1px solid var(--rule)',
                borderRadius: 'var(--radius-sm)',
                textDecoration: 'none',
                transition: 'box-shadow var(--duration-quick), transform var(--duration-quick)',
              }}
              onMouseEnter={e => {
                ;(e.currentTarget as HTMLAnchorElement).style.boxShadow = 'var(--shadow-md)'
                ;(e.currentTarget as HTMLAnchorElement).style.transform = 'translateY(-1px)'
              }}
              onMouseLeave={e => {
                ;(e.currentTarget as HTMLAnchorElement).style.boxShadow = 'none'
                ;(e.currentTarget as HTMLAnchorElement).style.transform = 'none'
              }}
            >
              <div style={{ fontSize: 'var(--type-body-sm-size)', fontWeight: 700, color: 'var(--accent)', marginBottom: 4 }}>
                {label} ↗
              </div>
              <div style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-secondary)' }}>{desc}</div>
              <div style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', marginTop: 6, fontFamily: 'var(--font-mono)' }}>
                {href.replace(API_BASE, '')}
              </div>
            </a>
          ))}
        </div>
      </section>

      <section style={cardStyle}>
        <h2 style={sectionTitle}>Data durability</h2>
        <p style={{ margin: '0 0 16px', fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-secondary)' }}>
          The DuckDB catalog file is the single point of persistence for inlined data and service
          config. Understanding what survives a catalog loss helps you plan backups.
        </p>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
          <DurabilityList title="Survives catalog file loss" items={SURVIVES} tone="success" icon="✓" />
          <DurabilityList title="Lost if catalog file is deleted" items={LOST} tone="error" icon="✗" />
        </div>
        <p style={{ margin: '16px 0 0', fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
          Mitigation: export catalog snapshots to object storage regularly via the <strong>Snapshots</strong> page. Parquet data on object storage is always durable.
        </p>
      </section>
    </Layout>
  )
}

function DurabilityList({ title, items, tone, icon }: {
  title: string
  items: string[]
  tone: 'success' | 'error'
  icon: string
}) {
  const color = tone === 'success' ? 'var(--signal-live-ink)' : 'var(--signal-error-ink)'
  const bg = tone === 'success' ? '#dcfce7' : '#fee2e2'
  const border = tone === 'success' ? 'var(--signal-live)' : 'var(--signal-error)'
  return (
    <div style={{ flex: '1 1 300px', background: bg, border: `1px solid ${border}`, borderRadius: 'var(--radius-sm)', padding: '14px 16px' }}>
      <div style={{ fontWeight: 700, fontSize: 'var(--type-body-sm-size)', color, marginBottom: 10 }}>{title}</div>
      <ul style={{ margin: 0, padding: 0, listStyle: 'none', display: 'flex', flexDirection: 'column', gap: 7 }}>
        {items.map(item => (
          <li key={item} style={{ display: 'flex', gap: 8, fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-secondary)' }}>
            <span style={{ color, fontWeight: 700, flexShrink: 0 }}>{icon}</span>
            <span>{item}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}

const sectionTitle: React.CSSProperties = {
  margin: '0 0 14px',
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-h4-size)',
  fontWeight: 600,
  color: 'var(--ink-primary)',
}

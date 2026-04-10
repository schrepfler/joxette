import { createFileRoute } from '@tanstack/react-router'
import { useQuery } from '@tanstack/react-query'
import { Layout } from '../components/Layout'

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
  {
    label: 'Swagger UI',
    desc: 'Explore and test the REST API interactively.',
    href: `${API_BASE}/swagger-ui.html`,
    color: '#2b6cb0',
    bg: '#ebf8ff',
    border: '#bee3f8',
  },
  {
    label: 'Health',
    desc: 'Liveness, consumer lag, catalog size, and inlined data.',
    href: `${API_BASE}/health`,
    color: '#276749',
    bg: '#f0fff4',
    border: '#c6f6d5',
  },
  {
    label: 'Prometheus metrics',
    desc: 'Micrometer metrics in Prometheus exposition format.',
    href: `${API_BASE}/actuator/prometheus`,
    color: '#744210',
    bg: '#fffaf0',
    border: '#fbd38d',
  },
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

function AboutPage() {
  const { data: infoData } = useQuery<Record<string, unknown>>({
    queryKey: ['actuator-info'],
    queryFn: () =>
      fetch(`${API_BASE}/actuator/info`).then(r => (r.ok ? r.json() : Promise.resolve({}))),
    retry: false,
    staleTime: Infinity,
  })

  const buildVersion =
    (infoData?.build as Record<string, string> | undefined)?.version ?? '0.1.0-SNAPSHOT'

  return (
    <Layout>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: '1.75rem', flexWrap: 'wrap' }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>About Joxette</h1>
        <span style={versionBadge}>{buildVersion}</span>
      </div>
      <p style={{ margin: '0 0 2rem', fontSize: 14, color: '#4a5568', maxWidth: 680 }}>
        Joxette is a Kafka topic cassette recorder. It captures Kafka streams into replayable
        archives (cassettes) stored in DuckLake-backed object storage, with optional grouping by
        business entity across multiple topics.
      </p>

      {/* Tech stack */}
      <section style={sectionCard}>
        <h2 style={sectionTitle}>Tech stack</h2>
        <table style={tableStyle}>
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
                <td style={{ ...tdStyle, color: '#4a5568', fontWeight: 500 }}>{component}</td>
                <td style={tdStyle}>{technology}</td>
                <td style={{ ...tdStyle, fontFamily: 'monospace', fontSize: 12 }}>{version}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      {/* Key concepts */}
      <section style={sectionCard}>
        <h2 style={sectionTitle}>Key concepts</h2>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <ConceptCard
            title="Cassettes"
            color="#2b6cb0"
            bg="#ebf8ff"
            border="#bee3f8"
          >
            A cassette is a replayable archive of Kafka messages stored in DuckLake. General
            cassettes record the raw topic stream in order; entity cassettes group messages from
            multiple topics by a business key (for example, all events for a single order ID).
          </ConceptCard>
          <ConceptCard
            title="DuckLake data inlining"
            color="#276749"
            bg="#f0fff4"
            border="#c6f6d5"
          >
            Incoming messages are first buffered in the DuckDB catalog file (inlined) before being
            flushed to Parquet on object storage. This eliminates the small-files problem, cuts S3
            PUT costs, and allows replay queries to read all tiers transparently in a single
            statement — no extra steps required.
          </ConceptCard>
          <ConceptCard
            title="Entity routing"
            color="#553c9a"
            bg="#faf5ff"
            border="#d6bcfa"
          >
            Entity routing extracts a business key (such as an order ID or user ID) from each
            Kafka message using a JSONPath expression and routes it into an entity cassette. One
            entity type can consume messages from multiple topics, giving you a unified replay view
            across your entire event graph for any individual entity.
          </ConceptCard>
        </div>
      </section>

      {/* Quick links */}
      <section style={sectionCard}>
        <h2 style={sectionTitle}>Operations links</h2>
        <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap' }}>
          {QUICK_LINKS.map(({ label, desc, href, color, bg, border }) => (
            <a
              key={label}
              href={href}
              target="_blank"
              rel="noopener noreferrer"
              style={{
                display: 'block',
                flex: '1 1 200px',
                padding: '0.85rem 1.1rem',
                background: bg,
                border: `1px solid ${border}`,
                borderRadius: 8,
                textDecoration: 'none',
                transition: 'box-shadow 0.15s, transform 0.1s',
              }}
              onMouseEnter={e => {
                ;(e.currentTarget as HTMLAnchorElement).style.boxShadow =
                  '0 2px 10px rgba(0,0,0,0.10)'
                ;(e.currentTarget as HTMLAnchorElement).style.transform = 'translateY(-1px)'
              }}
              onMouseLeave={e => {
                ;(e.currentTarget as HTMLAnchorElement).style.boxShadow = 'none'
                ;(e.currentTarget as HTMLAnchorElement).style.transform = 'none'
              }}
            >
              <div style={{ fontSize: 14, fontWeight: 700, color, marginBottom: 4 }}>
                {label} ↗
              </div>
              <div style={{ fontSize: 12, color: '#4a5568' }}>{desc}</div>
              <div style={{ fontSize: 11, color: '#718096', marginTop: 6, fontFamily: 'monospace' }}>
                {href.replace(API_BASE, '')}
              </div>
            </a>
          ))}
        </div>
      </section>

      {/* Data durability */}
      <section style={sectionCard}>
        <h2 style={sectionTitle}>Data durability</h2>
        <p style={{ margin: '0 0 1rem', fontSize: 13, color: '#4a5568' }}>
          The DuckDB catalog file is the single point of persistence for inlined data and service
          config. Understanding what survives a catalog loss helps you plan backups.
        </p>
        <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap' }}>
          <DurabilityList
            title="Survives catalog file loss"
            color="#276749"
            bg="#f0fff4"
            border="#c6f6d5"
            items={SURVIVES}
            icon="✓"
          />
          <DurabilityList
            title="Lost if catalog file is deleted"
            color="#9b2c2c"
            bg="#fff5f5"
            border="#feb2b2"
            items={LOST}
            icon="✗"
          />
        </div>
        <p style={{ margin: '1rem 0 0', fontSize: 12, color: '#718096' }}>
          Mitigation: export catalog snapshots to object storage regularly via the{' '}
          <strong>Snapshots</strong> page. Parquet data on object storage is always durable.
        </p>
      </section>
    </Layout>
  )
}

// ---- Sub-components ----

function ConceptCard({
  title,
  children,
  color,
  bg,
  border,
}: {
  title: string
  children: React.ReactNode
  color: string
  bg: string
  border: string
}) {
  return (
    <div
      style={{
        background: bg,
        border: `1px solid ${border}`,
        borderRadius: 8,
        padding: '0.75rem 1rem',
        display: 'flex',
        gap: 12,
      }}
    >
      <div style={{ width: 4, borderRadius: 4, background: color, flexShrink: 0 }} />
      <div>
        <div style={{ fontWeight: 700, fontSize: 14, color, marginBottom: 4 }}>{title}</div>
        <div style={{ fontSize: 13, color: '#4a5568', lineHeight: 1.65 }}>{children}</div>
      </div>
    </div>
  )
}

function DurabilityList({
  title,
  items,
  color,
  bg,
  border,
  icon,
}: {
  title: string
  items: string[]
  color: string
  bg: string
  border: string
  icon: string
}) {
  return (
    <div
      style={{
        flex: '1 1 300px',
        background: bg,
        border: `1px solid ${border}`,
        borderRadius: 8,
        padding: '0.85rem 1.1rem',
      }}
    >
      <div style={{ fontWeight: 700, fontSize: 13, color, marginBottom: 10 }}>{title}</div>
      <ul style={{ margin: 0, padding: 0, listStyle: 'none', display: 'flex', flexDirection: 'column', gap: 7 }}>
        {items.map(item => (
          <li key={item} style={{ display: 'flex', gap: 8, fontSize: 13, color: '#4a5568' }}>
            <span style={{ color, fontWeight: 700, flexShrink: 0 }}>{icon}</span>
            <span>{item}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}

// ---- Styles ----

const versionBadge: React.CSSProperties = {
  background: '#edf2f7',
  color: '#4a5568',
  padding: '3px 10px',
  borderRadius: 12,
  fontSize: 12,
  fontFamily: 'monospace',
  fontWeight: 600,
}

const sectionCard: React.CSSProperties = {
  background: '#fff',
  border: '1px solid #e2e8f0',
  borderRadius: 8,
  padding: '1rem 1.25rem',
  marginBottom: '1.5rem',
}

const sectionTitle: React.CSSProperties = {
  margin: '0 0 0.85rem',
  fontSize: 15,
  fontWeight: 700,
  color: '#2d3748',
}

const tableStyle: React.CSSProperties = {
  width: '100%',
  borderCollapse: 'collapse',
  fontSize: 13,
}

const thStyle: React.CSSProperties = {
  textAlign: 'left',
  padding: '0.45rem 0.65rem',
  background: '#edf2f7',
  fontWeight: 600,
  color: '#4a5568',
  borderBottom: '1px solid #e2e8f0',
}

const tdStyle: React.CSSProperties = {
  padding: '0.45rem 0.65rem',
  borderBottom: '1px solid #e2e8f0',
  color: '#2d3748',
}

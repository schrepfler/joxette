import { createFileRoute, Link, useNavigate } from '@tanstack/react-router'
import { useQuery } from '@tanstack/react-query'
import React, { useState, useEffect } from 'react'
import { brokersApi, type PeekMessage } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'

export const Route = createFileRoute('/brokers/$brokerId_/topics/$topic')({
  component: TopicPeekPage,
})

function truncateValue(value: string, encoding: string): string {
  if (encoding === 'base64') return '(binary)'
  if (value.length > 80) return value.slice(0, 80) + '…'
  return value
}

function tryPrettyJson(value: string): string {
  try {
    return JSON.stringify(JSON.parse(value), null, 2)
  } catch {
    return value
  }
}

function TopicPeekPage() {
  const { brokerId, topic } = Route.useParams()
  const navigate = useNavigate()
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null)
  const [autoRefresh, setAutoRefresh] = useState(false)

  const { data: messages = [], isLoading, error, refetch } = useQuery({
    queryKey: ['brokers', brokerId, 'topics', topic, 'peek'],
    queryFn: () => brokersApi.peekMessages(brokerId, topic, 20),
    staleTime: 0,
  })

  useEffect(() => {
    if (!autoRefresh) return
    const id = setInterval(() => void refetch(), 10_000)
    return () => clearInterval(id)
  }, [autoRefresh, refetch])

  return (
    <Layout>
      {/* Header */}
      <div style={{ marginBottom: '1.5rem' }}>
        <div style={{ fontSize: 13, color: '#718096', marginBottom: 6 }}>
          <Link to="/brokers" style={{ color: '#3182ce', textDecoration: 'none' }}>Brokers</Link>
          {' › '}
          <Link to="/brokers/$brokerId" params={{ brokerId }} style={{ color: '#3182ce', textDecoration: 'none' }}>{brokerId}</Link>
          {' › Topics › '}
          <span style={{ fontFamily: 'monospace' }}>{topic}</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8 }}>
          <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700, fontFamily: 'monospace' }}>{topic}</h1>
          <div style={{ display: 'flex', gap: 8 }}>
            <button style={secondaryBtnStyle} onClick={() => void refetch()}>Refresh</button>
            <button
              style={autoRefresh ? warnBtnSmall : primaryBtnSmall}
              onClick={() => setAutoRefresh(v => !v)}
            >
              {autoRefresh ? 'Auto-refresh: On' : 'Auto-refresh: Off'}
            </button>
            <button
              style={primaryBtnStyle}
              onClick={() => void navigate({
                to: '/brokers/$brokerId/topics/$topic/playground',
                params: { brokerId, topic },
              })}
            >
              Test Matcher
            </button>
          </div>
        </div>
      </div>

      {/* Content */}
      {isLoading && <LoadingSpinner />}
      {error && <ErrorMessage message={(error as Error).message} />}

      {!isLoading && !error && (
        <div style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '1rem 1.25rem' }}>
          {messages.length === 0 ? (
            <p style={{ margin: 0, fontSize: 14, color: '#a0aec0', fontStyle: 'italic' }}>
              No recent messages on this topic. The topic may be idle or have no messages.
            </p>
          ) : (
            <table aria-label="Recent messages on this topic" style={tableStyle}>
              <thead>
                <tr>
                  <th style={thStyle}>Timestamp</th>
                  <th style={thStyle}>Partition</th>
                  <th style={thStyle}>Offset</th>
                  <th style={thStyle}>Key</th>
                  <th style={thStyle}>Value</th>
                  <th style={thStyle}>Encoding</th>
                </tr>
              </thead>
              <tbody>
                {messages.map((msg, idx) => (
                  <React.Fragment key={idx}>
                    <tr
                      onClick={() => setExpandedIdx(expandedIdx === idx ? null : idx)}
                      style={{ cursor: 'pointer' }}
                      onMouseEnter={e => (e.currentTarget.style.background = '#ebf8ff')}
                      onMouseLeave={e => (e.currentTarget.style.background = expandedIdx === idx ? '#f0f8ff' : '')}
                    >
                      <td style={tdStyle}>{msg.timestamp.slice(0, 19).replace('T', ' ')}</td>
                      <td style={tdStyle}>{msg.partition}</td>
                      <td style={tdStyle}>{msg.offset}</td>
                      <td style={{ ...tdStyle, fontFamily: 'monospace', fontSize: 12 }}>
                        {msg.key ?? <span style={{ color: '#a0aec0' }}>—</span>}
                      </td>
                      <td style={{ ...tdStyle, fontFamily: 'monospace', fontSize: 12, maxWidth: 360 }}>
                        {truncateValue(msg.value, msg.valueEncoding)}
                      </td>
                      <td style={tdStyle}>
                        <span style={{
                          background: msg.valueEncoding === 'base64' ? '#fed7d7' : '#edf2f7',
                          color: msg.valueEncoding === 'base64' ? '#9b2c2c' : '#4a5568',
                          padding: '2px 6px', borderRadius: 10, fontSize: 11, fontWeight: 600,
                        }}>
                          {msg.valueEncoding}
                        </span>
                      </td>
                    </tr>
                    {expandedIdx === idx && (
                      <tr>
                        <td colSpan={6} style={{ ...tdStyle, background: '#f7fafc', padding: '0.75rem 1rem' }}>
                          <ExpandedRow msg={msg} brokerId={brokerId} topic={topic} />
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </Layout>
  )
}

function ExpandedRow({ msg, brokerId, topic }: { msg: PeekMessage; brokerId: string; topic: string }) {
  const navigate = useNavigate()

  return (
    <div>
      {msg.valueEncoding === 'base64' && (
        <div style={{ background: '#fff5f5', border: '1px solid #feb2b2', borderRadius: 4, padding: '0.4rem 0.75rem', marginBottom: '0.75rem', fontSize: 13, color: '#9b2c2c' }}>
          Binary value — displayed as base64
        </div>
      )}

      <div style={{ marginBottom: '0.75rem' }}>
        <div style={{ fontSize: 12, fontWeight: 600, color: '#718096', marginBottom: 4 }}>Value</div>
        <pre style={{
          margin: 0,
          padding: '0.6rem 0.75rem',
          background: '#1a202c',
          color: '#e2e8f0',
          borderRadius: 6,
          fontSize: 12,
          overflowX: 'auto',
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-all',
        }}>
          {msg.valueEncoding === 'base64' ? msg.value : tryPrettyJson(msg.value)}
        </pre>
      </div>

      {msg.headers.length > 0 && (
        <div style={{ marginBottom: '0.75rem' }}>
          <div style={{ fontSize: 12, fontWeight: 600, color: '#718096', marginBottom: 4 }}>Headers</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            {msg.headers.map((h, i) => (
              <div key={i} style={{ fontSize: 12, fontFamily: 'monospace' }}>
                <span style={{ color: '#4a5568', fontWeight: 600 }}>{h.key}</span>
                <span style={{ color: '#718096' }}>: </span>
                <span style={{ color: '#1a202c' }}>{h.value}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {msg.headers.length === 0 && (
        <div style={{ marginBottom: '0.75rem', fontSize: 12, color: '#a0aec0', fontStyle: 'italic' }}>No headers</div>
      )}

      <button
        style={primaryBtnSmall}
        onClick={() => void navigate({
          to: '/brokers/$brokerId/topics/$topic/playground',
          params: { brokerId, topic },
          state: {
            key: msg.key,
            value: msg.value,
            valueEncoding: msg.valueEncoding,
            headers: msg.headers,
          } as never,
        })}
      >
        Test this message in playground
      </button>
    </div>
  )
}

const primaryBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const secondaryBtnStyle: React.CSSProperties = { padding: '0.35rem 0.8rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 13 }
const primaryBtnSmall: React.CSSProperties = { padding: '0.2rem 0.6rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const warnBtnSmall: React.CSSProperties = { padding: '0.2rem 0.6rem', background: '#dd6b20', color: '#fff', border: 'none', borderRadius: 3, cursor: 'pointer', fontSize: 12 }
const tableStyle: React.CSSProperties = { width: '100%', borderCollapse: 'collapse', background: '#fff', fontSize: 13 }
const thStyle: React.CSSProperties = { textAlign: 'left', padding: '0.5rem 0.6rem', background: '#edf2f7', fontWeight: 600, color: '#4a5568', borderBottom: '1px solid #e2e8f0' }
const tdStyle: React.CSSProperties = { padding: '0.45rem 0.6rem', borderBottom: '1px solid #e2e8f0' }

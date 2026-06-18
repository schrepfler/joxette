import { createFileRoute } from '@tanstack/react-router'
import { useMutation, useQuery } from '@tanstack/react-query'
import { useEffect, useMemo, useRef, useState } from 'react'
import { EditorView, keymap } from '@codemirror/view'
import { Compartment, EditorState } from '@codemirror/state'
import { defaultKeymap, history, historyKeymap } from '@codemirror/commands'
import { sql, type SQLConfig } from '@codemirror/lang-sql'
import { DuckDB, PostgreSQL } from '../../components/duckdb-dialect'
import { autocompletion, completionKeymap } from '@codemirror/autocomplete'
import { oneDark } from '@codemirror/theme-one-dark'
import { catalogApi, type CatalogQueryResponse } from '../../api/client'
import { Layout } from '../../components/Layout'
import { LoadingSpinner } from '../../components/LoadingSpinner'
import { ErrorMessage } from '../../components/ErrorMessage'
import { useToast } from '../../components/Toast'
import {
  pageTitle,
  sectionTitle,
  cardStyle,
  tableStyle,
  thStyle,
  tdStyle,
  primaryBtnStyle,
  overlineStyle,
} from '../../styles/shared'

export const Route = createFileRoute('/catalog/')({
  component: CatalogPage,
})

// ---- Example query generator ----

interface ExampleQuery { label: string; sql: string }

function exampleQueriesFor(schemaTable: string): ExampleQuery[] {
  // schemaTable is e.g. "lake.general_feed_betgenius_fixture_v1" or "main.entity_order"
  const parts = schemaTable.split('.')
  const tableName = parts[parts.length - 1]
  const fqt = schemaTable

  if (tableName.startsWith('general_')) {
    const topic = tableName.slice('general_'.length).replace(/_/g, '.')
    return [
      {
        label: 'Recent messages (decoded)',
        sql:
`SELECT recorded_at, kafka_partition, kafka_offset, kafka_timestamp,
       kafka_key,
       decode(kafka_value) AS value,
       message_type
FROM ${fqt}
ORDER BY recorded_at DESC
LIMIT 50`,
      },
      {
        label: 'Message count & time range',
        sql:
`SELECT COUNT(*) AS total_messages,
       MIN(kafka_timestamp) AS first_message,
       MAX(kafka_timestamp) AS last_message,
       MAX(kafka_timestamp) - MIN(kafka_timestamp) AS span
FROM ${fqt}`,
      },
      {
        label: 'Top-level JSON key distribution',
        sql:
`SELECT json_keys(try_cast(decode(kafka_value) AS JSON))[1] AS primary_key,
       COUNT(*) AS cnt
FROM ${fqt}
WHERE kafka_value IS NOT NULL
GROUP BY 1
ORDER BY cnt DESC
LIMIT 20`,
      },
      {
        label: 'Messages per hour',
        sql:
`SELECT DATE_TRUNC('hour', kafka_timestamp) AS hour,
       COUNT(*) AS messages
FROM ${fqt}
GROUP BY 1
ORDER BY 1 DESC
LIMIT 48`,
      },
      {
        label: 'Messages per partition',
        sql:
`SELECT kafka_partition, COUNT(*) AS messages,
       MIN(kafka_offset) AS min_offset,
       MAX(kafka_offset) AS max_offset
FROM ${fqt}
GROUP BY 1
ORDER BY 1`,
      },
      {
        label: `Inspect a specific field across messages (${topic})`,
        sql:
`SELECT recorded_at,
       try_cast(decode(kafka_value) AS JSON)->>'$.id'     AS id,
       try_cast(decode(kafka_value) AS JSON)->>'$.status' AS status
FROM ${fqt}
WHERE try_cast(decode(kafka_value) AS JSON)->>'$.status' IS NOT NULL
ORDER BY recorded_at DESC
LIMIT 50`,
      },
    ]
  }

  if (tableName.startsWith('entity_')) {
    const entityType = tableName.slice('entity_'.length)
    return [
      {
        label: 'Top entities by message count',
        sql:
`SELECT entity_id, COUNT(*) AS messages,
       MIN(kafka_timestamp) AS first_seen,
       MAX(kafka_timestamp) AS last_seen
FROM ${fqt}
GROUP BY entity_id
ORDER BY messages DESC
LIMIT 20`,
      },
      {
        label: 'Recent events (replace entity_id)',
        sql:
`SELECT recorded_at, entity_id, message_type, topic,
       kafka_offset, kafka_partition, kafka_timestamp,
       decode(kafka_value) AS value
FROM ${fqt}
WHERE entity_id = '<entity_id>'
ORDER BY kafka_timestamp ASC`,
      },
      {
        label: 'Message type breakdown',
        sql:
`SELECT message_type, COUNT(*) AS cnt
FROM ${fqt}
GROUP BY message_type
ORDER BY cnt DESC`,
      },
      {
        label: 'Events per topic',
        sql:
`SELECT topic, COUNT(*) AS messages
FROM ${fqt}
GROUP BY topic
ORDER BY messages DESC`,
      },
      {
        label: `Active ${entityType}s in last 24 hours`,
        sql:
`SELECT entity_id, COUNT(*) AS recent_messages,
       MAX(kafka_timestamp) AS last_event
FROM ${fqt}
WHERE kafka_timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY entity_id
ORDER BY last_event DESC
LIMIT 50`,
      },
      {
        label: 'Deduplicated event stream (replace entity_id)',
        sql:
`SELECT recorded_at, entity_id, message_type, topic,
       kafka_offset, kafka_partition, kafka_timestamp,
       decode(kafka_value) AS value
FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY topic, kafka_partition, kafka_offset
    ORDER BY recorded_at DESC
  ) AS rn
  FROM ${fqt}
  WHERE entity_id = '<entity_id>'
) t
WHERE rn = 1
ORDER BY kafka_timestamp ASC`,
      },
    ]
  }

  // Generic fallback for config/other tables
  return [
    {
      label: 'Preview table',
      sql: `SELECT * FROM ${fqt} LIMIT 50`,
    },
    {
      label: 'Row count',
      sql: `SELECT COUNT(*) AS total FROM ${fqt}`,
    },
  ]
}

// ---- Schema browser ----

interface TableRow {
  schema_name: string
  name: string
}

function parseTableRows(result: CatalogQueryResponse): TableRow[] {
  const schemaIdx = result.columns.findIndex(c => c.name === 'schema_name')
  const nameIdx = result.columns.findIndex(c => c.name === 'name')
  if (schemaIdx === -1 || nameIdx === -1) return []
  return result.rows.map(r => ({
    schema_name: String(r[schemaIdx] ?? ''),
    name: String(r[nameIdx] ?? ''),
  }))
}

function groupBySchema(rows: TableRow[]): Record<string, string[]> {
  const out: Record<string, string[]> = {}
  for (const { schema_name, name } of rows) {
    ;(out[schema_name] ??= []).push(name)
  }
  return Object.fromEntries(
    Object.entries(out).sort(([a], [b]) => {
      if (a === 'lake') return -1
      if (b === 'lake') return 1
      return a.localeCompare(b)
    }),
  )
}

interface SchemaBrowserProps {
  onTableClick: (schemaTable: string) => void
  selectedTable: string | null
  onTableSelect: (schemaTable: string) => void
}

function SchemaBrowser({ onTableClick, selectedTable, onTableSelect }: SchemaBrowserProps) {
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({})

  const { data, isLoading, error } = useQuery({
    queryKey: ['catalog', 'tables'],
    queryFn: catalogApi.tables,
    staleTime: 30_000,
  })

  const groups = useMemo(
    () => (data ? groupBySchema(parseTableRows(data)) : {}),
    [data],
  )

  if (isLoading) return <LoadingSpinner />
  if (error) return <div style={{ padding: 8, color: 'var(--signal-error)', fontSize: 'var(--type-body-sm-size)' }}>Failed to load tables</div>

  return (
    <div style={{ paddingTop: 4 }}>
      {Object.entries(groups).map(([schema, tables]) => {
        const isCollapsed = collapsed[schema] ?? false
        return (
          <div key={schema} style={{ marginBottom: 4 }}>
            <button
              onClick={() => setCollapsed(prev => ({ ...prev, [schema]: !isCollapsed }))}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 6,
                width: '100%',
                background: 'none',
                border: 'none',
                cursor: 'pointer',
                padding: '4px 8px',
                borderRadius: 'var(--radius-xs)',
                fontFamily: 'var(--font-mono)',
                fontSize: 'var(--type-body-sm-size)',
                fontWeight: 600,
                color: 'var(--ink-secondary)',
                textAlign: 'left',
              }}
            >
              <span style={{ fontSize: '0.6rem', display: 'inline-block', transform: isCollapsed ? 'rotate(-90deg)' : 'rotate(0deg)', transition: 'transform 0.15s' }}>▼</span>
              {schema}
              <span style={{ marginLeft: 'auto', fontSize: 'var(--type-micro-size)', color: 'var(--ink-tertiary)', fontFamily: 'var(--font-body)' }}>{tables.length}</span>
            </button>
            {!isCollapsed && (
              <ul role="list" style={{ listStyle: 'none', margin: 0, padding: '0 0 0 8px' }}>
                {tables.map(t => (
                  <li key={t}>
                    <button
                      onClick={() => onTableSelect(`${schema}.${t}`)}
                      onDoubleClick={() => onTableClick(`${schema}.${t}`)}
                      title={`Insert ${schema}.${t}`}
                      style={{
                        display: 'block',
                        width: '100%',
                        background: selectedTable === `${schema}.${t}` ? 'var(--surface-raised)' : 'none',
                        border: 'none',
                        cursor: 'pointer',
                        padding: '3px 8px',
                        borderRadius: 'var(--radius-xs)',
                        fontFamily: 'var(--font-mono)',
                        fontSize: 'var(--type-caption-size, 0.75rem)',
                        color: selectedTable === `${schema}.${t}` ? 'var(--accent)' : 'var(--ink-primary)',
                        textAlign: 'left',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                      }}
                      onMouseEnter={e => { (e.currentTarget as HTMLButtonElement).style.background = 'var(--surface-raised)' }}
                      onMouseLeave={e => { (e.currentTarget as HTMLButtonElement).style.background = selectedTable === `${schema}.${t}` ? 'var(--surface-raised)' : 'none' }}
                    >
                      {t}
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </div>
        )
      })}
    </div>
  )
}

// ---- SQL Editor ----

interface SqlEditorProps {
  sqlConfig: SQLConfig
  onRun: (sql: string) => void
  editorViewRef: React.MutableRefObject<EditorView | null>
}

function SqlEditor({ sqlConfig, onRun, editorViewRef }: SqlEditorProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  // Compartment lets us hot-swap the SQL language extension (schema) without
  // touching the document or cursor position.
  const sqlCompartment = useRef(new Compartment())

  useEffect(() => {
    if (!containerRef.current) return

    const runCurrentSql = (view: EditorView) => {
      onRun(view.state.doc.toString())
      return true
    }

    const state = EditorState.create({
      doc: 'SELECT * FROM ',
      extensions: [
        history(),
        keymap.of([
          ...defaultKeymap,
          ...historyKeymap,
          ...completionKeymap,
          { key: 'Ctrl-Enter', run: runCurrentSql },
          { key: 'Mod-Enter', run: runCurrentSql },
        ]),
        autocompletion({ activateOnTyping: true }),
        sqlCompartment.current.of(sql(sqlConfig)),
        oneDark,
        EditorView.theme({
          '&': { borderRadius: 'var(--radius-sm)', overflow: 'hidden', fontSize: '13px' },
          '.cm-editor': { borderRadius: 'var(--radius-sm)' },
          '.cm-scroller': { minHeight: '140px', maxHeight: '280px', fontFamily: 'var(--font-mono)' },
        }),
        EditorView.lineWrapping,
      ],
    })

    const view = new EditorView({ state, parent: containerRef.current })
    editorViewRef.current = view
    return () => {
      view.destroy()
      editorViewRef.current = null
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Hot-swap the SQL language extension when schema data arrives (columns loaded).
  // Uses the compartment so document and cursor are preserved.
  useEffect(() => {
    const view = editorViewRef.current
    if (!view) return
    view.dispatch({
      effects: sqlCompartment.current.reconfigure(sql(sqlConfig)),
    })
  }, [sqlConfig]) // eslint-disable-line react-hooks/exhaustive-deps

  return <div ref={containerRef} style={{ border: '1px solid var(--rule)', borderRadius: 'var(--radius-sm)' }} />
}

// ---- Results table ----

function downloadCsv(result: CatalogQueryResponse) {
  const escape = (v: unknown) => {
    const s = v === null ? '' : String(v)
    return s.includes(',') || s.includes('"') || s.includes('\n')
      ? `"${s.replace(/"/g, '""')}"` : s
  }
  const lines = [
    result.columns.map(c => escape(c.name)).join(','),
    ...result.rows.map(r => r.map(escape).join(',')),
  ]
  const blob = new Blob([lines.join('\n')], { type: 'text/csv' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url; a.download = 'query-result.csv'; a.click()
  URL.revokeObjectURL(url)
}

function ResultsTable({ result }: { result: CatalogQueryResponse }) {
  if (!result.isQuery) {
    return (
      <div style={{ ...overlineStyle, padding: '12px 0', display: 'flex', gap: 16 }}>
        <span>{result.affectedRows} rows affected</span>
        <span>·</span>
        <span>{result.durationMs} ms</span>
      </div>
    )
  }

  return (
    <>
      {result.truncated && (
        <div style={{
          display: 'inline-flex',
          alignItems: 'center',
          gap: 6,
          background: 'color-mix(in oklab, var(--signal-warn) 12%, transparent)',
          border: '1px solid var(--signal-warn)',
          borderRadius: 'var(--radius-xs)',
          padding: '3px 10px',
          marginBottom: 8,
          fontSize: 'var(--type-caption-size, 0.75rem)',
          color: 'var(--signal-warn-ink)',
          fontWeight: 500,
        }}>
          First {result.rowCount.toLocaleString()} rows shown
        </div>
      )}

      <div style={{ overflowX: 'auto' }}>
        <table style={{ ...tableStyle, whiteSpace: 'nowrap' }}>
          <thead>
            <tr>
              {result.columns.map(col => (
                <th key={col.name} style={thStyle}>
                  {col.name}
                  <span style={{ ...overlineStyle, marginLeft: 4, textTransform: 'none', fontWeight: 400 }}>{col.typeName}</span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {result.rows.map((row, ri) => (
              <tr key={ri}>
                {row.map((cell, ci) => (
                  <td key={ci} style={{ ...tdStyle, fontFamily: 'var(--font-mono)', fontSize: 'var(--type-mono-size, 0.8125rem)' }}>
                    {cell === null ? <span style={{ color: 'var(--ink-tertiary)' }}>NULL</span> : String(cell)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div style={{ ...overlineStyle, marginTop: 8, display: 'flex', gap: 16, alignItems: 'center' }}>
        <span>{result.rowCount.toLocaleString()} rows</span>
        <span>·</span>
        <span>{result.durationMs} ms</span>
        <button
          onClick={() => downloadCsv(result)}
          style={{ marginLeft: 'auto', background: 'none', border: '1px solid var(--rule)', borderRadius: 'var(--radius-xs)', padding: '2px 10px', fontSize: 'var(--type-caption-size)', cursor: 'pointer', color: 'var(--ink-secondary)' }}
        >
          Download CSV
        </button>
      </div>
    </>
  )
}

// ---- Page ----

function CatalogPage() {
  const { addToast } = useToast()
  const editorViewRef = useRef<EditorView | null>(null)
  const [selectedTable, setSelectedTable] = useState<string | null>(null)

  const tablesQuery = useQuery({
    queryKey: ['catalog', 'tables'],
    queryFn: catalogApi.tables,
    staleTime: 30_000,
  })

  const columnsQuery = useQuery({
    queryKey: ['catalog', 'columns'],
    queryFn: catalogApi.columns,
    staleTime: 30_000,
  })

  const backendQuery = useQuery({
    queryKey: ['catalog', 'backend'],
    queryFn: catalogApi.backend,
    staleTime: Infinity, // backend type never changes at runtime
  })

  // Build SQLConfig: schema maps each "schema.table" to its column names so the
  // SQL language extension can offer column-level autocomplete after a dot.
  // Dialect is DuckDB for embedded/Quack, PostgreSQL for pg catalog.
  const sqlConfig = useMemo<SQLConfig>(() => {
    const dialect = backendQuery.data === 'POSTGRESQL' ? PostgreSQL : DuckDB

    const tableRows = tablesQuery.data ? parseTableRows(tablesQuery.data) : []
    const colRows = columnsQuery.data?.rows ?? []
    const schemaIdx = columnsQuery.data?.columns.findIndex(c => c.name === 'table_schema') ?? 0
    const tableIdx  = columnsQuery.data?.columns.findIndex(c => c.name === 'table_name')  ?? 1
    const colIdx    = columnsQuery.data?.columns.findIndex(c => c.name === 'column_name') ?? 2

    const schema: Record<string, string[]> = {}
    for (const row of tableRows) {
      schema[`${row.schema_name}.${row.name}`] = []
    }
    for (const row of colRows) {
      const key = `${row[schemaIdx]}.${row[tableIdx]}`
      ;(schema[key] ??= []).push(String(row[colIdx]))
    }
    return { dialect, schema }
  }, [tablesQuery.data, columnsQuery.data, backendQuery.data])

  const queryMutation = useMutation({
    mutationFn: (sqlText: string) => catalogApi.query(sqlText),
    onError: (e: Error) => addToast(e.message, 'error'),
  })

  function handleRun(sqlText: string) {
    const trimmed = sqlText.trim()
    if (!trimmed) return
    queryMutation.mutate(trimmed)
  }

  function handleTableClick(schemaTable: string) {
    const view = editorViewRef.current
    if (!view) return
    const { from, to } = view.state.selection.main
    view.dispatch({
      changes: { from, to, insert: schemaTable },
      selection: { anchor: from + schemaTable.length },
    })
    view.focus()
  }

  function handleExampleClick(sql: string) {
    const view = editorViewRef.current
    if (!view) return
    view.dispatch({
      changes: { from: 0, to: view.state.doc.length, insert: sql },
      selection: { anchor: sql.length },
    })
    view.focus()
  }

  const errorMessage = queryMutation.error
    ? (() => {
        const raw = (queryMutation.error as Error).message
        // try to extract RFC 7807 detail
        try {
          const json = JSON.parse(raw.replace(/^HTTP \d+ [^:]*:\s*/, ''))
          return (json as { detail?: string }).detail ?? raw
        } catch {
          return raw
        }
      })()
    : null

  return (
    <Layout>
      <h1 style={{ ...pageTitle, marginBottom: 20 }}>Catalog SQL Console</h1>

      <div style={{ display: 'flex', gap: 16, alignItems: 'flex-start', height: 'calc(100vh - 140px)', minHeight: 0 }}>
        {/* Left: schema browser + example queries */}
        <div style={{
          width: 240,
          flexShrink: 0,
          display: 'flex',
          flexDirection: 'column',
          gap: 12,
          height: '100%',
          minHeight: 0,
        }}>
          <div style={{
            ...cardStyle,
            padding: '12px 8px',
            overflowY: 'auto',
            flex: selectedTable ? '0 0 40%' : '1 1 auto',
            minHeight: 0,
          }}>
            <h2 style={{ ...sectionTitle, padding: '0 8px', marginBottom: 8 }}>Schema</h2>
            <SchemaBrowser
              onTableClick={handleTableClick}
              selectedTable={selectedTable}
              onTableSelect={setSelectedTable}
            />
          </div>

          {selectedTable && (
            <div style={{
              ...cardStyle,
              padding: '12px 8px',
              overflowY: 'auto',
              flex: '1 1 0',
              minHeight: 0,
            }}>
              <h2 style={{ ...sectionTitle, padding: '0 8px', marginBottom: 4 }}>
                Examples
              </h2>
              <p style={{ padding: '0 8px', margin: '0 0 6px', fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', fontFamily: 'var(--font-mono)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {selectedTable.split('.').pop()}
              </p>
              <ul role="list" style={{ listStyle: 'none', margin: 0, padding: 0 }}>
                {exampleQueriesFor(selectedTable).map(ex => (
                  <li key={ex.label}>
                    <button
                      onClick={() => handleExampleClick(ex.sql)}
                      title={ex.sql}
                      style={{
                        display: 'block',
                        width: '100%',
                        background: 'none',
                        border: 'none',
                        cursor: 'pointer',
                        padding: '5px 8px',
                        borderRadius: 'var(--radius-xs)',
                        fontSize: 'var(--type-caption-size, 0.75rem)',
                        color: 'var(--ink-primary)',
                        textAlign: 'left',
                        lineHeight: 1.4,
                      }}
                      onMouseEnter={e => { (e.currentTarget as HTMLButtonElement).style.background = 'var(--surface-raised)' }}
                      onMouseLeave={e => { (e.currentTarget as HTMLButtonElement).style.background = 'none' }}
                    >
                      {ex.label}
                    </button>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>

        {/* Right: editor + results */}
        <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', gap: 12, height: '100%', minHeight: 0, overflowY: 'auto' }}>
          <div style={cardStyle}>
            <SqlEditor
              sqlConfig={sqlConfig}
              onRun={handleRun}
              editorViewRef={editorViewRef}
            />
            <div style={{ marginTop: 10, display: 'flex', alignItems: 'center', gap: 10 }}>
              <button
                style={primaryBtnStyle}
                disabled={queryMutation.isPending}
                onClick={() => {
                  const view = editorViewRef.current
                  if (view) handleRun(view.state.doc.toString())
                }}
              >
                {queryMutation.isPending
                  ? <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                      <span className="jx-spin" style={{ display: 'inline-block', width: 12, height: 12, border: '2px solid var(--accent-ink)', borderTopColor: 'transparent', borderRadius: '50%' }} />
                      Running…
                    </span>
                  : 'Run (Ctrl+Enter)'}
              </button>
              <span style={{ ...overlineStyle, fontSize: '0.7rem' }}>Ctrl+Enter or Cmd+Enter also runs</span>
            </div>
          </div>

          <div style={cardStyle}>
            {queryMutation.isPending && <LoadingSpinner />}
            {errorMessage && <ErrorMessage message={errorMessage} />}
            {!queryMutation.isPending && queryMutation.data && (
              <ResultsTable result={queryMutation.data} />
            )}
            {!queryMutation.isPending && !queryMutation.data && !errorMessage && (
              <p style={{ color: 'var(--ink-tertiary)', fontSize: 'var(--type-body-sm-size)', margin: 0 }}>
                Run a query to see results.
              </p>
            )}
          </div>
        </div>
      </div>
    </Layout>
  )
}

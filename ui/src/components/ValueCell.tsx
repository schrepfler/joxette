import { useState } from 'react'
import { JsonView } from './JsonView'
import { tryParseValue } from '../lib/encoding'

/**
 * Renders a Kafka message value column cell.
 * - null/empty → em-dash placeholder
 * - non-JSON text → plain monospace string
 * - JSON (raw or base64-encoded) → collapsible JsonView with a decode badge
 */
export function ValueCell({ raw }: { raw: string | null }) {
  const [open, setOpen] = useState(false)
  if (!raw) return <span style={{ color: 'var(--ink-tertiary)' }}>—</span>
  const result = tryParseValue(raw)
  const isJson = result !== null
  const preview = raw.length > 80 ? raw.slice(0, 80) + '…' : raw

  if (!isJson) {
    return (
      <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--type-mono-size)', color: 'var(--ink-primary)' }}>
        {preview}
      </span>
    )
  }

  const decodedPreview = result.raw.length > 80 ? result.raw.slice(0, 80) + '…' : result.raw

  return (
    <div>
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        title={open ? 'Collapse' : 'Expand JSON'}
        style={{
          cursor: 'pointer',
          fontFamily: 'var(--font-mono)',
          fontSize: 'var(--type-mono-size)',
          color: 'var(--accent)',
          userSelect: 'none',
          background: 'none',
          border: 0,
          padding: 0,
          display: 'inline-flex',
          alignItems: 'baseline',
          gap: 6,
          textAlign: 'left',
        }}
      >
        <span
          aria-hidden
          style={{
            fontSize: 9,
            display: 'inline-block',
            transform: open ? 'rotate(90deg)' : 'rotate(0deg)',
            transition: 'transform var(--duration-quick) var(--ease-out-soft)',
          }}
        >▶</span>
        {open ? (result.raw !== raw ? 'JSON — base64 decoded' : 'JSON') : decodedPreview}
      </button>
      {open && <JsonView src={result.parsed as object} />}
    </div>
  )
}

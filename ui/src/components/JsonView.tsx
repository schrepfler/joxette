import ReactJson from '@microlink/react-json-view'

interface JsonViewProps {
  src: object
  /** Collapse depth — default 2 */
  collapsed?: number | boolean
}

// Custom base16 theme for a light background.
// Slot→role mapping discovered from the source:
//   base00 → background
//   base02 → object borders, datatype badge bg
//   base04 → objectSize count
//   base07 → OBJECT KEYS + braces  ← must be dark on light bg
//   base08 → NaN
//   base09 → STRINGS + ellipsis
//   base0A → null / regexp
//   base0B → floats
//   base0C → ARRAY indices
//   base0D → expand/collapse icons, dates, functions
//   base0E → booleans + collapse icon
//   base0F → INTEGERS + clipboard icon
const JOXETTE_THEME = {
  base00: '#f3f4f6', // background — surface-sunken
  base01: '#e5e7eb', // slightly elevated surface
  base02: '#d1d5db', // borders / badge backgrounds
  base03: '#9ca3af', // muted text — ink-tertiary
  base04: '#6b7280', // objectSize count — ink-secondary
  base05: '#374151', // default text
  base06: '#1f2937', // light foreground
  base07: '#111827', // KEYS + braces — ink-primary (dark, must contrast light bg)
  base08: '#dc2626', // NaN — signal-error
  base09: '#0369a1', // STRINGS — dark blue, readable on light
  base0A: '#6d28d9', // null / regexp — purple
  base0B: '#15803d', // floats — dark green
  base0C: '#6b7280', // array indices — muted ink
  base0D: '#166DF8', // icons, dates, functions — accent
  base0E: '#7c3aed', // booleans — violet
  base0F: '#0f766e', // INTEGERS — dark teal
}

export function JsonView({ src, collapsed = 2 }: JsonViewProps) {
  return (
    <div style={wrap}>
      <ReactJson
        src={src}
        name={null}
        collapsed={collapsed}
        indentWidth={2}
        displayDataTypes={false}
        displayObjectSize={false}
        enableClipboard
        theme={JOXETTE_THEME}
        style={style}
      />
    </div>
  )
}

const wrap: React.CSSProperties = {
  borderRadius: 'var(--radius-sm)',
  overflow: 'hidden',
  border: '1px solid var(--rule-strong)',
}

const style: React.CSSProperties = {
  fontFamily: 'var(--font-mono)',
  fontSize: 'var(--type-mono-size)',
  lineHeight: 1.5,
  letterSpacing: 'normal',
  wordSpacing: 'normal',
  fontFeatureSettings: 'normal',
  textRendering: 'auto',
  WebkitFontSmoothing: 'auto',
  padding: '8px 12px',
  background: '#f3f4f6', // surface-sunken literal — matches base00
}

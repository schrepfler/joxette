import ReactJson from '@microlink/react-json-view'

interface JsonViewProps {
  src: object
  /** Collapse depth — default 2 */
  collapsed?: number | boolean
}

// Custom base16 theme wired to Joxette design tokens (resolved to literals
// because react-json-view reads these values before CSS variables are
// evaluated — CSS vars cannot be used inside the theme object).
const JOXETTE_THEME = {
  base00: '#f3f4f6', // background — surface-sunken
  base01: '#e5e7eb', // slightly elevated surface
  base02: '#d1d5db', // selection bg / rule-strong
  base03: '#9ca3af', // comments, line numbers — ink-tertiary
  base04: '#6b7280', // dark foreground (unused in most views)
  base05: '#111827', // default text — near ink-primary
  base06: '#030712', // light foreground — ink-primary
  base07: '#ffffff', // lightest foreground
  base08: '#dc2626', // variables / error values — signal-error
  base09: '#d97706', // integers, constants — amber
  base0A: '#0369a1', // classes, bold — dark blue
  base0B: '#16a34a', // strings — signal-live
  base0C: '#0891b2', // support / regex — teal
  base0D: '#166DF8', // functions / keys — accent blue
  base0E: '#7c3aed', // keywords — violet
  base0F: '#b45309', // deprecated — brown
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

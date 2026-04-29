import ReactJson from '@microlink/react-json-view'

interface JsonViewProps {
  src: object
  /** Collapse depth — default 2 */
  collapsed?: number | boolean
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
        theme="flat"
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
  background: 'var(--surface-sunken)',
}

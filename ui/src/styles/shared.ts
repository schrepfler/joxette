export const pageTitle: React.CSSProperties = {
  margin: 0,
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-h2-size)',
  fontWeight: 'var(--type-h2-weight)',
  lineHeight: 'var(--type-h2-leading)',
  letterSpacing: 'var(--type-h2-tracking)',
  color: 'var(--ink-primary)',
}

export const sectionTitle: React.CSSProperties = {
  margin: '0 0 12px',
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-h4-size)',
  fontWeight: 'var(--type-h4-weight)',
  color: 'var(--ink-primary)',
}

export const cardStyle: React.CSSProperties = {
  background: 'var(--surface-paper)',
  border: '1px solid var(--rule)',
  borderRadius: 'var(--radius-md)',
  padding: '16px 20px',
  boxShadow: 'var(--shadow-sm)',
}

export const thStyle: React.CSSProperties = {
  textAlign: 'left' as const,
  padding: '10px 12px',
  background: 'var(--surface-raised)',
  fontSize: 'var(--type-micro-size)',
  fontWeight: 'var(--type-micro-weight)',
  letterSpacing: 'var(--type-micro-tracking)',
  textTransform: 'uppercase' as const,
  color: 'var(--ink-tertiary)',
  borderBottom: '1px solid var(--rule)',
}

export const tdStyle: React.CSSProperties = {
  padding: '10px 12px',
  borderBottom: '1px solid var(--rule)',
  color: 'var(--ink-primary)',
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-body-sm-size)',
}

export const tableStyle: React.CSSProperties = {
  width: '100%',
  borderCollapse: 'collapse' as const,
  background: 'var(--surface-paper)',
  borderRadius: 'var(--radius-md)',
  overflow: 'hidden',
  boxShadow: 'var(--shadow-sm)',
  border: '1px solid var(--rule)',
}

export const primaryBtnStyle: React.CSSProperties = {
  padding: '8px 16px',
  background: 'var(--accent)',
  color: 'var(--accent-ink)',
  border: '1px solid var(--accent)',
  borderRadius: 'var(--radius-sm)',
  cursor: 'pointer',
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-body-sm-size)',
  fontWeight: 500,
}

export const primaryBtnSmall: React.CSSProperties = {
  padding: '4px 10px',
  background: 'var(--accent)',
  color: 'var(--accent-ink)',
  border: '1px solid var(--accent)',
  borderRadius: 'var(--radius-xs)',
  cursor: 'pointer',
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-caption-size)',
  fontWeight: 500,
}

export const cancelBtnStyle: React.CSSProperties = {
  padding: '8px 16px',
  background: 'transparent',
  color: 'var(--ink-secondary)',
  border: '1px solid var(--rule-strong)',
  borderRadius: 'var(--radius-sm)',
  cursor: 'pointer',
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-body-sm-size)',
  fontWeight: 500,
}

export const dangerBtnSmall: React.CSSProperties = {
  padding: '4px 10px',
  background: 'transparent',
  color: 'var(--signal-error)',
  border: '1px solid color-mix(in oklab, var(--signal-error) 45%, transparent)',
  borderRadius: 'var(--radius-xs)',
  cursor: 'pointer',
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-caption-size)',
  fontWeight: 500,
}

export const warnBtnSmall: React.CSSProperties = {
  padding: '4px 10px',
  background: 'transparent',
  color: 'var(--signal-warn-ink)',
  border: '1px solid var(--signal-warn)',
  borderRadius: 'var(--radius-xs)',
  cursor: 'pointer',
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-caption-size)',
  fontWeight: 500,
}

export const labelStyle: React.CSSProperties = {
  display: 'block',
  marginBottom: 4,
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-micro-size)',
  fontWeight: 'var(--type-micro-weight)' as unknown as number,
  letterSpacing: 'var(--type-micro-tracking)',
  textTransform: 'uppercase' as const,
  color: 'var(--ink-secondary)',
}

export const modalH2: React.CSSProperties = {
  margin: '0 0 20px',
  fontFamily: 'var(--font-body)',
  fontSize: 'var(--type-h4-size)',
  fontWeight: 600,
  color: 'var(--ink-primary)',
}

export const overlineStyle: React.CSSProperties = {
  fontSize: 'var(--type-micro-size)',
  fontWeight: 'var(--type-micro-weight)' as unknown as number,
  letterSpacing: 'var(--type-micro-tracking)',
  textTransform: 'uppercase' as const,
  color: 'var(--ink-tertiary)',
}

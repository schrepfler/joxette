import { useEffect } from 'react'

export interface ViewMode<T extends string> {
  id: T
  label: string
  icon?: string
}

interface ViewModeBarProps<T extends string> {
  modes: ViewMode<T>[]
  active: T
  onChange: (mode: T) => void
}

/**
 * Segmented control for switching between visualisation modes.
 * Renders as a pill-group where exactly one mode is active.
 *
 * Usage:
 *   <ViewModeBar
 *     modes={[
 *       { id: 'table',    label: 'Table',    icon: '☰' },
 *       { id: 'timeline', label: 'Timeline', icon: '⏱' },
 *       { id: 'barcode',  label: 'Barcode',  icon: '▦' },
 *       { id: 'sunburst', label: 'Sunburst', icon: '◎' },
 *     ]}
 *     active={viewMode}
 *     onChange={setViewMode}
 *   />
 */
export function ViewModeBar<T extends string>({ modes, active, onChange }: ViewModeBarProps<T>) {
  // Inject hover style once
  useEffect(() => {
    if (document.getElementById('jx-viewmodebar-style')) return
    const el = document.createElement('style')
    el.id = 'jx-viewmodebar-style'
    el.textContent = `
.jx-viewmode-btn { transition: background var(--duration-quick), color var(--duration-quick), border-color var(--duration-quick); }
.jx-viewmode-btn:hover:not([data-active="true"]) { background: var(--ink-wash); color: var(--ink-primary); border-color: var(--rule-strong); }
`
    document.head.appendChild(el)
  }, [])

  return (
    <div
      style={{
        display: 'inline-flex',
        border: '1px solid var(--rule)',
        borderRadius: 'var(--radius-sm)',
        overflow: 'hidden',
        background: 'var(--surface-raised)',
      }}
    >
      {modes.map((m, i) => {
        const isActive = m.id === active
        return (
          <button
            key={m.id}
            data-active={isActive}
            className="jx-viewmode-btn"
            onClick={() => onChange(m.id)}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              gap: 5,
              padding: '5px 12px',
              fontFamily: 'var(--font-body)',
              fontSize: 'var(--type-body-sm-size)',
              fontWeight: isActive ? 600 : 400,
              color: isActive ? 'var(--accent-ink)' : 'var(--ink-secondary)',
              background: isActive ? 'var(--accent)' : 'transparent',
              border: 'none',
              borderLeft: i > 0 ? '1px solid var(--rule)' : 'none',
              cursor: 'pointer',
              whiteSpace: 'nowrap',
            }}
          >
            {m.icon && (
              <span style={{ fontSize: 12, lineHeight: 1 }}>{m.icon}</span>
            )}
            {m.label}
          </button>
        )
      })}
    </div>
  )
}

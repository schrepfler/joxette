/**
 * SolToolbar — shared toolbar for all SOL editor surfaces.
 *
 * Renders: label · recipes dropdown · spacer · [children] · hints · run button
 *
 * The `children` slot accepts mode-specific controls (e.g. scan-size selector
 * in the explorer, type-field input in the query panel) that are placed between
 * the spacer and the hints text.
 */

import { useState } from 'react'
import { SOL_RECIPES } from './sol-recipes'

interface Props {
  label?: string
  onRun: () => void
  isPending: boolean
  messageTypes?: string[]
  fieldPaths?: string[]
  onQueryChange?: (sol: string) => void
  /** Mode-specific controls rendered between spacer and hints */
  children?: React.ReactNode
}

export function SolToolbar({
  label = 'SOL Query',
  onRun,
  isPending,
  messageTypes = [],
  fieldPaths = [],
  onQueryChange,
  children,
}: Props) {
  const [recipesOpen, setRecipesOpen] = useState(false)

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
      <span style={{ fontSize: 'var(--type-body-sm-size)', fontWeight: 600, color: 'var(--ink-primary)', marginRight: 4 }}>
        {label}
      </span>

      {/* Recipes dropdown */}
      <div style={{ position: 'relative' }}>
        <button style={secondaryBtnSm} onClick={() => setRecipesOpen(o => !o)}>
          ☰ Recipes ▾
        </button>
        {recipesOpen && (
          <div
            style={{
              position: 'absolute', top: '100%', left: 0, zIndex: 50,
              background: 'var(--surface-paper)',
              border: '1px solid var(--rule)',
              borderRadius: 'var(--radius-sm)',
              boxShadow: 'var(--shadow-md)',
              minWidth: 380, marginTop: 4,
              maxHeight: 420, overflowY: 'auto',
            }}
          >
            <div style={{
              padding: '8px 14px 4px',
              fontSize: 'var(--type-micro-size)', fontWeight: 700,
              textTransform: 'uppercase', letterSpacing: 'var(--type-micro-tracking)',
              color: 'var(--ink-tertiary)',
            }}>
              Start with a recipe
            </div>
            {SOL_RECIPES.map(r => (
              <button
                key={r.label}
                onClick={() => { onQueryChange?.(r.sol); setRecipesOpen(false) }}
                style={{
                  display: 'block', width: '100%', textAlign: 'left',
                  padding: '8px 14px',
                  background: 'none', border: 'none', cursor: 'pointer',
                  fontFamily: 'var(--font-body)',
                }}
                onMouseEnter={e => (e.currentTarget.style.background = 'var(--ink-wash)')}
                onMouseLeave={e => (e.currentTarget.style.background = 'none')}
              >
                <div style={{ fontSize: 'var(--type-body-sm-size)', color: 'var(--ink-primary)' }}>{r.label}</div>
                <div style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)', marginTop: 2 }}>{r.description}</div>
              </button>
            ))}
          </div>
        )}
      </div>

      <div style={{ flex: 1 }} />

      {/* Mode-specific controls */}
      {children}

      {/* Hints */}
      {(messageTypes.length > 0 || fieldPaths.length > 0) && (
        <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>
          {messageTypes.length > 0 && `${messageTypes.length} event types`}
          {messageTypes.length > 0 && fieldPaths.length > 0 && ' · '}
          {fieldPaths.length > 0 && `${fieldPaths.length} fields`}
          {' · Ctrl+Space'}
        </span>
      )}
      <span style={{ fontSize: 'var(--type-caption-size)', color: 'var(--ink-tertiary)' }}>⌘↵ to run</span>

      {/* Run button */}
      <button
        style={{ ...primaryBtnSm, minWidth: 72 }}
        disabled={isPending}
        onClick={onRun}
      >
        {isPending ? 'Running…' : '▶ Run'}
      </button>
    </div>
  )
}

// ── Shared micro-styles (exported so panels don't duplicate them) ──────────────

export const primaryBtnSm: React.CSSProperties = {
  padding: '5px 12px',
  background: 'var(--accent)', color: 'var(--accent-ink)',
  border: '1px solid var(--accent)',
  borderRadius: 'var(--radius-sm)', cursor: 'pointer',
  fontFamily: 'var(--font-body)', fontSize: 'var(--type-body-sm-size)', fontWeight: 500,
}

export const secondaryBtnSm: React.CSSProperties = {
  padding: '5px 10px',
  background: 'transparent', color: 'var(--ink-secondary)',
  border: '1px solid var(--rule)',
  borderRadius: 'var(--radius-sm)', cursor: 'pointer',
  fontFamily: 'var(--font-body)', fontSize: 'var(--type-body-sm-size)',
}

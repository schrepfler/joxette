import { STEP_COLORS, useSequenceStore } from '../../stores/sequenceStore'

// ---------------------------------------------------------------------------
// SequenceModelPanel
//
// Renders at the bottom of the left panel.
// Color for step i = STEP_COLORS[i % STEP_COLORS.length] — same palette as
// the left-panel step badges and the right-panel anchor chips.
// ---------------------------------------------------------------------------

interface ActionIconProps {
  title: string
  children: React.ReactNode
  onClick?: () => void
}

function ActionIcon({ title, children, onClick }: ActionIconProps) {
  return (
    <button
      title={title}
      onClick={onClick}
      style={{
        background: 'none',
        border: 'none',
        padding: '2px 4px',
        cursor: 'pointer',
        color: 'var(--color-text-muted, #aaa)',
        fontSize: 13,
        lineHeight: 1,
        borderRadius: 3,
        display: 'inline-flex',
        alignItems: 'center',
      }}
    >
      {children}
    </button>
  )
}

interface ModelStepRowProps {
  label: string
  color: string
  reachRate: number
}

function ModelStepRow({ label, color, reachRate }: ModelStepRowProps) {
  const pct = Math.round(reachRate * 100)

  // Fade bar color toward grey as reach rate drops
  const barOpacity = 0.35 + reachRate * 0.65

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: 8,
        padding: '5px 0',
      }}
    >
      {/* Colored badge */}
      <span
        style={{
          display: 'inline-block',
          width: 10,
          height: 10,
          borderRadius: '50%',
          background: color,
          flexShrink: 0,
        }}
      />

      {/* Step label */}
      <span
        style={{
          fontSize: 12,
          color: 'var(--color-text, #eee)',
          flex: 1,
          minWidth: 0,
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}
        title={label}
      >
        {label}
      </span>

      {/* Percentage bar */}
      <div
        style={{
          width: 80,
          height: 6,
          borderRadius: 3,
          background: 'var(--color-surface-raised, #2a2a3a)',
          flexShrink: 0,
          overflow: 'hidden',
        }}
      >
        <div
          style={{
            width: `${pct}%`,
            height: '100%',
            borderRadius: 3,
            background: color,
            opacity: barOpacity,
            transition: 'width 300ms ease',
          }}
        />
      </div>

      {/* Percentage text */}
      <span
        style={{
          fontSize: 11,
          color: 'var(--color-text-muted, #aaa)',
          width: 36,
          textAlign: 'right',
          flexShrink: 0,
        }}
      >
        ~{pct}%
      </span>

      {/* Action icons */}
      <div style={{ display: 'flex', gap: 0, flexShrink: 0 }}>
        <ActionIcon title="Expand">⊞</ActionIcon>
        <ActionIcon title="Copy">□</ActionIcon>
        <ActionIcon title="Flag">⚑</ActionIcon>
      </div>
    </div>
  )
}

export function SequenceModelPanel() {
  const { query, results } = useSequenceStore()
  const steps = query.steps

  if (steps.length === 0) return null

  // reachRates from results, or [1.0, …, 1.0] as placeholder when no results yet
  const reachRates: number[] =
    results?.reachRates ?? steps.map(() => 1)

  return (
    <div
      style={{
        borderTop: '1px solid var(--color-border, #333)',
        marginTop: 12,
        paddingTop: 10,
      }}
    >
      {/* Section header */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: 6,
        }}
      >
        <span
          style={{
            fontSize: 11,
            fontWeight: 600,
            textTransform: 'uppercase',
            letterSpacing: '0.06em',
            color: 'var(--color-text-muted, #aaa)',
          }}
        >
          Sequence model
        </span>
        <div
          style={{
            display: 'flex',
            gap: 16,
            fontSize: 10,
            color: 'var(--color-text-muted, #666)',
          }}
        >
          <span>duration</span>
          <span>steps</span>
          <span>reached_outcome</span>
        </div>
      </div>

      {/* One row per step */}
      <div style={{ display: 'flex', flexDirection: 'column' }}>
        {steps.map((step, i) => {
          const color = STEP_COLORS[i % STEP_COLORS.length]
          const label = step.label ?? `Step ${i + 1}`
          const reachRate = reachRates[i] ?? 1

          return (
            <ModelStepRow
              key={step._id}
              label={label}
              color={color}
              reachRate={reachRate}
            />
          )
        })}
      </div>
    </div>
  )
}

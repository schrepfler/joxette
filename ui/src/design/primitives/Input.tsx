import { forwardRef, useId, type InputHTMLAttributes, type ReactNode, type SelectHTMLAttributes } from 'react'

/**
 * <Field> wrapper — consistent vertical rhythm for label + ruled input.
 * Labels are micro-scale uppercase, not the input's twin.
 */
function Field({
  label,
  hint,
  error,
  id,
  disabled,
  children,
  inlineLabel = false,
}: {
  label?: ReactNode
  hint?: ReactNode
  error?: ReactNode
  id: string
  disabled?: boolean
  children: ReactNode
  inlineLabel?: boolean
}) {
  return (
    <div
      style={{
        display: inlineLabel ? 'grid' : 'flex',
        flexDirection: 'column',
        gap: inlineLabel ? 0 : 6,
        minWidth: 0,
      }}
    >
      {label && (
        <label
          htmlFor={id}
          style={{
            fontFamily: 'var(--font-body)',
            fontSize: 'var(--type-micro-size)',
            letterSpacing: 'var(--type-micro-tracking)',
            textTransform: 'uppercase',
            fontWeight: 'var(--type-micro-weight)',
            color: disabled ? 'var(--ink-tertiary)' : 'var(--ink-secondary)',
            lineHeight: 1.2,
          }}
        >
          {label}
        </label>
      )}
      {children}
      {(hint || error) && (
        <div
          style={{
            fontFamily: 'var(--font-body)',
            fontSize: '0.75rem',
            color: error ? 'var(--signal-error)' : 'var(--ink-tertiary)',
            lineHeight: 1.4,
          }}
        >
          {error ?? hint}
        </div>
      )}
    </div>
  )
}

interface InputProps extends InputHTMLAttributes<HTMLInputElement> {
  label?: ReactNode
  hint?: ReactNode
  error?: ReactNode
  mono?: boolean
}

/**
 * <Input> — ruled underline, not a boxed control.
 *
 * A hairline on the bottom only; on focus, the hairline thickens and takes
 * the accent colour. The input itself never carries a shadow, radius, or
 * visible border. This is a deliberate departure from bordered-box inputs.
 */
export const Input = forwardRef<HTMLInputElement, InputProps>(function Input(
  { label, hint, error, mono, id, disabled, style, ...rest },
  ref,
) {
  const reactId = useId()
  const inputId = id ?? reactId
  return (
    <Field label={label} hint={hint} error={error} id={inputId} disabled={disabled}>
      <input
        ref={ref}
        id={inputId}
        disabled={disabled}
        {...rest}
        className={['jx-input', rest.className].filter(Boolean).join(' ')}
        style={{
          fontFamily: mono ? 'var(--font-mono)' : 'var(--font-body)',
          fontSize: mono ? 'var(--type-mono-size)' : 'var(--type-body-size)',
          fontVariantNumeric: mono ? 'tabular-nums' : undefined,
          color: 'var(--ink-primary)',
          background: 'transparent',
          border: 0,
          borderRadius: 0,
          padding: '8px 2px 8px',
          lineHeight: 1.4,
          width: '100%',
          minWidth: 0,
          outline: 'none',
          borderBottom: error ? '2px solid var(--signal-error)' : '1px solid var(--rule-strong)',
          transition: 'border-color var(--duration-quick) var(--ease-out-soft)',
          ...style,
        }}
      />
    </Field>
  )
})

interface SelectProps extends SelectHTMLAttributes<HTMLSelectElement> {
  label?: ReactNode
  hint?: ReactNode
  error?: ReactNode
  children: ReactNode
}

export const Select = forwardRef<HTMLSelectElement, SelectProps>(function Select(
  { label, hint, error, id, disabled, children, style, ...rest },
  ref,
) {
  const reactId = useId()
  const selectId = id ?? reactId
  return (
    <Field label={label} hint={hint} error={error} id={selectId} disabled={disabled}>
      <select
        ref={ref}
        id={selectId}
        disabled={disabled}
        {...rest}
        className={['jx-input', 'jx-select', rest.className].filter(Boolean).join(' ')}
        style={{
          fontFamily: 'var(--font-body)',
          fontSize: 'var(--type-body-size)',
          color: 'var(--ink-primary)',
          background: 'transparent',
          border: 0,
          borderRadius: 0,
          padding: '8px 24px 8px 2px',
          lineHeight: 1.4,
          width: '100%',
          minWidth: 0,
          outline: 'none',
          borderBottom: error ? '2px solid var(--signal-error)' : '1px solid var(--rule-strong)',
          transition: 'border-color var(--duration-quick) var(--ease-out-soft)',
          appearance: 'none',
          backgroundImage:
            "url(\"data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'><path fill='none' stroke='%235A5249' stroke-width='1.25' stroke-linecap='round' stroke-linejoin='round' d='M1 1l4 4 4-4'/></svg>\")",
          backgroundRepeat: 'no-repeat',
          backgroundPosition: 'right 6px center',
          ...style,
        }}
      >
        {children}
      </select>
    </Field>
  )
})

if (typeof document !== 'undefined' && !document.getElementById('jx-input-style')) {
  const el = document.createElement('style')
  el.id = 'jx-input-style'
  el.textContent = `
.jx-input:focus {
  border-bottom-color: var(--accent) !important;
  border-bottom-width: 2px !important;
}
.jx-input:disabled {
  color: var(--ink-tertiary);
  cursor: not-allowed;
  opacity: .7;
}
.jx-input::placeholder {
  color: var(--ink-tertiary);
  font-style: italic;
}
`
  document.head.appendChild(el)
}

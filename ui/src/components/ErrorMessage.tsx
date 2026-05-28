interface ErrorMessageProps {
  message: string
}

export function ErrorMessage({ message }: ErrorMessageProps) {
  return (
    <div
      role="alert"
      aria-live="assertive"
      style={{
        background: '#fee2e2',
        border: '1px solid var(--signal-error)',
        borderRadius: 'var(--radius-sm)',
        padding: '10px 14px',
        color: 'var(--signal-error-ink)',
        marginBottom: 16,
        fontFamily: 'var(--font-body)',
        fontSize: 'var(--type-body-sm-size)',
      }}
    >
      <strong>Error:</strong> {message}
    </div>
  )
}

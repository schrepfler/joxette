interface ErrorMessageProps {
  message: string
}

export function ErrorMessage({ message }: ErrorMessageProps) {
  return (
    <div
      style={{
        background: '#fff5f5',
        border: '1px solid #fc8181',
        borderRadius: 6,
        padding: '0.75rem 1rem',
        color: '#c53030',
        marginBottom: '1rem',
      }}
    >
      <strong>Error:</strong> {message}
    </div>
  )
}

export function LoadingSpinner() {
  return (
    <div role="status" aria-label="Loading" style={{ display: 'flex', justifyContent: 'center', padding: '48px 0' }}>
      <span
        aria-hidden="true"
        className="jx-spin"
        style={{
          display: 'inline-block',
          width: 24,
          height: 24,
          border: '2px solid var(--rule-strong)',
          borderTopColor: 'var(--accent)',
          borderRadius: '50%',
        }}
      />
    </div>
  )
}

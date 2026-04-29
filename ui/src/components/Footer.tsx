export default function Footer() {
  const year = new Date().getFullYear()

  return (
    <footer
      style={{
        marginTop: 80,
        borderTop: '1px solid var(--rule)',
        background: 'var(--surface-raised)',
        padding: '32px 24px',
      }}
    >
      <div
        className="page-wrap"
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 16,
          flexWrap: 'wrap',
        }}
      >
        <p
          style={{
            margin: 0,
            fontFamily: 'var(--font-body)',
            fontSize: 'var(--type-caption-size)',
            color: 'var(--ink-tertiary)',
          }}
        >
          &copy; {year} Joxette. Record once. Replay anything.
        </p>
        <p
          style={{
            margin: 0,
            fontFamily: 'var(--font-body)',
            fontSize: 'var(--type-micro-size)',
            fontWeight: 'var(--type-micro-weight)',
            letterSpacing: 'var(--type-micro-tracking)',
            textTransform: 'uppercase',
            color: 'var(--ink-tertiary)',
          }}
        >
          Kafka topic cassette recorder
        </p>
      </div>
    </footer>
  )
}

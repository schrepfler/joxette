import { Link } from '@tanstack/react-router'
import ThemeToggle from './ThemeToggle'

export default function Header() {
  return (
    <header
      style={{
        position: 'sticky',
        top: 0,
        zIndex: 50,
        borderBottom: '1px solid var(--rule)',
        background: 'var(--surface-paper)',
        padding: '0 24px',
      }}
    >
      <div
        className="page-wrap"
        style={{ display: 'flex', alignItems: 'center', gap: 12, height: 64 }}
      >
        <Link
          to="/"
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: 10,
            textDecoration: 'none',
          }}
        >
          <img
            src="/joxette logo.png"
            alt="Joxette"
            style={{ width: 28, height: 28, objectFit: 'contain' }}
          />
          <span
            style={{
              fontFamily: 'var(--font-display)',
              fontWeight: 800,
              fontSize: '0.9375rem',
              letterSpacing: '0.06em',
              textTransform: 'uppercase',
              color: 'var(--ink-primary)',
            }}
          >
            Joxette
          </span>
        </Link>

        <div style={{ marginLeft: 'auto' }}>
          <ThemeToggle />
        </div>
      </div>
    </header>
  )
}

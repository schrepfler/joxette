import { Link, useNavigate } from '@tanstack/react-router'
import { useHotkeySequences } from '@tanstack/react-hotkeys'

const NAV_LINKS = [
  { to: '/topics',     label: 'Topics',     shortcut: 'g t' },
  { to: '/entities',   label: 'Entities',   shortcut: 'g e' },
  { to: '/brokers',    label: 'Brokers',    shortcut: 'g b' },
  { to: '/compaction', label: 'Compaction', shortcut: 'g c' },
  { to: '/retention',  label: 'Retention',  shortcut: 'g r' },
  { to: '/health',     label: 'Health',     shortcut: 'g h' },
  { to: '/snapshots',  label: 'Snapshots',  shortcut: 'g s' },
  { to: '/settings',   label: 'Settings',   shortcut: 'g n' },
  { to: '/about',      label: 'About',      shortcut: 'g a' },
]

interface LayoutProps {
  children: React.ReactNode
}

export function Layout({ children }: LayoutProps) {
  const navigate = useNavigate()

  useHotkeySequences([
    { sequence: ['G', 'T'], callback: () => { void navigate({ to: '/topics' }) } },
    { sequence: ['G', 'E'], callback: () => { void navigate({ to: '/entities' }) } },
    { sequence: ['G', 'B'], callback: () => { void navigate({ to: '/brokers' }) } },
    { sequence: ['G', 'C'], callback: () => { void navigate({ to: '/compaction' }) } },
    { sequence: ['G', 'R'], callback: () => { void navigate({ to: '/retention' }) } },
    { sequence: ['G', 'H'], callback: () => { void navigate({ to: '/health' }) } },
    { sequence: ['G', 'S'], callback: () => { void navigate({ to: '/snapshots' }) } },
    { sequence: ['G', 'N'], callback: () => { void navigate({ to: '/settings' }) } },
    { sequence: ['G', 'A'], callback: () => { void navigate({ to: '/about' }) } },
  ])

  return (
    <div style={{ display: 'flex', minHeight: '100vh' }}>
      {/* Sidebar — always navy, regardless of theme */}
      <nav
        style={{
          width: 216,
          flexShrink: 0,
          background: 'var(--nav-bg)',
          color: 'var(--nav-text)',
          display: 'flex',
          flexDirection: 'column',
          position: 'sticky',
          top: 0,
          height: '100vh',
          overflowY: 'auto',
        }}
      >
        {/* Logo */}
        <Link
          to="/"
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            height: 88,
            padding: '0 16px',
            borderBottom: '1px solid var(--nav-border)',
            textDecoration: 'none',
          }}
        >
          <img
            src="/joxette logo.png"
            alt="Joxette"
            style={{ width: 72, height: 72, objectFit: 'contain', filter: 'brightness(0) invert(1)' }}
          />
        </Link>

        {/* Navigation links */}
        <ul style={{ listStyle: 'none', margin: 0, padding: '8px 0', flex: 1 }}>
          {NAV_LINKS.map(({ to, label, shortcut }) => (
            <li key={to}>
              <Link
                to={to}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  padding: '8px 20px',
                  color: 'var(--nav-text-muted)',
                  textDecoration: 'none',
                  fontFamily: 'var(--font-body)',
                  fontSize: 'var(--type-body-sm-size)',
                  fontWeight: 400,
                  borderLeft: '2px solid transparent',
                  transition: 'background var(--duration-quick), color var(--duration-quick), border-color var(--duration-quick)',
                }}
                activeProps={{
                  style: {
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    padding: '8px 20px',
                    color: 'var(--nav-text)',
                    textDecoration: 'none',
                    fontFamily: 'var(--font-body)',
                    fontSize: 'var(--type-body-sm-size)',
                    fontWeight: 500,
                    background: 'var(--nav-item-active)',
                    borderLeft: '2px solid var(--nav-accent)',
                    transition: 'background var(--duration-quick), color var(--duration-quick), border-color var(--duration-quick)',
                  },
                }}
              >
                <span>{label}</span>
                <kbd
                  style={{
                    fontFamily: 'var(--font-mono)',
                    fontSize: '0.625rem',
                    background: 'var(--nav-border)',
                    padding: '1px 5px',
                    borderRadius: 'var(--radius-xs)',
                    color: 'var(--nav-text-muted)',
                    letterSpacing: '0.02em',
                  }}
                >
                  {shortcut}
                </kbd>
              </Link>
            </li>
          ))}
        </ul>
      </nav>

      {/* Main content area */}
      <main
        style={{
          flex: 1,
          minWidth: 0,
          padding: '32px',
          background: 'var(--surface-paper)',
          color: 'var(--ink-primary)',
          overflowX: 'auto',
        }}
      >
        {children}
      </main>
    </div>
  )
}

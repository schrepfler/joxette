import { Link, useNavigate } from '@tanstack/react-router'
import { useHotkeySequences } from '@tanstack/react-hotkeys'

const NAV_LINKS = [
  { to: '/topics', label: 'Topics', shortcut: 'g t' },
  { to: '/entities', label: 'Entities', shortcut: 'g e' },
  { to: '/compaction', label: 'Compaction', shortcut: 'g c' },
  { to: '/health', label: 'Health', shortcut: 'g h' },
  { to: '/snapshots', label: 'Snapshots', shortcut: 'g s' },
  { to: '/about', label: 'About', shortcut: 'g a' },
]

interface LayoutProps {
  children: React.ReactNode
}

export function Layout({ children }: LayoutProps) {
  const navigate = useNavigate()

  useHotkeySequences([
    { sequence: ['G', 'T'], callback: () => { void navigate({ to: '/topics' }) } },
    { sequence: ['G', 'E'], callback: () => { void navigate({ to: '/entities' }) } },
    { sequence: ['G', 'C'], callback: () => { void navigate({ to: '/compaction' }) } },
    { sequence: ['G', 'H'], callback: () => { void navigate({ to: '/health' }) } },
    { sequence: ['G', 'S'], callback: () => { void navigate({ to: '/snapshots' }) } },
    { sequence: ['G', 'A'], callback: () => { void navigate({ to: '/about' }) } },
  ])

  return (
    <div style={{ display: 'flex', minHeight: '100vh', fontFamily: 'system-ui, sans-serif' }}>
      {/* Sidebar */}
      <nav
        style={{
          width: 200,
          background: '#1a202c',
          color: '#fff',
          display: 'flex',
          flexDirection: 'column',
          flexShrink: 0,
        }}
      >
        <div
          style={{
            padding: '1.25rem 1rem',
            borderBottom: '1px solid rgba(255,255,255,0.1)',
            fontWeight: 700,
            fontSize: 18,
            letterSpacing: 0.5,
          }}
        >
          Joxette
        </div>
        <ul style={{ listStyle: 'none', margin: 0, padding: '0.5rem 0' }}>
          {NAV_LINKS.map(({ to, label, shortcut }) => (
            <li key={to}>
              <Link
                to={to}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  padding: '0.6rem 1rem',
                  color: '#a0aec0',
                  textDecoration: 'none',
                  fontSize: 14,
                  transition: 'background 0.15s',
                }}
                activeProps={{ style: { color: '#fff', background: 'rgba(255,255,255,0.1)', display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0.6rem 1rem', textDecoration: 'none', fontSize: 14 } }}
              >
                <span>{label}</span>
                <kbd
                  style={{
                    fontSize: 10,
                    background: 'rgba(255,255,255,0.1)',
                    padding: '1px 4px',
                    borderRadius: 3,
                    color: '#718096',
                  }}
                >
                  {shortcut}
                </kbd>
              </Link>
            </li>
          ))}
        </ul>
      </nav>

      {/* Main content */}
      <main style={{ flex: 1, padding: '1.5rem', overflowX: 'auto', background: '#f7fafc' }}>
        {children}
      </main>
    </div>
  )
}

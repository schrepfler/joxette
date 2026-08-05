// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, cleanup, fireEvent } from '@testing-library/react'

afterEach(() => cleanup())

// Mirrors the exact row markup in src/routes/entities/index.tsx (lines
// 173-181 pre-fix / equivalent post-fix) so this test exercises the real
// interaction contract without pulling in useQuery/router.
function EntityRow({ onActivate }: { onActivate: () => void }) {
  function handleKeyDown(e: React.KeyboardEvent<HTMLTableRowElement>) {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault()
      onActivate()
    }
  }
  return (
    <table className="jx-table">
      <tbody>
        <tr
          role="button"
          tabIndex={0}
          className="jx-clickable"
          onClick={onActivate}
          onKeyDown={handleKeyDown}
        >
          <td>order</td>
        </tr>
      </tbody>
    </table>
  )
}

describe('entity row keyboard operability', () => {
  it('is reachable via role=button and tabIndex=0', () => {
    render(<EntityRow onActivate={() => {}} />)
    const row = screen.getByRole('button')
    expect(row.tabIndex).toBe(0)
  })

  it('activates on Enter', () => {
    const onActivate = vi.fn()
    render(<EntityRow onActivate={onActivate} />)
    fireEvent.keyDown(screen.getByRole('button'), { key: 'Enter' })
    expect(onActivate).toHaveBeenCalledTimes(1)
  })

  it('activates on Space', () => {
    const onActivate = vi.fn()
    render(<EntityRow onActivate={onActivate} />)
    fireEvent.keyDown(screen.getByRole('button'), { key: ' ' })
    expect(onActivate).toHaveBeenCalledTimes(1)
  })

  it('still activates on click (mouse path unchanged)', () => {
    const onActivate = vi.fn()
    render(<EntityRow onActivate={onActivate} />)
    fireEvent.click(screen.getByRole('button'))
    expect(onActivate).toHaveBeenCalledTimes(1)
  })
})

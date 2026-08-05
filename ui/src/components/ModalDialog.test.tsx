// @vitest-environment jsdom
import { describe, it, expect, afterEach } from 'vitest'
import { useState } from 'react'
import { render, screen, cleanup, fireEvent, act } from '@testing-library/react'
import { ModalDialog } from './ModalDialog'

afterEach(() => cleanup())

describe('ModalDialog', () => {
  it('exposes the dialog and its own content to the accessibility tree while open', () => {
    render(
      <div>
        <button>background action</button>
        <ModalDialog title="Test dialog" onClose={() => {}}>
          <button>confirm</button>
        </ModalDialog>
      </div>,
    )
    expect(screen.getByRole('dialog', { name: 'Test dialog' })).toBeTruthy()
    expect(screen.getByRole('button', { name: 'confirm' })).toBeTruthy()
  })

  it('hides background content from the accessibility tree while open, and marks it inert', () => {
    render(
      <div>
        <button>background action</button>
        <ModalDialog title="Test dialog" onClose={() => {}}>
          <button>confirm</button>
        </ModalDialog>
      </div>,
    )
    expect(screen.queryByRole('button', { name: 'background action' })).toBeNull()

    const portalRoot = document.getElementById('jx-modal-root')
    const backgroundContainer = Array.from(document.body.children).find(c => c !== portalRoot)!
    expect(backgroundContainer.hasAttribute('inert')).toBe(true)
    expect(backgroundContainer.getAttribute('aria-hidden')).toBe('true')
  })

  it('restores the background (removes inert/aria-hidden) once the dialog unmounts', () => {
    const { unmount } = render(
      <div>
        <button>background action</button>
        <ModalDialog title="Test dialog" onClose={() => {}}>
          <button>confirm</button>
        </ModalDialog>
      </div>,
    )
    unmount()
    expect(document.body.querySelector('[inert]')).toBeNull()
    expect(document.body.querySelector('[aria-hidden="true"]')).toBeNull()
  })

  it('focuses the first focusable element inside the dialog on mount', () => {
    render(
      <ModalDialog title="Test dialog" onClose={() => {}}>
        <input aria-label="first field" />
        <button>submit</button>
      </ModalDialog>,
    )
    expect(document.activeElement).toBe(screen.getByLabelText('first field'))
  })

  it('calls onClose on Escape', () => {
    let closed = false
    render(
      <ModalDialog title="Test dialog" onClose={() => { closed = true }}>
        <button>confirm</button>
      </ModalDialog>,
    )
    document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }))
    expect(closed).toBe(true)
  })

  it('does not yank focus back to the first field on an unrelated parent re-render with a changed onClose identity (Important 3)', () => {
    // Every real call site passes an inline `() => setShowX(false)` arrow as
    // onClose, which is a fresh function identity on every parent render —
    // exactly what this harness reproduces via `tick`.
    function Harness() {
      const [tick, setTick] = useState(0)
      return (
        <div>
          <button onClick={() => setTick(t => t + 1)}>trigger unrelated rerender</button>
          <span>tick: {tick}</span>
          <ModalDialog title="Test dialog" onClose={() => {}}>
            <input aria-label="first field" />
            <input aria-label="second field" />
          </ModalDialog>
        </div>
      )
    }

    render(<Harness />)
    const second = screen.getByLabelText('second field')
    second.focus()
    expect(document.activeElement).toBe(second)

    // The trigger button lives in the (now correctly) inert/aria-hidden
    // background, so it must be looked up with `hidden: true` to bypass the
    // accessibility-tree filter — it's still a real, clickable DOM node.
    const trigger = screen.getByRole('button', { name: 'trigger unrelated rerender', hidden: true })
    fireEvent.click(trigger)
    expect(screen.getByText('tick: 1')).toBeTruthy() // confirm the rerender actually happened
    expect(document.activeElement).toBe(second) // NOT yanked back to "first field"
  })

  it('restores focus to the pre-open active element after Escape closes the dialog (Important 4)', () => {
    function Harness() {
      const [show, setShow] = useState(false)
      return (
        <div>
          <button onClick={() => setShow(true)}>opener</button>
          {show && (
            <ModalDialog title="Test dialog" onClose={() => setShow(false)}>
              <button>confirm</button>
            </ModalDialog>
          )}
        </div>
      )
    }

    render(<Harness />)
    const opener = screen.getByRole('button', { name: 'opener' })
    opener.focus()
    fireEvent.click(opener)

    expect(screen.getByRole('dialog')).toBeTruthy()
    act(() => {
      document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }))
    })
    expect(screen.queryByRole('dialog')).toBeNull()
    expect(document.activeElement).toBe(opener)
  })

  it('restores focus to the pre-open active element after the dialog unmounts (non-Escape close path)', () => {
    function Harness() {
      const [show, setShow] = useState(false)
      return (
        <div>
          <button onClick={() => setShow(true)}>opener</button>
          {show && (
            <ModalDialog title="Test dialog" onClose={() => setShow(false)}>
              <button onClick={() => setShow(false)}>confirm</button>
            </ModalDialog>
          )}
        </div>
      )
    }

    render(<Harness />)
    const opener = screen.getByRole('button', { name: 'opener' })
    opener.focus()
    fireEvent.click(opener)

    fireEvent.click(screen.getByRole('button', { name: 'confirm' }))
    expect(screen.queryByRole('dialog')).toBeNull()
    expect(document.activeElement).toBe(opener)
  })
})

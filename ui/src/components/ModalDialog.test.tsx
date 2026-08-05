// @vitest-environment jsdom
import { describe, it, expect, afterEach } from 'vitest'
import { render, screen, cleanup } from '@testing-library/react'
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
})

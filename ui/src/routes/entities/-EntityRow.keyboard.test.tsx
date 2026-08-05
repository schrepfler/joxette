// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, cleanup, fireEvent, within } from '@testing-library/react'
import { EntityRow } from './index'

afterEach(() => cleanup())

// Renders the *real* EntityRow component (exported from ./index.tsx) inside
// a minimal <table>/<tbody> — the same structural context it runs in on the
// Entities page — instead of a hand-copied replica. The previous version of
// this test file duplicated the row markup by hand and omitted the actions
// column entirely, which is exactly why it didn't catch the Delete-button
// keyboard-trap regression fixed alongside this test.
function renderRow(onNavigate: () => void, onDelete: () => void) {
  return render(
    <table>
      <tbody>
        <EntityRow
          entityType="order"
          buckets={256}
          retentionDays={365}
          onNavigate={onNavigate}
          onDelete={onDelete}
        />
      </tbody>
    </table>,
  )
}

describe('EntityRow keyboard operability', () => {
  it('renders as a real table row — native `row` semantics are preserved (Important 2)', () => {
    renderRow(() => {}, () => {})
    // role="button" on the <tr> used to strip the whole table of its row
    // semantics; queryAllByRole('row') returned nothing. With no role
    // override on the <tr>, it must be queryable as a native row.
    expect(screen.getAllByRole('row')).toHaveLength(1)
    const row = screen.getByRole('row')
    expect(row.tagName).toBe('TR')
    expect(row.hasAttribute('role')).toBe(false)
    expect(row.hasAttribute('tabindex')).toBe(false)
  })

  it('the entity-type cell is its own focusable, native <button> — not the whole row', () => {
    renderRow(() => {}, () => {})
    const link = screen.getByRole('button', { name: 'order' })
    expect(link.tagName).toBe('BUTTON')
  })

  it('clicking the entity-type control navigates', () => {
    const onNavigate = vi.fn()
    renderRow(onNavigate, () => {})
    fireEvent.click(screen.getByRole('button', { name: 'order' }))
    expect(onNavigate).toHaveBeenCalledTimes(1)
  })

  it('Enter/Space on the entity-type control navigate (native <button> keyboard activation, unimpeded by any row-level handler)', () => {
    const onNavigate = vi.fn()
    renderRow(onNavigate, () => {})
    const link = screen.getByRole('button', { name: 'order' })
    link.focus()
    // jsdom does not simulate a real browser's native "Enter/Space on a
    // focused <button> synthesizes a click" behaviour, so the keydown alone
    // proves no code intercepts/prevents it (nothing here calls
    // preventDefault or stops propagation), and the follow-up click is what
    // a real browser's default action would fire — together they cover the
    // full contract: no interference + the actual navigation call.
    fireEvent.keyDown(link, { key: 'Enter', bubbles: true })
    fireEvent.click(link)
    expect(onNavigate).toHaveBeenCalledTimes(1)

    onNavigate.mockClear()
    fireEvent.keyDown(link, { key: ' ', bubbles: true })
    fireEvent.click(link)
    expect(onNavigate).toHaveBeenCalledTimes(1)
  })

  it('REGRESSION (Critical 1 / Important 2): Enter on the Delete button deletes — it does NOT bubble into a row-level handler and navigate', () => {
    const onNavigate = vi.fn()
    const onDelete = vi.fn()
    renderRow(onNavigate, onDelete)
    const deleteBtn = within(screen.getByRole('row')).getByRole('button', { name: 'Delete' })
    deleteBtn.focus()

    // Before this fix, the <tr> itself had role="button" + a keydown handler
    // that called preventDefault() and navigated on ANY bubbled Enter/Space
    // keydown — including one that originated on the nested Delete button.
    // A keyboard user tabbing to Delete and pressing Enter was navigated
    // away instead of deleting. Firing a bubbling keydown here reproduces
    // that exact path: if a row-level handler still existed, onNavigate
    // would fire. It must not.
    fireEvent.keyDown(deleteBtn, { key: 'Enter', bubbles: true })
    expect(onNavigate).not.toHaveBeenCalled()

    // The Delete button's own click (its real activation path, including a
    // real browser's native Enter-triggers-click for a focused <button>)
    // must still work, unimpeded.
    fireEvent.click(deleteBtn)
    expect(onDelete).toHaveBeenCalledTimes(1)
    expect(onNavigate).not.toHaveBeenCalled()
  })

  it('still activates navigation on click (mouse path unchanged)', () => {
    const onNavigate = vi.fn()
    renderRow(onNavigate, () => {})
    fireEvent.click(screen.getByRole('button', { name: 'order' }))
    expect(onNavigate).toHaveBeenCalledTimes(1)
  })
})

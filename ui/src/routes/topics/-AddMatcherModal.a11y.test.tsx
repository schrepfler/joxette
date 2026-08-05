// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, cleanup } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

vi.mock('../../api/client', () => ({
  topicsApi: { addMatcher: vi.fn() },
}))

// AddMatcherModal is not exported from $topic.tsx today; this plan does
// not change that (it stays a route-local component like AddEntityModal's
// sibling AddTopicModal was before Task 1). Import it directly by adding
// a named export — the only change to $topic.tsx's export surface this
// task requires — so it's testable in isolation like the other modals.

import { AddMatcherModal } from './$topic'

afterEach(() => cleanup())

describe('AddMatcherModal a11y (Task 6 — depends on Task 1 ModalDialog)', () => {
  it('exposes a dialog role with the matcher fields reachable, background hidden', () => {
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    render(<div><button>page background</button></div>)
    render(
      <QueryClientProvider client={qc}>
        <AddMatcherModal topic="orders.events" onClose={() => {}} />
      </QueryClientProvider>,
    )
    expect(screen.getByRole('dialog', { name: 'Add matcher' })).toBeTruthy()
    expect(screen.getByLabelText('Message type *')).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'page background' })).toBeNull()
  })
})

// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach } from 'vitest'
import type { ReactNode } from 'react'
import { render, screen, cleanup } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { AddTopicModal } from './AddTopicModal'
import { ConfirmDialog } from './ConfirmDialog'
import { TruncateDialog } from './TruncateDialog'

vi.mock('../api/client', () => ({
  topicsApi: { create: vi.fn() },
  brokersApi: { list: vi.fn().mockResolvedValue([]) },
}))

afterEach(() => cleanup())

function withQueryClient(ui: ReactNode) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return <QueryClientProvider client={qc}>{ui}</QueryClientProvider>
}

describe('CRUD modals expose their own dialog role and hide the background (Task 1 regression guard)', () => {
  it('AddTopicModal: dialog role is queryable, its "Topic *" field is reachable, background is hidden', () => {
    render(<div><button>page background</button></div>)
    render(withQueryClient(<AddTopicModal onClose={() => {}} />))
    expect(screen.getByRole('dialog', { name: 'Add Topic' })).toBeTruthy()
    expect(screen.getByLabelText('Topic *')).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'page background' })).toBeNull()
  })

  it('ConfirmDialog: dialog role is queryable, Confirm/Cancel are reachable, background is hidden', () => {
    render(<div><button>page background</button></div>)
    render(<ConfirmDialog message='Delete matcher "x"?' onConfirm={() => {}} onCancel={() => {}} />)
    expect(screen.getByRole('dialog', { name: 'Delete matcher "x"?' })).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Confirm' })).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'page background' })).toBeNull()
  })

  it('TruncateDialog: dialog role is queryable, the date field is reachable, background is hidden', () => {
    render(<div><button>page background</button></div>)
    render(<TruncateDialog label='topic "orders"' onConfirm={() => {}} onCancel={() => {}} />)
    expect(screen.getByRole('dialog', { name: 'Truncate topic "orders"' })).toBeTruthy()
    expect(screen.getByLabelText('Before')).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'page background' })).toBeNull()
  })
})

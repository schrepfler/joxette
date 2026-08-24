// @vitest-environment jsdom
import { describe, it, expect, vi, afterEach } from 'vitest'
import { render, screen, cleanup, fireEvent } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

vi.mock('../api/client', () => ({
  cassettesApi: {
    getTopicSummary: vi.fn(),
    getEntitySummary: vi.fn(),
  },
}))

import { cassettesApi } from '../api/client'
import { DatasetSummaryPanel } from './DatasetSummaryPanel'

afterEach(() => cleanup())

function renderPanel(overrides: Partial<React.ComponentProps<typeof DatasetSummaryPanel>> = {}) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const onSelect = overrides.onSelect ?? vi.fn()
  return {
    onSelect,
    ...render(
      <QueryClientProvider client={qc}>
        <DatasetSummaryPanel
          kind="topic"
          topic="orders.events"
          selected={null}
          onSelect={onSelect}
          {...overrides}
        />
      </QueryClientProvider>,
    ),
  }
}

describe('DatasetSummaryPanel', () => {
  it('renders dimension leaderboards from the summary response', async () => {
    vi.mocked(cassettesApi.getTopicSummary).mockResolvedValue({
      totalRecords: 100,
      from: null,
      to: null,
      dimensions: {
        partition: [{ value: '0', count: 60 }, { value: '1', count: 40 }],
        messageType: [{ value: 'OrderCreated', count: 100 }],
      },
    })

    renderPanel()

    expect(await screen.findByText('0')).toBeTruthy()
    expect(screen.getByText('60')).toBeTruthy()
    expect(screen.getByText('OrderCreated')).toBeTruthy()
  })

  it('calls onSelect with the clicked dimension and value', async () => {
    vi.mocked(cassettesApi.getTopicSummary).mockResolvedValue({
      totalRecords: 60,
      from: null,
      to: null,
      dimensions: {
        partition: [{ value: '0', count: 60 }],
        messageType: [],
      },
    })
    const { onSelect } = renderPanel()

    fireEvent.click(await screen.findByText('0'))

    expect(onSelect).toHaveBeenCalledWith('partition', '0')
  })

  it('shows an error message when the summary request fails', async () => {
    vi.mocked(cassettesApi.getTopicSummary).mockRejectedValue(new Error('boom'))

    renderPanel()

    expect(await screen.findByRole('alert')).toBeTruthy()
  })
})

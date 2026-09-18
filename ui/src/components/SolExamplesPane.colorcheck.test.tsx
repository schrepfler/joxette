// @vitest-environment jsdom
import { describe, it, expect, afterEach } from 'vitest'
import { render, screen, cleanup } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { buildTagColors, SOL_TAG_PALETTE } from './sol-colors'
import { SolExamplesPane } from './SolExamplesPane'
import type { SolSequenceExample } from '../api/client'

afterEach(() => cleanup())

function renderPane(examples: SolSequenceExample[], tagColors: ReturnType<typeof buildTagColors>) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={qc}>
      <SolExamplesPane
        entityType="fixture"
        examples={examples}
        totalSequences={1}
        matchedSequences={1}
        tagColors={tagColors}
      />
    </QueryClientProvider>,
  )
}

describe('SolExamplesPane colors', () => {
  it('applies the shared palette to matched tag segments (chip wash + header color)', () => {
    const example: SolSequenceExample = {
      entityId: '14220131',
      events: ['footballMatchDetails', 'noise', 'footballMatchSummary'],
      tags: {
        A: { from: 0, to: 1 },
        B: { from: 2, to: 3 },
      },
      matched: true,
      truncated: false,
    }
    const tagColors = buildTagColors(['A', 'B'])

    renderPane([example], tagColors)

    const aChip = screen.getByRole('button', { name: 'footballMatchDetails' })
    const bChip = screen.getByRole('button', { name: 'footballMatchSummary' })

    expect(aChip.style.background).toBe(asBg(SOL_TAG_PALETTE[0].wash))
    expect(bChip.style.background).toBe(asBg(SOL_TAG_PALETTE[1].wash))
  })
})

function asBg(value: string): string {
  const el = document.createElement('div')
  el.style.background = value
  return el.style.background
}

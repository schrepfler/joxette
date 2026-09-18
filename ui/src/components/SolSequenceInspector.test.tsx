// @vitest-environment jsdom
import { describe, it, expect, afterEach } from 'vitest'
import { render, screen, cleanup } from '@testing-library/react'
import { buildTagColors, SOL_NEUTRAL, SOL_TAG_PALETTE } from './sol-colors'
import { SolSequenceInspector } from './SolSequenceInspector'

afterEach(() => cleanup())

// jsdom normalises inline colour values (e.g. hex → rgb()) when reading them back
// via `.style.color`; round-trip the expected value through the same normalisation.
function asCssColor(value: string): string {
  const el = document.createElement('div')
  el.style.color = value
  return el.style.color
}

describe('SolSequenceInspector', () => {
  it('colours a named tag using the shared tagColors palette', () => {
    const tagColors = buildTagColors(['A', 'B'])
    render(
      <SolSequenceInspector
        tags={{ SEQ: { from: 0, to: 10 }, PREFIX: { from: 0, to: 2 }, A: { from: 2, to: 3 }, B: { from: 5, to: 6 } }}
        sequenceLength={10}
        tagColors={tagColors}
      />,
    )

    expect(screen.getByTitle('A').style.color).toBe(asCssColor(SOL_TAG_PALETTE[0].strong))
    expect(screen.getByTitle('B').style.color).toBe(asCssColor(SOL_TAG_PALETTE[1].strong))
  })

  it('colours implicit PREFIX/SUFFIX/SEQ with the shared neutral colour', () => {
    const tagColors = buildTagColors(['A'])
    render(
      <SolSequenceInspector
        tags={{ SEQ: { from: 0, to: 10 }, PREFIX: { from: 0, to: 2 }, SUFFIX: { from: 8, to: 10 }, A: { from: 2, to: 3 } }}
        sequenceLength={10}
        tagColors={tagColors}
      />,
    )

    expect(screen.getByTitle('SEQ').style.color).toBe(asCssColor(SOL_NEUTRAL.strong))
    expect(screen.getByTitle('PREFIX').style.color).toBe(asCssColor(SOL_NEUTRAL.strong))
    expect(screen.getByTitle('SUFFIX').style.color).toBe(asCssColor(SOL_NEUTRAL.strong))
  })

  it('keeps MATCHED on the distinguishing accent colour, not the shared palette', () => {
    const tagColors = buildTagColors(['A'])
    render(
      <SolSequenceInspector
        tags={{ SEQ: { from: 0, to: 10 }, MATCHED: { from: 2, to: 6 }, A: { from: 2, to: 3 } }}
        sequenceLength={10}
        tagColors={tagColors}
      />,
    )

    expect(screen.getByTitle('MATCHED').style.color).toBe('var(--accent)')
  })
})

// @vitest-environment jsdom
import { describe, it, expect } from 'vitest'
import { resolveThemeColors, colorForKey, PALETTE } from './CassetteTimeline'

function elWithVars(vars: Record<string, string>): HTMLElement {
  const el = document.createElement('div')
  for (const [k, v] of Object.entries(vars)) el.style.setProperty(k, v)
  document.body.appendChild(el)
  return el
}

describe('resolveThemeColors', () => {
  it('reads the 10 categorical chart-cat custom properties and the four surface/ink tokens from the given element', () => {
    const el = elWithVars({
      '--chart-cat-1': '#111111',
      '--chart-cat-2': '#222222',
      '--surface-sunken': '#f7fafc',
      '--rule-strong': '#cbd5e0',
      '--ink-tertiary': '#718096',
      '--ink-primary': '#1a202c',
    })
    const theme = resolveThemeColors(el)
    expect(theme.chartCat[0]).toBe('#111111')
    expect(theme.chartCat[1]).toBe('#222222')
    expect(theme.surfaceSunken).toBe('#f7fafc')
    expect(theme.ruleStrong).toBe('#cbd5e0')
    expect(theme.inkTertiary).toBe('#718096')
    expect(theme.inkPrimary).toBe('#1a202c')
  })
})

describe('colorForKey', () => {
  it('defaults to the module PALETTE when no palette argument is given (external-caller compatibility)', () => {
    expect(colorForKey('b', ['a', 'b', 'c'])).toBe(PALETTE[1])
  })

  it('uses a supplied palette (e.g. a resolved theme palette) when given one', () => {
    const custom = ['#aaa', '#bbb', '#ccc']
    expect(colorForKey('b', ['a', 'b', 'c'], custom)).toBe('#bbb')
  })

  it('falls back to the safety-net gray for a key not present in allKeys, regardless of palette', () => {
    expect(colorForKey('missing', ['a', 'b'])).toBe('#718096')
  })
})

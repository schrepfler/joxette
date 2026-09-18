import { describe, expect, it } from 'vitest'
import { detectContext, OPERATOR_COMPLETIONS } from './sol-language'

describe('detectContext', () => {
  it('wants only event types right after a tag binding paren', () => {
    expect(detectContext('match A(')).toBe('event-type')
  })

  it('wants only event types while typing a partial event type inside a tag binding', () => {
    expect(detectContext('match A(foot')).toBe('event-type')
  })

  it('wants only event types inside a later tag binding in the pattern', () => {
    expect(detectContext('match A(footballMatchDetails) >> * >> B(')).toBe('event-type')
  })

  it('still wants a fresh match-pattern term right after >>', () => {
    expect(detectContext('match A(footballMatchDetails) >> ')).toBe('match-pattern')
  })

  it('still wants tag-field completions after a dot', () => {
    expect(detectContext('A.')).toBe('tag-field')
  })

  it('still wants expression completions after if', () => {
    expect(detectContext('if ')).toBe('expression')
  })
})

describe('OPERATOR_COMPLETIONS', () => {
  const bySymbol = (label: string) => OPERATOR_COMPLETIONS.find(c => c.label === label)

  it('lists every pattern symbol from the SOL spec, each with a name and description', () => {
    for (const label of ['>>', '*', '+', '?', '|', '^', '{n}', '{n,}', '{,m}', '{n,m}']) {
      const completion = bySymbol(label)
      expect(completion, `expected an entry for "${label}"`).toBeDefined()
      expect(completion!.detail, `expected a name for "${label}"`).toBeTruthy()
      expect(completion!.info, `expected a description for "${label}"`).toBeTruthy()
    }
  })

  it('lists every relational/arithmetic symbol from the SOL spec, each with a name and description', () => {
    for (const label of ['=', '!=', '<', '>', '<=', '>=', '+', '-', '*', '/', '^', 'between', 'in']) {
      const completion = bySymbol(label)
      expect(completion, `expected an entry for "${label}"`).toBeDefined()
      expect(completion!.detail, `expected a name for "${label}"`).toBeTruthy()
    }
  })

  it('documents both meanings of a symbol that is overloaded between patterns and expressions', () => {
    const star = bySymbol('*')!
    expect(star.info).toMatch(/wildcard/i)
    expect(star.info).toMatch(/multiplication/i)
  })
})

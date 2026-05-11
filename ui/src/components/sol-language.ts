/**
 * CodeMirror 6 language support for SOL (Sequence Operations Language).
 *
 * Provides:
 *   - Syntax highlighting via a hand-rolled Lezer-style token stream
 *   - Autocomplete for keywords, functions, quantifiers, and event names
 */

import { StreamLanguage, LanguageSupport } from '@codemirror/language'
import {
  autocompletion,
  type CompletionContext,
  type CompletionResult,
  type Completion,
} from '@codemirror/autocomplete'

// ── Token definitions ──────────────────────────────────────────────────────────

const KEYWORDS   = ['match', 'split', 'filter', 'set', 'replace', 'combine', 'with', 'if', 'and', 'or', 'not', 'null', 'start', 'end']
const FUNCTIONS  = ['duration', 'length', 'sum', 'min', 'max', 'avg', 'any', 'all', 'unique', 'position', 'concat', 'lower', 'upper', 'strlen', 'coalesce', 'now', 'abs', 'round', 'floor', 'ceiling', 'log', 'date', 'time', 'datetime', 'datepart', 'time_bucket', 'edit_distance', 'regex_count', 'regex_substr', 'regex_replace']
const IMPLICIT_TAGS = ['MATCHED', 'PREFIX', 'SUFFIX', 'SEQ']
const OPERATORS  = ['>>', '*', '+', '?', '^', '|']

// ── Stream language (tokeniser) ────────────────────────────────────────────────

const solStreamLanguage = StreamLanguage.define<{ inString: boolean }>({
  name: 'sol',
  startState: () => ({ inString: false }),

  token(stream, state) {
    // String literals
    if (state.inString) {
      if (stream.skipTo("'")) { stream.next(); state.inString = false }
      else stream.skipToEnd()
      return 'string'
    }
    if (stream.peek() === "'") {
      stream.next(); state.inString = true
      if (stream.skipTo("'")) { stream.next(); state.inString = false }
      else stream.skipToEnd()
      return 'string'
    }

    // Comments
    if (stream.match('//')) { stream.skipToEnd(); return 'comment' }

    // Numbers (including duration literals like 5min, 1h, 30s)
    if (stream.match(/^-?\d+(\.\d+)?(ms|min|h|d|w|s)?/)) return 'number'

    // Operators
    if (stream.match('>>')) return 'operator'
    if (stream.match(/^[*+?^|]/)) return 'operator'
    if (stream.match(/^[<>!=]=?/)) return 'operator'
    if (stream.match(/^[-+*/^]/)) return 'operator'

    // Identifiers / keywords
    if (stream.match(/^[A-Za-z_][A-Za-z0-9_]*/)) {
      const word = stream.current()
      if (KEYWORDS.includes(word.toLowerCase()))    return 'keyword'
      if (FUNCTIONS.includes(word.toLowerCase()))   return 'builtin'
      if (IMPLICIT_TAGS.includes(word))             return 'typeName'
      // Capitalised identifiers are tag names by convention
      if (/^[A-Z]/.test(word))                      return 'typeName'
      return 'variableName'
    }

    // Punctuation
    if (stream.match(/^[()[\]{},.:@$]/)) return 'punctuation'

    stream.next()
    return null
  },

  languageData: {
    commentTokens: { line: '//' },
    closeBrackets: { brackets: ['(', '[', '{', "'"] },
  },
})

// ── Autocomplete ───────────────────────────────────────────────────────────────

const KEYWORD_COMPLETIONS: Completion[] = KEYWORDS.map(k => ({
  label: k, type: 'keyword',
}))

const FUNCTION_COMPLETIONS: Completion[] = FUNCTIONS.map(f => ({
  label: f + '()', type: 'function', apply: f + '(', detail: 'function',
}))

const TAG_COMPLETIONS: Completion[] = IMPLICIT_TAGS.map(t => ({
  label: t, type: 'constant', detail: 'implicit tag',
}))

const RECIPE_SNIPPETS: Completion[] = [
  { label: 'match A >> * >> B',           type: 'text', detail: 'funnel pattern',         apply: 'match A(event_a) >> * >> B(event_b)' },
  { label: 'match split Session()+',      type: 'text', detail: 'sessionize',             apply: 'match split Session()+\nif duration(Session[-1], SUFFIX[0]) > 30min' },
  { label: 'filter MATCHED',              type: 'keyword', detail: 'keep matched only' },
  { label: 'filter not MATCHED',          type: 'keyword', detail: 'keep unmatched only' },
  { label: 'duration(A, B)',              type: 'function', detail: 'time between tags',  apply: 'duration(A, B)' },
]

function solCompletions(eventNames: string[]) {
  const eventCompletions: Completion[] = eventNames.map(name => ({
    label: name, type: 'variable', detail: 'event type',
    boost: 1,
  }))

  return (ctx: CompletionContext): CompletionResult | null => {
    // Match any word/identifier being typed
    const word = ctx.matchBefore(/[\w.]+/)
    if (!word && !ctx.explicit) return null

    const from = word?.from ?? ctx.pos
    const text = word?.text ?? ''

    // After '>>' — prioritise event names
    const beforeCursor = ctx.state.doc.sliceString(0, ctx.pos)
    const afterArrow = />>[\s]*\w*$/.test(beforeCursor)

    const all = [
      ...(afterArrow ? eventCompletions : []),
      ...KEYWORD_COMPLETIONS,
      ...FUNCTION_COMPLETIONS,
      ...TAG_COMPLETIONS,
      ...RECIPE_SNIPPETS,
      ...(!afterArrow ? eventCompletions : []),
    ]

    const filtered = text
      ? all.filter(c => c.label.toLowerCase().startsWith(text.toLowerCase()))
      : all

    if (filtered.length === 0) return null

    return { from, options: filtered, validFor: /^[\w.]*$/ }
  }
}

// ── Public factory ─────────────────────────────────────────────────────────────

export function solLanguage(eventNames: string[] = []): LanguageSupport {
  return new LanguageSupport(solStreamLanguage, [
    autocompletion({
      override: [solCompletions(eventNames)],
      activateOnTyping: true,
      maxRenderedOptions: 20,
    }),
  ])
}

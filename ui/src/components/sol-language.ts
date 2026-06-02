/**
 * CodeMirror 6 language support for SOL (Sequence Operations Language).
 *
 * Provides:
 *   - Syntax highlighting via a hand-rolled stream tokeniser
 *   - Context-aware autocompletion:
 *       · After `match` / `>>` / `|`  → event type names (message types)
 *       · After `Tag.` or `SEQ.`      → field names (bare, without `$.value.` prefix)
 *       · After `if`/`filter`/`set`/`=`/operators → field paths + functions
 *       · General                     → keywords + snippets + all of the above
 *   - Function signature tooltips when cursor is inside a function call
 *   - Live syntax linting (balanced brackets, unknown names)
 */

import { StreamLanguage, LanguageSupport } from '@codemirror/language'
import {
  autocompletion,
  type CompletionContext,
  type CompletionResult,
  type Completion,
} from '@codemirror/autocomplete'
import { linter, type Diagnostic } from '@codemirror/lint'
import { hoverTooltip, type Tooltip } from '@codemirror/view'

// ── Token definitions ──────────────────────────────────────────────────────────

const KEYWORDS    = ['match', 'split', 'filter', 'set', 'replace', 'combine', 'with', 'if', 'and', 'or', 'not', 'null', 'start', 'end']
const IMPLICIT_TAGS = ['MATCHED', 'PREFIX', 'SUFFIX', 'SEQ']

// Functions with signature metadata for hover / completion detail
interface FnSig { params: string[]; returns: string; desc: string }
const FUNCTION_SIGS: Record<string, FnSig> = {
  duration:       { params: ['from', 'to'],                 returns: 'duration', desc: 'Time between two events or timestamps' },
  length:         { params: ['collection'],                  returns: 'number',   desc: 'Number of elements in a collection' },
  sum:            { params: ['...values'],                   returns: 'number',   desc: 'Sum of numeric values' },
  min:            { params: ['...values'],                   returns: 'number',   desc: 'Minimum value' },
  max:            { params: ['...values'],                   returns: 'number',   desc: 'Maximum value' },
  avg:            { params: ['...values'],                   returns: 'number',   desc: 'Average of numeric values' },
  any:            { params: ['...booleans'],                 returns: 'boolean',  desc: 'True if any argument is true' },
  all:            { params: ['...booleans'],                 returns: 'boolean',  desc: 'True if all arguments are true' },
  unique:         { params: ['collection'],                  returns: 'array',    desc: 'Deduplicated values' },
  position:       { params: ['event'],                       returns: 'number',   desc: 'Index of event in sequence' },
  concat:         { params: ['...strings'],                  returns: 'string',   desc: 'Concatenate strings' },
  lower:          { params: ['string'],                      returns: 'string',   desc: 'Lowercase a string' },
  upper:          { params: ['string'],                      returns: 'string',   desc: 'Uppercase a string' },
  strlen:         { params: ['string'],                      returns: 'number',   desc: 'Length of a string' },
  coalesce:       { params: ['...values'],                   returns: 'any',      desc: 'First non-null value' },
  now:            { params: [],                              returns: 'timestamp', desc: 'Current timestamp' },
  abs:            { params: ['number'],                      returns: 'number',   desc: 'Absolute value' },
  round:          { params: ['number', 'decimals?'],         returns: 'number',   desc: 'Round to N decimal places' },
  floor:          { params: ['number'],                      returns: 'number',   desc: 'Round down' },
  ceiling:        { params: ['number'],                      returns: 'number',   desc: 'Round up' },
  log:            { params: ['number', 'base?'],             returns: 'number',   desc: 'Logarithm' },
  date:           { params: ['timestamp'],                   returns: 'date',     desc: 'Extract date part' },
  time:           { params: ['timestamp'],                   returns: 'time',     desc: 'Extract time part' },
  datetime:       { params: ['string'],                      returns: 'timestamp', desc: 'Parse a datetime string' },
  datepart:       { params: ['unit', 'timestamp'],           returns: 'number',   desc: 'Extract a date/time part (year, month, day, …)' },
  time_bucket:    { params: ['bucket', 'timestamp'],         returns: 'timestamp', desc: 'Truncate timestamp to bucket boundary' },
  edit_distance:  { params: ['a', 'b'],                      returns: 'number',   desc: 'Levenshtein edit distance between two strings' },
  regex_count:    { params: ['string', 'pattern'],           returns: 'number',   desc: 'Count regex matches in string' },
  regex_substr:   { params: ['string', 'pattern', 'group?'],returns: 'string',   desc: 'Extract first regex match' },
  regex_replace:  { params: ['string', 'pattern', 'repl'],  returns: 'string',   desc: 'Replace regex matches' },
  geo_distance:   { params: ['lat1', 'lon1', 'lat2', 'lon2'], returns: 'number', desc: 'Haversine distance in km' },
  geo_contains:   { params: ['point', 'polygon'],            returns: 'boolean',  desc: 'Point-in-polygon test' },
  flatten:        { params: ['array'],                       returns: 'array',    desc: 'Flatten nested arrays one level' },
}
const FUNCTIONS = Object.keys(FUNCTION_SIGS)

function fnDetail(name: string): string {
  const sig = FUNCTION_SIGS[name]
  if (!sig) return 'function'
  return `${name}(${sig.params.join(', ')}): ${sig.returns} — ${sig.desc}`
}

// ── Stream language (tokeniser) ────────────────────────────────────────────────

const solStreamLanguage = StreamLanguage.define<{ inString: boolean }>({
  name: 'sol',
  startState: () => ({ inString: false }),

  token(stream, state) {
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
    if (stream.match('//')) { stream.skipToEnd(); return 'comment' }
    if (stream.match(/^-?\d+(\.\d+)?(ms|min|h|d|w|s)?/)) return 'number'
    if (stream.match('>>')) return 'operator'
    if (stream.match(/^[*+?^|]/)) return 'operator'
    if (stream.match(/^[<>!=]=?/)) return 'operator'
    if (stream.match(/^[-+*/^]/)) return 'operator'
    if (stream.match(/^[A-Za-z_][A-Za-z0-9_]*/)) {
      const word = stream.current()
      if (KEYWORDS.includes(word.toLowerCase()))    return 'keyword'
      if (FUNCTIONS.includes(word.toLowerCase()))   return 'builtin'
      if (IMPLICIT_TAGS.includes(word))             return 'typeName'
      if (/^[A-Z]/.test(word))                      return 'typeName'
      return 'variableName'
    }
    if (stream.match(/^[()[\]{},.:@$]/)) return 'punctuation'
    stream.next()
    return null
  },

  languageData: {
    commentTokens: { line: '//' },
    closeBrackets: { brackets: ['(', '[', '{', "'"] },
  },
})

// ── Context detection ──────────────────────────────────────────────────────────

type CursorCtx =
  | 'match-pattern'   // after match / >> / | — wants event type names
  | 'tag-field'       // after Tag. or SEQ. etc. — wants bare field names
  | 'expression'      // after if/filter/set/=/operator — wants field paths + functions
  | 'general'         // anywhere else

function detectContext(beforeCursor: string): CursorCtx {
  // Strip trailing partial word being typed
  const text = beforeCursor.trimEnd()

  // Dot after an implicit tag or capitalised tag name → field access
  if (/(?:SEQ|MATCHED|PREFIX|SUFFIX|[A-Z][A-Za-z0-9_]*)\s*(?:\[.*?\])?\s*\.$/.test(text)) {
    return 'tag-field'
  }

  // After >> or | — inside a MATCH pattern
  if (/(?:>>|\|)\s*[\w]*$/.test(text)) return 'match-pattern'

  // Opening of a match clause (start of line or after newline, possibly after "match split")
  if (/(?:^|[\n;])\s*match(?:\s+split)?\s+[\w]*$/.test(text)) return 'match-pattern'

  // Inside if / filter / set / after = or comparison operator
  if (/(?:if|filter|set|and|or|not|=|<|>|!|,)\s*[\w$.]*$/.test(text)) return 'expression'

  return 'general'
}

// ── Completion provider ────────────────────────────────────────────────────────

const KEYWORD_COMPLETIONS: Completion[] = KEYWORDS.map(k => ({ label: k, type: 'keyword' }))

const FUNCTION_COMPLETIONS: Completion[] = FUNCTIONS.map(f => ({
  label: f + '()', type: 'function', apply: f + '(',
  detail: fnDetail(f),
  boost: 0,
}))

const TAG_COMPLETIONS: Completion[] = IMPLICIT_TAGS.map(t => ({
  label: t, type: 'constant', detail: 'implicit tag', boost: 2,
}))

const RECIPE_SNIPPETS: Completion[] = [
  { label: 'match A >> * >> B',      type: 'text', detail: 'funnel pattern',   apply: 'match A(event_a) >> * >> B(event_b)' },
  { label: 'match split Session()+', type: 'text', detail: 'sessionize',       apply: 'match split Session()+\nif duration(Session[-1], SUFFIX[0]) > 30min' },
  { label: 'filter MATCHED',         type: 'keyword', detail: 'keep matched only' },
  { label: 'filter not MATCHED',     type: 'keyword', detail: 'keep unmatched only' },
  { label: 'set SEQ.label = ',       type: 'text', detail: 'add dimension' },
  { label: 'filter length(SEQ) > ',  type: 'text', detail: 'min length guard' },
  { label: 'duration(A, B)',         type: 'function', detail: 'time between tags', apply: 'duration(A, B)' },
]

function makeCompletionProvider(messageTypes: string[], fieldPaths: string[]) {
  // Build completions from live data
  const typeCompletions: Completion[] = messageTypes.map(name => ({
    label: name, type: 'variable', detail: 'event type', boost: 3,
  }))

  // Full paths for expression contexts: "$.value.orderId"
  const fieldPathCompletions: Completion[] = fieldPaths.map(path => ({
    label: path, type: 'property', detail: 'field path', boost: 1,
  }))

  // Bare names after a dot: "orderId" (strip $.value. prefix)
  const bareFieldCompletions: Completion[] = fieldPaths
    .filter(p => p.startsWith('$.value.'))
    .map(p => ({
      label: p.replace(/^\$\.value\./, ''), type: 'property',
      detail: p, boost: 3,
    }))

  return (ctx: CompletionContext): CompletionResult | null => {
    const word = ctx.matchBefore(/[\w$.]*/)
    if (!word && !ctx.explicit) return null

    const from  = word?.from ?? ctx.pos
    const text  = word?.text ?? ''
    const before = ctx.state.doc.sliceString(0, ctx.pos)
    const ctxKind = detectContext(before)

    let pool: Completion[]
    switch (ctxKind) {
      case 'match-pattern':
        pool = [...typeCompletions, ...TAG_COMPLETIONS]
        break
      case 'tag-field':
        pool = [...bareFieldCompletions, ...fieldPathCompletions]
        break
      case 'expression':
        pool = [...fieldPathCompletions, ...FUNCTION_COMPLETIONS, ...TAG_COMPLETIONS]
        break
      case 'general':
      default:
        pool = [
          ...TAG_COMPLETIONS,
          ...typeCompletions,
          ...KEYWORD_COMPLETIONS,
          ...FUNCTION_COMPLETIONS,
          ...RECIPE_SNIPPETS,
          ...fieldPathCompletions,
        ]
        break
    }

    const filtered = text
      ? pool.filter(c => c.label.toLowerCase().includes(text.toLowerCase()))
      : pool

    if (filtered.length === 0) return null
    return { from, options: filtered, validFor: /^[\w$.]*$/ }
  }
}

// ── Function signature hover tooltip ─────────────────────────────────────────

function makeSigTooltip() {
  return hoverTooltip((view, pos): Tooltip | null => {
    // Find the word under the cursor
    const line = view.state.doc.lineAt(pos)
    const lineText = line.text
    const colInLine = pos - line.from

    // Walk backwards to find word start
    let start = colInLine
    while (start > 0 && /\w/.test(lineText[start - 1])) start--
    let end = colInLine
    while (end < lineText.length && /\w/.test(lineText[end])) end++

    const word = lineText.slice(start, end).toLowerCase()
    const sig = FUNCTION_SIGS[word]
    if (!sig) return null

    return {
      pos: line.from + start,
      end: line.from + end,
      above: true,
      create() {
        const dom = document.createElement('div')
        dom.style.cssText = 'padding:4px 8px;font-family:var(--font-mono);font-size:0.75rem;max-width:380px;line-height:1.5'
        dom.innerHTML =
          `<strong>${word}(${sig.params.join(', ')})</strong>` +
          ` <span style="opacity:0.6">→ ${sig.returns}</span>` +
          `<br><span style="opacity:0.75">${sig.desc}</span>`
        return { dom }
      },
    }
  })
}

// ── Linter ─────────────────────────────────────────────────────────────────────

function makeLinter(messageTypes: string[], fieldPaths: string[]) {
  const typeSet  = new Set(messageTypes.map(t => t.toLowerCase()))
  const fieldSet = new Set(fieldPaths)

  return linter(view => {
    const diagnostics: Diagnostic[] = []
    const doc = view.state.doc.toString()

    // 1. Balanced parens/brackets
    const stack: Array<{ ch: string; pos: number }> = []
    const pairs: Record<string, string> = { ')': '(', ']': '[', '}': '{' }
    for (let i = 0; i < doc.length; i++) {
      const ch = doc[i]
      if (ch === '(' || ch === '[' || ch === '{') {
        stack.push({ ch, pos: i })
      } else if (ch === ')' || ch === ']' || ch === '}') {
        if (stack.length === 0 || stack[stack.length - 1].ch !== pairs[ch]) {
          diagnostics.push({ from: i, to: i + 1, severity: 'error', message: `Unmatched '${ch}'` })
        } else {
          stack.pop()
        }
      }
    }
    for (const { ch, pos } of stack) {
      diagnostics.push({ from: pos, to: pos + 1, severity: 'error', message: `Unclosed '${ch}'` })
    }

    // 2. Unknown event type names in MATCH patterns (only if we have real data)
    if (typeSet.size > 0) {
      // Find match clauses and check uppercase identifiers used as event names
      const matchPattern = /\bmatch(?:\s+split)?\s+([\s\S]+?)(?=\n(?:if|filter|set|replace|combine|match|\s*$)|$)/gm
      let m: RegExpExecArray | null
      while ((m = matchPattern.exec(doc)) !== null) {
        const clause = m[1]
        const clauseStart = m.index + m[0].indexOf(clause)
        // Find capitalised identifiers (tag names with event types) like Tag(event_name)
        const identRe = /\b([a-z][a-z0-9_]*)\b/g
        let id: RegExpExecArray | null
        while ((id = identRe.exec(clause)) !== null) {
          const name = id[1]
          // Skip keywords, operators pseudo-words
          if (KEYWORDS.includes(name) || IMPLICIT_TAGS.map(t => t.toLowerCase()).includes(name)) continue
          if (!typeSet.has(name)) {
            const absPos = clauseStart + id.index
            diagnostics.push({
              from: absPos, to: absPos + name.length,
              severity: 'warning',
              message: `Unknown event type '${name}' — not seen in sampled data`,
            })
          }
        }
      }
    }

    // 3. Flag `$.value.xxx` field references not in the field set (only if we have real data)
    if (fieldSet.size > 0) {
      const fieldRefRe = /\$\.value\.\w[\w.]*/g
      let fr: RegExpExecArray | null
      while ((fr = fieldRefRe.exec(doc)) !== null) {
        const ref = fr[0]
        if (!fieldSet.has(ref)) {
          diagnostics.push({
            from: fr.index, to: fr.index + ref.length,
            severity: 'warning',
            message: `Field '${ref}' not found in sampled data`,
          })
        }
      }
    }

    return diagnostics
  }, { delay: 400 })
}

// ── Public factory ─────────────────────────────────────────────────────────────

export interface SolLanguageOptions {
  /** Distinct message_type values from the cassette — for MATCH autocompletion */
  messageTypes?: string[]
  /** Full JSONPath field suggestions ($.value.x, $.key, …) — for expression autocompletion */
  fieldPaths?: string[]
}

export function solLanguage(opts: SolLanguageOptions = {}): LanguageSupport {
  const messageTypes = opts.messageTypes ?? []
  const fieldPaths   = opts.fieldPaths   ?? []

  return new LanguageSupport(solStreamLanguage, [
    autocompletion({
      override: [makeCompletionProvider(messageTypes, fieldPaths)],
      activateOnTyping: true,
      maxRenderedOptions: 30,
    }),
    makeSigTooltip(),
    makeLinter(messageTypes, fieldPaths),
  ])
}

// Legacy overload — backwards compat for callers passing a plain string[]
// (previously `eventNames`). Treats the array as fieldPaths.
export function solLanguageLegacy(eventNames: string[] = []): LanguageSupport {
  return solLanguage({ fieldPaths: eventNames })
}

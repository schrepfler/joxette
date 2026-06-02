/**
 * SolEditor — CodeMirror 6 editor for SOL queries.
 *
 * Features:
 *   - SOL syntax highlighting (keywords, functions, tags, strings, comments)
 *   - Autocomplete: keywords, functions, implicit tags, event names from dataset
 *   - ⌘↵ / Ctrl↵ to run
 *   - Tab inserts 2 spaces
 *   - Respects the app's dark/light theme
 */

import { useEffect, useRef, useCallback } from 'react'
import { EditorState, type Extension } from '@codemirror/state'
import {
  EditorView,
  keymap,
  lineNumbers,
  highlightActiveLineGutter,
  highlightSpecialChars,
  drawSelection,
  dropCursor,
  rectangularSelection,
  crosshairCursor,
  highlightActiveLine,
} from '@codemirror/view'
import {
  defaultKeymap,
  history,
  historyKeymap,
  indentWithTab,
} from '@codemirror/commands'
import { completionKeymap, closeBrackets, closeBracketsKeymap } from '@codemirror/autocomplete'
import { syntaxHighlighting, defaultHighlightStyle } from '@codemirror/language'
import { oneDark } from '@codemirror/theme-one-dark'
import { solLanguage } from './sol-language'
import { lintGutter } from '@codemirror/lint'

interface Props {
  value: string
  onChange: (value: string) => void
  onRun?: () => void
  /** @deprecated Use `messageTypes` + `fieldPaths` instead */
  eventNames?: string[]
  /** Distinct message_type values — fed to MATCH clause autocompletion */
  messageTypes?: string[]
  /** Full JSONPath field suggestions ($.value.x, $.key, …) */
  fieldPaths?: string[]
  dark?: boolean
  minHeight?: number
  disabled?: boolean
  /** Compact mode: no line numbers, no gutter, tighter padding. For inline strips. */
  compact?: boolean
}

// ── Light theme overrides to match the app's surface variables ─────────────────

const lightTheme = EditorView.theme({
  '&': {
    fontFamily: 'var(--font-mono)',
    fontSize: 'var(--type-mono-size)',
    background: 'var(--surface-raised)',
    color: 'var(--ink-primary)',
    border: '1px solid var(--rule)',
    borderRadius: 'var(--radius-sm)',
  },
  '.cm-focused': {
    outline: 'none',
    borderColor: 'var(--accent)',
  },
  '.cm-editor.cm-focused': {
    outline: 'none',
  },
  '.cm-scroller': {
    fontFamily: 'inherit',
    lineHeight: '1.6',
  },
  '.cm-content': {
    padding: '8px 0',
    caretColor: 'var(--accent)',
  },
  '.cm-line': {
    padding: '0 12px',
  },
  '.cm-gutters': {
    background: 'var(--surface-raised)',
    borderRight: '1px solid var(--rule)',
    color: 'var(--ink-tertiary)',
  },
  '.cm-activeLineGutter': {
    background: 'var(--ink-wash)',
  },
  '.cm-activeLine': {
    background: 'var(--ink-wash)',
  },
  '.cm-cursor': {
    borderLeftColor: 'var(--accent)',
  },
  '.cm-selectionBackground, ::selection': {
    background: 'color-mix(in oklab, var(--accent) 25%, transparent) !important',
  },
  // Autocomplete dropdown
  '.cm-tooltip': {
    background: 'var(--surface-paper)',
    border: '1px solid var(--rule)',
    borderRadius: 'var(--radius-sm)',
    boxShadow: 'var(--shadow-md)',
  },
  '.cm-tooltip-autocomplete ul li[aria-selected]': {
    background: 'var(--accent)',
    color: 'var(--accent-ink)',
  },
  '.cm-completionLabel': {
    fontFamily: 'var(--font-mono)',
    fontSize: '0.8125rem',
  },
  '.cm-completionDetail': {
    color: 'var(--ink-tertiary)',
    fontSize: '0.75rem',
    marginLeft: 8,
  },
})

// ── Token colours for light mode ───────────────────────────────────────────────

const solHighlightStyle = syntaxHighlighting(defaultHighlightStyle)

// ── Component ──────────────────────────────────────────────────────────────────

export function SolEditor({ value, onChange, onRun, eventNames = [], messageTypes, fieldPaths, dark = false, minHeight = 120, disabled = false, compact = false }: Props) {
  // Resolve props: prefer explicit messageTypes/fieldPaths; fall back to legacy eventNames
  const resolvedMessageTypes = messageTypes ?? []
  const resolvedFieldPaths   = fieldPaths ?? eventNames
  const containerRef = useRef<HTMLDivElement>(null)
  const viewRef      = useRef<EditorView | null>(null)
  const onChangeRef  = useRef(onChange)
  const onRunRef     = useRef(onRun)

  onChangeRef.current = onChange
  onRunRef.current    = onRun

  // Build run keymap
  const runKeymap = keymap.of([
    {
      key: 'Mod-Enter',
      run() { onRunRef.current?.(); return true },
    },
  ])

  const compactTheme = EditorView.theme({
    '&': {
      fontFamily: 'var(--font-mono)',
      fontSize: 'var(--type-caption-size)',
      background: 'var(--surface-paper)',
      color: 'var(--ink-primary)',
      border: '1px solid var(--rule)',
      borderRadius: 'var(--radius-xs)',
    },
    '.cm-focused': { outline: 'none', borderColor: 'var(--accent)' },
    '.cm-editor.cm-focused': { outline: 'none' },
    '.cm-scroller': { fontFamily: 'inherit', lineHeight: '1.4', overflowX: 'auto' },
    '.cm-content': { padding: '4px 8px', caretColor: 'var(--accent)' },
    '.cm-line': { padding: '0' },
    // Autocomplete dropdown
    '.cm-tooltip': {
      background: 'var(--surface-paper)',
      border: '1px solid var(--rule)',
      borderRadius: 'var(--radius-sm)',
      boxShadow: 'var(--shadow-md)',
    },
    '.cm-tooltip-autocomplete ul li[aria-selected]': {
      background: 'var(--accent)', color: 'var(--accent-ink)',
    },
    '.cm-completionLabel': { fontFamily: 'var(--font-mono)', fontSize: '0.8125rem' },
    '.cm-completionDetail': { color: 'var(--ink-tertiary)', fontSize: '0.75rem', marginLeft: 8 },
  })

  const buildExtensions = useCallback((): Extension[] => {
    const base: Extension[] = [
      // Core editing
      history(),
      drawSelection(),
      dropCursor(),
      rectangularSelection(),
      crosshairCursor(),
      ...(!compact ? [highlightActiveLine(), highlightActiveLineGutter(), highlightSpecialChars(), lineNumbers()] : []),
      closeBrackets(),

      // Keymaps
      keymap.of([
        ...closeBracketsKeymap,
        ...defaultKeymap,
        ...historyKeymap,
        ...completionKeymap,
        indentWithTab,
      ]),
      runKeymap,

      // SOL language + autocomplete + linting
      solLanguage({ messageTypes: resolvedMessageTypes, fieldPaths: resolvedFieldPaths }),
      lintGutter(),

      // Highlighting
      solHighlightStyle,

      // Theme
      dark ? oneDark : compact ? compactTheme : lightTheme,

      // Min height
      EditorView.theme({ '.cm-editor': { minHeight: `${minHeight}px` }, '.cm-scroller': { minHeight: `${minHeight}px` } }),

      // Change listener
      EditorView.updateListener.of(update => {
        if (update.docChanged) {
          onChangeRef.current(update.state.doc.toString())
        }
      }),

      // Read-only when disabled
      ...(disabled ? [EditorState.readOnly.of(true)] : []),
    ]
    return base
  }, [resolvedMessageTypes, resolvedFieldPaths, dark, minHeight, disabled, compact]) // eslint-disable-line react-hooks/exhaustive-deps

  // Mount
  useEffect(() => {
    if (!containerRef.current) return
    const view = new EditorView({
      state: EditorState.create({
        doc: value,
        extensions: buildExtensions(),
      }),
      parent: containerRef.current,
    })
    viewRef.current = view
    return () => { view.destroy(); viewRef.current = null }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // Sync value when it changes externally (e.g. recipe selected)
  useEffect(() => {
    const view = viewRef.current
    if (!view) return
    const current = view.state.doc.toString()
    if (current !== value) {
      view.dispatch({
        changes: { from: 0, to: current.length, insert: value },
      })
    }
  }, [value])

  // Rebuild extensions when eventNames/theme changes
  useEffect(() => {
    viewRef.current?.dispatch({
      effects: [],
    })
    // Full reconfigure when event names change — recreate the state
    const view = viewRef.current
    if (!view) return
    const doc = view.state.doc.toString()
    view.setState(EditorState.create({ doc, extensions: buildExtensions() }))
  }, [resolvedMessageTypes, resolvedFieldPaths, dark, disabled, compact]) // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div
      ref={containerRef}
      style={{
        opacity: disabled ? 0.6 : 1,
        pointerEvents: disabled ? 'none' : undefined,
        borderRadius: 'var(--radius-sm)',
        overflow: 'hidden',
      }}
    />
  )
}

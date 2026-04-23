# Joxette — Design System

## Thesis

Joxette is an instrument, not a dashboard. It records Kafka traffic to
replayable cassettes; its readers are the people who investigate, replay,
and reason about production data flows. The interface is for concentration.

The system is **calm, deliberate, unshowy** — rooted in typography and
space. The page is paper-toned, not white. Ink is warm charcoal, not
black. One confident accent — a deep oxblood — carries every load-bearing
call-to-action. Motion is a punctuation mark: slow enough to notice, quiet
enough to trust.

What the system is **not**: a shadcn restyle, a SaaS landing page, a
neon cockpit. Nothing glows. Nothing pulses for attention. Nothing uses
purple-to-pink gradients, nothing uses Inter.

## Typography

**Pairing**: Fraunces + Figtree + JetBrains Mono. All three are self-hosted
as variable fonts via `@fontsource-variable/*`, preloaded with
`font-display: swap`.

| Role        | Family          | Used for                                         |
| ----------- | --------------- | ------------------------------------------------ |
| Display     | Fraunces        | page titles, editorial headings, empty-state pull-quotes |
| Body        | Figtree         | paragraphs, controls, labels, button text        |
| Technical   | JetBrains Mono  | offsets, timestamps, partition numbers, JSON, keys, expressions |

**Why Fraunces**: it carries optical sizing and a SOFT axis, so display
sizes have the warmth of a literary revival typeface without the
stiffness of a revival ripped straight from the nineteenth century. It
holds its character at the small sizes used for empty-state headlines,
where a conventional serif would feel merely decorative.

**Why Figtree**: a humanist sans with a deliberate, slightly geometric
feel. It reads long-form without fatigue, and sits quietly next to the
Fraunces display without competing. It is decidedly not Inter.

**Why JetBrains Mono**: it was drawn for code. Readable tabulars, good
zero distinction, sufficient width to anchor columns of offsets without
eating the row. Every on-screen number uses it.

**Scale** (see `src/design/tokens.css`):

| Token           | Family    | Notes                              |
| --------------- | --------- | ---------------------------------- |
| `display`       | Fraunces  | `clamp(2.25rem, ..., 3.5rem)`, wt 420 |
| `h1`            | Fraunces  | `clamp(1.625rem, ..., 2.125rem)`      |
| `h2`            | Fraunces  | 1.375rem                           |
| `h3`            | Figtree   | 1.0625rem / 620                    |
| `body-lg`       | Figtree   | 1.0625rem / 420                    |
| `body`          | Figtree   | 0.9375rem / 420                    |
| `caption`       | Figtree   | 0.8125rem / 500                    |
| `micro`         | Figtree   | 0.6875rem, uppercase, tracking 0.14em |
| `mono`          | JB Mono   | 0.8125rem, `tabular-nums`          |

Tabular numerals (`font-variant-numeric: tabular-nums`) are non-negotiable
on every offset, count, duration, size, and timestamp. The `<Tabular>`
primitive enforces this.

## Colour

Warm paper; one accent; restrained signals.

### Light

| Token               | Value     | Use                                     |
| ------------------- | --------- | --------------------------------------- |
| `--surface-paper`   | `#F5EFE4` | page base (warm near-white, no pure #FFF) |
| `--surface-sunken`  | `#ECE4D4` | insets, code blocks, modal overlays     |
| `--surface-raised`  | `#FBF7EE` | cards, modals, panels                   |
| `--ink-primary`     | `#1E1A14` | headings, body                          |
| `--ink-secondary`   | `#5A5249` | labels, captions                        |
| `--ink-tertiary`    | `#8A8074` | metadata, empties                       |
| `--accent`          | `#6E1C1C` | primary action, link, focus             |
| `--accent-muted`    | `#9B3232` | hover                                   |
| `--rule`            | `ink @ 9% α` | hairlines                             |
| `--rule-strong`     | `ink @ 18% α`| ruled-input borders, table heads      |
| `--signal-live`     | `#3E6A44` | tailing / recording                     |
| `--signal-warn`     | `#A26612` | paused, disconnected                    |
| `--signal-error`    | `#8B2121` | overflow, error                         |

### Dark

Not a naive inversion. The dark palette is its own composition.

| Token               | Value     |
| ------------------- | --------- |
| `--surface-paper`   | `#1A1713` |
| `--surface-sunken`  | `#14110E` |
| `--surface-raised`  | `#221E18` |
| `--ink-primary`     | `#ECE4D1` |
| `--ink-secondary`   | `#B3A894` |
| `--ink-tertiary`    | `#7A7160` |
| `--accent`          | `#C5585A` — oxblood desaturated for dim surfaces |
| `--signal-live`     | `#7FB684` |
| `--signal-warn`     | `#D59F4C` |
| `--signal-error`    | `#E07070` |

Applied by setting `data-theme="dark"` (or `.dark`) on `<html>`. A
`prefers-color-scheme` media query mirrors the tokens so the OS default is
honoured when no explicit choice is stored.

## Motion

Motion is punctuation, never decoration.

| Token                 | Value            | Use                                 |
| --------------------- | ---------------- | ----------------------------------- |
| `--duration-instant`  | `120ms`          | smallest acknowledgements           |
| `--duration-quick`    | `240ms`          | hover, focus, small state changes   |
| `--duration-default`  | `420ms`          | tab switches, content swaps         |
| `--duration-slow`     | `680ms`          | signature moments (see below)       |
| `--ease-out-soft`     | `cubic-bezier(0.22, 1, 0.36, 1)` | default |
| `--ease-in-out-soft`  | `cubic-bezier(0.65, 0.05, 0.35, 1)` | symmetric |
| `--ease-entrance`     | `cubic-bezier(0.16, 1, 0.3, 1)` | first-paint rise-in |

Under `prefers-reduced-motion: reduce`, all non-instant durations collapse
to `--duration-instant`. Motion never vanishes entirely — a 120ms fade still
communicates a state change; ripping that away hurts comprehension.

### Signature moment: draining → tailing

The transition from **draining** (replaying history) to **tailing** (live
follow) is the one motion worth investing in. When the backend sends its
`follow` preamble, a single hairline rule draws in from left to right
beneath the status badge over `--duration-slow`, easing out. Once drawn,
it holds steady — the live dot beside it carries the ongoing motion.

If the stream disconnects, the rule retracts to the right over
`--duration-quick`. Drawing in is a commitment; retreating is a note.

See `src/design/primitives/SignatureRule.tsx`.

## Primitives

All in `src/design/primitives/`, all consuming tokens, none using
stock Tailwind colours.

- **`<Button>`** — `primary` | `secondary` | `ghost` | `danger`. No shadow.
  Hover lifts by ½px; active drops by ½px.
- **`<Input>` / `<Select>`** — ruled underline, not bordered box. Focus
  thickens the rule and colours it accent over `--duration-quick`.
- **`<Badge>` + `<StatusDot>`** — status chips. Each dot has a distinct
  *shape* (filled / ring / square / pulse / slash), not just a colour, so
  status reads in monochrome and across colour-blind gamuts.
- **`<Hairline>`** — 1px `--rule` divider. Used liberally; boxes are a last
  resort.
- **`<Kbd>`** — mono, ruled, subtle. For inline keyboard shortcuts.
- **`<Tabular>`** — mono + `tabular-nums`. Used on every offset, timestamp,
  count, byte-size. Accepts `size: 'xs' | 'sm' | 'md'` and a `muted` tone.
- **`<SignatureRule>`** — the one motion primitive; see above.

### Accessibility

- All focus states use `:focus-visible` with a 2px `--focus-ring` outline
  offset 2px. No relying on OS defaults.
- Colour is always doubled with shape, text, or iconography — never the
  sole carrier of meaning.
- Contrast tested against AA at minimum for `--ink-primary` on
  `--surface-paper` (light) and `--surface-raised` (dark).
- `prefers-reduced-motion` respected throughout.

## Reference implementation

`src/routes/topics/$topic.tsx` is rebuilt against the system. It is
deliberately the busiest route in the app: recording config, retention,
matchers, stats, replay (paged + SSE + NDJSON), tab navigation, ruled
inputs, a segmented control, an empty state, and first-class stream status
treatment. If the system works there, it works.

Other routes remain on legacy tokens and are migrated as work arrives.
The legacy `--sea-ink`, `--lagoon`, etc. variables alias to new tokens in
`src/styles.css` so that un-migrated pages do not break.

## Non-goals

- Matching any particular third-party design system (shadcn, MUI, Ant).
- A component library for external consumption.
- Theme customisation beyond light/dark.
- Animation libraries — CSS transitions and tiny keyframes only.

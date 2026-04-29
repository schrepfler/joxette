---
version: alpha
name: Joxette
description: Design system for Joxette, a Kafka topic cassette recorder. Developer-tool aesthetic, data-forward, light-first, minimal ornamentation. Evokes tape recording gear — precise, functional, retro-technical.

colors:
  # Brand
  primary: "#0C1D42"
  primary-800: "#162851"
  primary-700: "#1E3566"
  primary-600: "#284480"
  primary-500: "#3A5CA0"
  primary-400: "#5A7ABF"
  primary-300: "#8AA0D4"
  primary-200: "#B8C6E4"
  primary-100: "#DDE3F0"
  primary-50: "#F0F3FA"
  accent: "#166DF8"
  accent-hover: "#1d4ed8"
  accent-50: "#eff6ff"
  silver: "#B8C0D0"
  silver-dark: "#8E9AAE"
  silver-light: "#DDE1EC"
  # Neutrals
  neutral-0: "#ffffff"
  neutral-50: "#f9fafb"
  neutral-100: "#f3f4f6"
  neutral-200: "#e5e7eb"
  neutral-300: "#d1d5db"
  neutral-400: "#9ca3af"
  neutral-500: "#6b7280"
  neutral-600: "#4b5563"
  neutral-700: "#374151"
  neutral-800: "#1f2937"
  neutral-900: "#111827"
  neutral-950: "#030712"
  # Semantic
  bg-page: "#ffffff"
  bg-subtle: "#f9fafb"
  bg-muted: "#f3f4f6"
  bg-inverted: "#030712"
  fg-primary: "#030712"
  fg-secondary: "#4b5563"
  fg-tertiary: "#9ca3af"
  fg-inverted: "#ffffff"
  fg-link: "#166DF8"
  border-default: "#e5e7eb"
  border-strong: "#d1d5db"
  border-focus: "#166DF8"
  # Status
  success: "#16a34a"
  error: "#dc2626"
  warning: "#f59e0b"

typography:
  h1:
    fontFamily: DM Sans
    fontSize: 3rem
    fontWeight: 600
    lineHeight: 1.2
    letterSpacing: -0.02em
  h2:
    fontFamily: DM Sans
    fontSize: 2.25rem
    fontWeight: 600
    lineHeight: 1.2
    letterSpacing: -0.02em
  h3:
    fontFamily: DM Sans
    fontSize: 1.5rem
    fontWeight: 600
    lineHeight: 1.35
    letterSpacing: -0.02em
  h4:
    fontFamily: DM Sans
    fontSize: 1.125rem
    fontWeight: 600
    lineHeight: 1.35
  body-lg:
    fontFamily: DM Sans
    fontSize: 1.125rem
    fontWeight: 400
    lineHeight: 1.65
  body:
    fontFamily: DM Sans
    fontSize: 1rem
    fontWeight: 400
    lineHeight: 1.65
  body-sm:
    fontFamily: DM Sans
    fontSize: 0.875rem
    fontWeight: 400
    lineHeight: 1.5
  label:
    fontFamily: DM Sans
    fontSize: 0.875rem
    fontWeight: 500
    lineHeight: 1.5
  caption:
    fontFamily: DM Sans
    fontSize: 0.75rem
    fontWeight: 400
    lineHeight: 1.5
    letterSpacing: 0.04em
  overline:
    fontFamily: DM Sans
    fontSize: 0.75rem
    fontWeight: 600
    lineHeight: 1.5
    letterSpacing: 0.08em
  display:
    fontFamily: Montserrat
    fontSize: 3.75rem
    fontWeight: 800
    lineHeight: 1.2
    letterSpacing: 0.08em
  code:
    fontFamily: DM Mono
    fontSize: 0.875rem
    fontWeight: 400
    lineHeight: 1.65

rounded:
  sm: 4px
  md: 6px
  lg: 8px
  xl: 12px
  2xl: 16px
  full: 9999px

spacing:
  1: 4px
  2: 8px
  3: 12px
  4: 16px
  5: 20px
  6: 24px
  8: 32px
  10: 40px
  12: 48px
  16: 64px
  20: 80px
  24: 96px

components:
  button-primary:
    backgroundColor: "{colors.accent}"
    textColor: "{colors.fg-inverted}"
    typography: "{typography.label}"
    rounded: "{rounded.md}"
    padding: "8px 16px"
  button-primary-hover:
    backgroundColor: "{colors.accent-hover}"
    textColor: "{colors.fg-inverted}"
  button-secondary:
    backgroundColor: "{colors.bg-page}"
    textColor: "{colors.fg-primary}"
    rounded: "{rounded.md}"
    padding: "7px 15px"
  button-secondary-hover:
    backgroundColor: "{colors.bg-subtle}"
  button-ghost:
    backgroundColor: "transparent"
    textColor: "{colors.accent}"
    rounded: "{rounded.md}"
    padding: "8px 12px"
  button-ghost-hover:
    backgroundColor: "{colors.accent-50}"
  button-danger:
    backgroundColor: "{colors.error}"
    textColor: "{colors.fg-inverted}"
    rounded: "{rounded.md}"
    padding: "8px 16px"
  button-disabled:
    backgroundColor: "{colors.bg-muted}"
    textColor: "{colors.fg-tertiary}"
    rounded: "{rounded.md}"
    padding: "8px 16px"
  card:
    backgroundColor: "{colors.bg-page}"
    rounded: "{rounded.xl}"
    padding: "24px"
  card-dark:
    backgroundColor: "{colors.primary}"
    textColor: "{colors.fg-inverted}"
    rounded: "{rounded.xl}"
    padding: "24px"
  input:
    backgroundColor: "{colors.bg-page}"
    textColor: "{colors.fg-primary}"
    rounded: "{rounded.md}"
    padding: "8px 12px"
  badge-default:
    backgroundColor: "{colors.bg-muted}"
    textColor: "{colors.fg-secondary}"
    rounded: "{rounded.full}"
    padding: "2px 10px"
  badge-brand:
    backgroundColor: "{colors.primary-50}"
    textColor: "{colors.primary-700}"
    rounded: "{rounded.full}"
    padding: "2px 10px"
  badge-success:
    backgroundColor: "#dcfce7"
    textColor: "{colors.success}"
    rounded: "{rounded.full}"
    padding: "2px 10px"
  badge-error:
    backgroundColor: "#fee2e2"
    textColor: "{colors.error}"
    rounded: "{rounded.full}"
    padding: "2px 10px"
  nav:
    backgroundColor: "{colors.primary}"
    textColor: "{colors.fg-inverted}"
    height: "64px"
---

## Overview

**Developer precision meets tape-recorder character.** Joxette's UI is light-first — crisp white and neutral-gray surfaces as the default canvas, with navy used as a structural accent for the nav and sidebar rather than as a background wash. A single CTA blue handles all interactive affordances. The visual language reflects the product: a system that captures, stores, and replays data streams with fidelity.

The brand mark is a cassette tape with a cursive "J" on the label window — rendered in dark ink on white for light surfaces, silver-on-navy when placed on dark backgrounds. The system is built for a technical audience: platform engineers, data engineers, and backend developers who care about Kafka offset semantics and DuckLake storage.

**Core principle:** Every element earns its place. No emoji, no gradients as backgrounds, no decorative flourishes. Density and precision over marketing softness. Light backgrounds keep data readable at a glance; navy is reserved for structure and emphasis, not decoration.

## Colors

The palette has three tiers:

- **Brand Navy (`#0C1D42`):** The primary identity color. Used as the nav and sidebar background only — not as a general page or section fill. Communicates depth and reliability — the "tape housing" of the product. Most content surfaces use `bg-page` or `bg-subtle` (light neutrals). A full scale from `primary-50` (#F0F3FA) to `primary-950` supports tints and semantic uses.
- **CTA Blue (`#166DF8`):** The sole interactive accent. Used for buttons, links, focus rings, and active states. The single "record" signal — act here.
- **Silver (`#B8C0D0`):** Metallic accent used exclusively on the logo mark and select decorative moments, evoking the metallic parts of cassette hardware. Not used for UI interaction.

Neutral grays follow a standard 50–950 scale. Status colors are standard semantic green/red/amber — functional only, not decorative.

**On dark surfaces** (navy bg): use `fg-inverted` (#fff) for headings, `rgba(255,255,255,0.65)` for secondary text, and `rgba(255,255,255,0.08)` for card overlays.

## Typography

Three font families, strict roles:

- **Montserrat** — brand/display only. Used for the wordmark, hero headlines, and landing page display text. Always wide-tracked (`0.08em`), weight 800 for the brand name. Tagline reads: *"Record once. Replay anything."*
- **DM Sans** — all UI text. Clean geometric sans. Headings at 600 weight, body at 400. Minimum font size in production UI: 12px (0.75rem).
- **DM Mono** — offsets, partition numbers, byte counts, timestamps, JSON values, cursor tokens, and all data metrics. Tabular numbers enabled. This is a data tool — numeric precision is a first-class concern.

Heading hierarchy is tight (`line-height: 1.2`) with `-0.02em` tracking. Body is relaxed (`line-height: 1.65`). Overlines use `0.08em` tracking, uppercase, semibold — for section labels like `TOPICS`, `ENTITIES`, `CASSETTES`.

**Copy voice:** Sentence case throughout; all-caps reserved for the brand name and overline labels. No hedging language. Direct, tool-like. Examples: "Start recording", "Replay from offset", "Compact now."

## Layout

- **8px base grid.** All spacing values are multiples of 4px, starting at `space-1` (4px) through `space-24` (96px).
- **Max content width:** 1200px, centered with horizontal padding of `space-6` (24px) on mobile and `space-8` (32px) on desktop.
- **Column system:** 12-column grid with 24px gutters.
- **Section rhythm:** Sections separated by `space-16` (64px) or `space-20` (80px) vertically.

Light surfaces (`bg-page`, `bg-subtle`) are the default. They alternate to create section separation without borders. Navy (`primary`) is reserved for the nav, sidebar, and occasional CTA banners — not for general section backgrounds. The default reading experience is light.

## Elevation & Depth

Shadows are intentionally subtle — low-opacity, cold-tinted blacks. They indicate layering, not drama.

| Token | Value | Use |
|---|---|---|
| `shadow-xs` | `0 1px 2px rgba(0,0,0,0.05)` | Inputs, chips |
| `shadow-sm` | `0 1px 3px rgba(0,0,0,0.08)` | Cards, badges |
| `shadow-md` | `0 4px 6px rgba(0,0,0,0.08)` | Popovers, dropdowns |
| `shadow-lg` | `0 10px 15px rgba(0,0,0,0.08)` | Modals, elevated panels |
| `shadow-xl` | `0 20px 25px rgba(0,0,0,0.08)` | Toast notifications |

No colored shadows. No glow effects.

## Shapes

Border radii follow a constrained scale:

- `sm` (4px) — tags, code snippets, small inputs, offset/partition chips
- `md` (6px) — buttons (default), form inputs
- `lg` (8px) — cards, panels
- `xl` (12px) — feature cards, modals, topic config panels
- `2xl` (16px) — large stat panels, cassette summary cards
- `full` (9999px) — status badges, pills, avatar circles

No sharp (0px) corners in the UI. No extreme rounding on rectangular containers.

## Components

### Buttons
Four variants: **Primary** (CTA blue fill), **Secondary** (white + border), **Ghost** (transparent + blue text), **Danger** (red fill). Three sizes: `sm` (5px 12px padding), default (8px 16px), `lg` (12px 24px). All use `border-radius: md` (6px) and `font-weight: 500`.

Transition: `150ms ease` on background and color. No scale transforms on hover.

### Cards
White background, `1px solid border-default`, `border-radius: lg` or `xl`, `shadow-sm`. On dark surfaces: `rgba(255,255,255,0.08)` background, no border. Internal padding: `space-6` (24px).

Topic cards, entity type cards, and cassette stat cards all use this base. Add a left-accent border (2px, `accent` blue) for active recording topics.

### Form Inputs
White background, `border-default` border, `border-radius: md`. Focus ring: `2px solid border-focus` (CTA blue), `outline: none`. Placeholder text: `fg-tertiary`.

### Navigation
Top nav: `primary` navy background, 64px height, white text. Logo (cassette mark) left, nav links center or right, primary action button rightmost. Sidebar nav: same navy background, items at `body-sm` size, active item highlighted with `rgba(255,255,255,0.12)` background and `accent` left border (2px).

Nav sections map to product areas: Topics, Entities, Cassettes, Compaction. The nav is the primary dark surface in the layout; everything behind it is light.

### Badges
Pill shape (`border-radius: full`), `caption` typography. Used for topic recording mode (`GENERAL`, `ENTITY`, `BOTH`), consumer status (`RECORDING`, `PAUSED`, `STOPPED`), and compaction state (`IDLE`, `RUNNING`).

Default (gray), Brand (navy tint, for modes), Success (green, for healthy/recording), Warning (amber, for paused/lagging), Error (red, for failed/stopped).

### Data & Charts
`DM Mono` for all numeric values: offsets, partition numbers, message counts, byte sizes, lag counts. Chart accent colors: CTA blue for the primary series (e.g., messages/sec); `primary-300` and `primary-400` for secondary series (e.g., per-partition breakdown). Grid lines: `border-default`. Axis labels: `fg-tertiary`, `caption` size.

Replay timeline visualizations should use the cassette tape visual metaphor where appropriate — a horizontal scrub bar with a playhead cursor.

## Do's and Don'ts

**Do:**
- Use `primary` navy for the nav, sidebar, and hero sections — it anchors the cassette-recorder identity.
- Use `accent` blue exclusively for interactive affordances (buttons, links, focus states, active recording indicators).
- Use `DM Mono` for any displayed number, offset, partition, byte count, or timestamp — it signals data precision.
- Maintain generous whitespace. Density is fine for data tables; breathing room is required for structural sections.
- Use `overline` style (uppercase, wide-tracked) for section labels: `TOPICS`, `ENTITY TYPES`, `CASSETTES`, `COMPACTION`.
- Use `shadow-sm` on cards and `shadow-md` on dropdowns/popovers.
- Use active-recording state (left-accent border, green badge) to make it immediately clear which topics are live.

**Don't:**
- Use gradient backgrounds. The system is flat surfaces with shadow depth.
- Use emoji as icons or decorative elements. Use Lucide icons (stroke-weight 1.8).
- Mix Montserrat into UI body copy — it is display/brand only.
- Use `silver` as a UI interaction color — it is logo decoration only.
- Apply `border-radius: full` to non-pill rectangular containers.
- Use more than one accent color per screen. The CTA blue is the sole interactive signal.
- Add animations beyond `150ms ease` transitions. No bounce, elastic, or spring effects.
- Show raw file paths or internal storage URIs in the UI — surface logical concepts (cassette, topic, entity) instead.

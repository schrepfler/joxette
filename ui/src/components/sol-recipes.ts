/**
 * SOL recipe library — common query patterns offered by the "Add recipe"
 * dropdown in the SOL editors. Shared between the single-sequence query panel
 * and the multi-sequence explorer.
 */

export interface SolRecipe {
  label: string
  description: string
  sol: string
}

export const SOL_RECIPES: SolRecipe[] = [
  {
    label: 'Define a funnel',
    description: 'Find sequences where event_a is eventually followed by event_b.',
    sol: 'match event_a >> * >> event_b',
  },
  {
    label: 'Funnel with time constraint',
    description: 'Same funnel, but only keep sequences that complete within 5 minutes.',
    sol: 'match A(event_a) >> * >> B(event_b)\nif duration(A, B) < 5min',
  },
  {
    label: 'Filter to matched sequences',
    description: 'Keep only the sequences where the pattern matched.',
    sol: 'match Target(event_name)\nfilter MATCHED',
  },
  {
    label: 'Compute duration between events',
    description: 'Annotate each sequence with the elapsed time between two events.',
    sol: 'match A(event_a) >> * >> B(event_b)\nset time_between = duration(A, B)\nfilter MATCHED',
  },
  {
    label: 'Find when something didn’t happen',
    description: 'Keep only sequences that never contain a specific unwanted event.',
    sol: 'match start >> (^unwanted_event)* >> end\nfilter MATCHED',
  },
  {
    label: 'Explore before or after a specific event',
    description: 'Trim each sequence to the events leading up to (and including) a target event.',
    sol: 'match start >> PREFIX()* >> Target(event_name)\nreplace SEQ with PREFIX >> Target',
  },
  {
    label: 'Split sequences on time gap between events',
    description: 'Sessionize: split a sequence wherever the gap between events exceeds 30 minutes.',
    sol: 'match split Session()+\nif duration(Session[-1], SUFFIX[0]) > 30min',
  },
  {
    label: 'Remove consecutive repeated events',
    description: 'Collapse runs of the same event down to a single occurrence.',
    sol: 'match split A(event_name){2,}\nreplace A with A[-1]\ncombine',
  },
  {
    label: 'Count occurrences of an event',
    description: 'Count how many times an event appears using split + combine.',
    sol: 'match split E(event_name)\ncombine count = max(split_index)',
  },
  {
    label: 'Compute attribution',
    description: 'Tag the most recent touchpoint event before an outcome, ignoring repeated touches.',
    sol: 'match LastTouch(event_a | event_b | event_c) >>\n  (^event_a, ^event_b, ^event_c)* >> outcome_event',
  },
]

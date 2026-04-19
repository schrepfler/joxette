import { Store } from '@tanstack/store'
import { useEffect, useState } from 'react'
import type { MatchStep, SequenceConstraints, SequenceQuery, SequenceMatchResponse } from '../transforms/types'

export const STEP_COLORS = [
  '#4f6ef7',
  '#e05c5c',
  '#2ecc71',
  '#f39c12',
  '#9b59b6',
  '#1abc9c',
  '#e67e22',
  '#3498db',
  '#e91e63',
  '#00bcd4',
]

interface SequenceState {
  query: SequenceQuery
  results: SequenceMatchResponse | null
  loading: boolean
  error: string | null
}

const initialState: SequenceState = {
  query: { steps: [], constraints: undefined },
  results: null,
  loading: false,
  error: null,
}

export const sequenceStore = new Store<SequenceState>(initialState)

export function useSequenceStore(): SequenceState {
  const [state, setState] = useState<SequenceState>(sequenceStore.state)
  useEffect(() => {
    const sub = sequenceStore.subscribe(() => setState(sequenceStore.state))
    return () => sub.unsubscribe()
  }, [])
  return state
}

export function setSteps(steps: MatchStep[]) {
  sequenceStore.setState((s) => ({ ...s, query: { ...s.query, steps } }))
}

export function addStep(step: MatchStep) {
  sequenceStore.setState((s) => ({ ...s, query: { ...s.query, steps: [...s.query.steps, step] } }))
}

export function removeStep(id: string) {
  sequenceStore.setState((s) => ({
    ...s,
    query: { ...s.query, steps: s.query.steps.filter((st) => st._id !== id) },
  }))
}

export function updateStep(id: string, patch: Partial<MatchStep>) {
  sequenceStore.setState((s) => ({
    ...s,
    query: {
      ...s.query,
      steps: s.query.steps.map((st) => (st._id === id ? { ...st, ...patch } : st)),
    },
  }))
}

export function reorderSteps(fromIndex: number, toIndex: number) {
  sequenceStore.setState((s) => {
    const steps = [...s.query.steps]
    const [moved] = steps.splice(fromIndex, 1)
    steps.splice(toIndex, 0, moved)
    return { ...s, query: { ...s.query, steps } }
  })
}

export function setConstraints(constraints: SequenceConstraints | undefined) {
  sequenceStore.setState((s) => ({ ...s, query: { ...s.query, constraints } }))
}

export function setResults(results: SequenceMatchResponse | null) {
  sequenceStore.setState((s) => ({ ...s, results }))
}

export function setLoading(loading: boolean) {
  sequenceStore.setState((s) => ({ ...s, loading }))
}

export function setError(error: string | null) {
  sequenceStore.setState((s) => ({ ...s, error }))
}

export function resetSequenceStore() {
  sequenceStore.setState(() => initialState)
}

/** Maps each step _id to a color from STEP_COLORS by its position in the steps array. */
export function stepColors(steps: MatchStep[]): Record<string, string> {
  return Object.fromEntries(
    steps.map((st, i) => [st._id, STEP_COLORS[i % STEP_COLORS.length]]),
  )
}

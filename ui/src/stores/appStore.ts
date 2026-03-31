import { Store } from '@tanstack/store'
import { useEffect, useState } from 'react'

interface AppState {
  selectedTopic: string | null
  selectedEntityType: string | null
  selectedEntityId: string | null
}

export const appStore = new Store<AppState>({
  selectedTopic: null,
  selectedEntityType: null,
  selectedEntityId: null,
})

export function useAppStore(): AppState {
  const [state, setState] = useState<AppState>(appStore.state)
  useEffect(() => {
    const sub = appStore.subscribe(() => setState(appStore.state))
    return () => sub.unsubscribe()
  }, [])
  return state
}

export function setSelectedTopic(topic: string | null) {
  appStore.setState((s) => ({ ...s, selectedTopic: topic }))
}

export function setSelectedEntityType(entityType: string | null) {
  appStore.setState((s) => ({ ...s, selectedEntityType: entityType }))
}

export function setSelectedEntityId(entityId: string | null) {
  appStore.setState((s) => ({ ...s, selectedEntityId: entityId }))
}

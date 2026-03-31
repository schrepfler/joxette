const API_BASE = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8080'

// ---- Types ----

export interface TopicConfig {
  topic: string
  mode: string
  paused: boolean
  active: boolean
  consumerGroup?: string
}

export interface CreateTopicRequest {
  topic: string
  mode?: string
  timePartition?: string
  maxFileSizeMb?: number
  maxRecordsPerFile?: number
  retentionDays?: number
  consumerGroup?: string
  startFrom?: string
}

export interface UpdateTopicRequest {
  mode: string
}

export interface EntityTypeConfig {
  entityType: string
  buckets: number
  retentionDays?: number
}

export interface EntitySourceConfig {
  entityType: string
  topic: string
  idSource: string
  idExpression: string
}

export interface CreateEntityRequest {
  type: string
  buckets?: number
  retentionDays?: number
}

export interface UpdateEntityRequest {
  buckets: number
}

export interface AddSourceRequest {
  topic: string
  idSource?: string
  idExpression: string
}

export interface Header {
  key: string
  value: string
}

export interface CassetteRecord {
  topic: string
  partition: number
  offset: number
  timestamp: string
  recordedAt: string
  key: string | null
  value: string | null
  headers: Header[]
}

export interface EntityRecord {
  entityId: string
  entityBucket: number
  topic: string
  partition: number
  offset: number
  timestamp: string
  recordedAt: string
  key: string | null
  value: string | null
  headers: Header[]
}

export interface PagedResponse<T> {
  data: T[]
  nextCursor?: string
  hasMore: boolean
}

export interface EntityInfo {
  entityType: string
  entityId: string
  entityBucket: number
  firstSeen: string
  lastSeen: string
}

export interface CassetteStats {
  topic: string
  tableName: string
  rowCount: number
  estimatedSizeBytes: number
}

export interface EntityStats {
  entityType: string
  entityId: string
  messageCount: number
  firstMessage: string | null
  lastMessage: string | null
  firstSeen: string | null
  lastSeen: string | null
  countByTopic: Record<string, number>
}

export interface CompactionRun {
  id: number
  startedAt: string
  completedAt: string | null
  status: string
  triggeredBy: string
  targets: string[] | null
  entityBucketsCompacted: number
  generalPartitionsCompacted: number
  errorMessage: string | null
}

export interface CompactionStatus {
  lastRun: CompactionRun | null
  nextScheduledRun: string | null
  running: boolean
}

export interface TriggerRequest {
  targets?: string[]
}

export interface TopicLag {
  topic: string
  totalLag: number
  lagByPartition: Record<string, number>
}

export interface HealthStatus {
  status: string
  activeRecorders: string[]
  consumerLag: TopicLag[]
  catalogSizeBytes: number
  inlinedDataSizeBytes: number
  catalogPath: string
}

export interface SnapshotInfo {
  name: string
  createdAt: string
  sizeBytes: number
}

// ---- HTTP helpers ----

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...init?.headers },
    ...init,
  })
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    throw new Error(`HTTP ${res.status} ${res.statusText}: ${text}`)
  }
  if (res.status === 204) return undefined as unknown as T
  return res.json() as Promise<T>
}

type QueryParams = Record<string, string | number | undefined | null>

function buildQuery(params: QueryParams): string {
  const entries = Object.entries(params).filter(([, v]) => v != null && v !== '')
  if (entries.length === 0) return ''
  return '?' + entries.map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`).join('&')
}

// ---- Topics ----

export const topicsApi = {
  list: () => request<TopicConfig[]>('/topics'),
  create: (body: CreateTopicRequest) => request<TopicConfig>('/topics', { method: 'POST', body: JSON.stringify(body) }),
  get: (topic: string) => request<TopicConfig>(`/topics/${encodeURIComponent(topic)}`),
  update: (topic: string, body: UpdateTopicRequest) =>
    request<TopicConfig>(`/topics/${encodeURIComponent(topic)}`, { method: 'PUT', body: JSON.stringify(body) }),
  delete: (topic: string) => request<void>(`/topics/${encodeURIComponent(topic)}`, { method: 'DELETE' }),
  pause: (topic: string) => request<TopicConfig>(`/topics/${encodeURIComponent(topic)}/pause`, { method: 'POST' }),
  resume: (topic: string) => request<TopicConfig>(`/topics/${encodeURIComponent(topic)}/resume`, { method: 'POST' }),
}

// ---- Entities ----

export const entitiesApi = {
  list: () => request<EntityTypeConfig[]>('/entities'),
  create: (body: CreateEntityRequest) => request<EntityTypeConfig>('/entities', { method: 'POST', body: JSON.stringify(body) }),
  get: (type: string) => request<EntityTypeConfig>(`/entities/${encodeURIComponent(type)}`),
  update: (type: string, body: UpdateEntityRequest) =>
    request<EntityTypeConfig>(`/entities/${encodeURIComponent(type)}`, { method: 'PUT', body: JSON.stringify(body) }),
  delete: (type: string) => request<void>(`/entities/${encodeURIComponent(type)}`, { method: 'DELETE' }),
  getSources: (type: string) => request<EntitySourceConfig[]>(`/entities/${encodeURIComponent(type)}/sources`),
  addSource: (type: string, body: AddSourceRequest) =>
    request<EntitySourceConfig>(`/entities/${encodeURIComponent(type)}/sources`, { method: 'POST', body: JSON.stringify(body) }),
  deleteSource: (type: string, topic: string) =>
    request<void>(`/entities/${encodeURIComponent(type)}/sources/${encodeURIComponent(topic)}`, { method: 'DELETE' }),
}

// ---- Cassettes ----

export type CassetteTopicParams = QueryParams & {
  from?: string
  to?: string
  partition?: number
  offset_from?: number
  offset_to?: number
  limit?: number
  cursor?: string
}

export type EntityListParams = QueryParams & {
  limit?: number
  cursor?: string
}

export type EntitySearchParams = QueryParams & {
  q?: string
  limit?: number
  cursor?: string
}

export type EntityRecordsParams = QueryParams & {
  from?: string
  to?: string
  limit?: number
  cursor?: string
}

export const cassettesApi = {
  getTopicRecords: (topic: string, params?: CassetteTopicParams) =>
    request<PagedResponse<CassetteRecord>>(`/cassettes/topics/${encodeURIComponent(topic)}${buildQuery(params ?? {})}`),
  getTopicStats: (topic: string) =>
    request<CassetteStats>(`/cassettes/topics/${encodeURIComponent(topic)}/stats`),
  compactTopic: (topic: string) =>
    request<void>(`/cassettes/topics/${encodeURIComponent(topic)}/compact`, { method: 'POST' }),
  truncateTopic: (topic: string) =>
    request<{ deleted: number }>(`/cassettes/topics/${encodeURIComponent(topic)}/truncate`, { method: 'POST' }),
  listEntities: (entityType: string, params?: EntityListParams) =>
    request<PagedResponse<EntityInfo>>(`/cassettes/entities/${encodeURIComponent(entityType)}${buildQuery(params ?? {})}`),
  searchEntities: (entityType: string, params?: EntitySearchParams) =>
    request<PagedResponse<EntityInfo>>(`/cassettes/entities/${encodeURIComponent(entityType)}/search${buildQuery(params ?? {})}`),
  getEntityRecords: (entityType: string, entityId: string, params?: EntityRecordsParams) =>
    request<PagedResponse<EntityRecord>>(`/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}${buildQuery(params ?? {})}`),
  getEntityStats: (entityType: string, entityId: string) =>
    request<EntityStats>(`/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}/stats`),
  deleteEntity: (entityType: string, entityId: string) =>
    request<{ deleted: number }>(`/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}`, { method: 'DELETE' }),
  listSnapshots: () => request<SnapshotInfo[]>('/cassettes/snapshots'),
  createSnapshot: (body?: { name?: string }) =>
    request<SnapshotInfo>('/cassettes/snapshots', { method: 'POST', body: JSON.stringify(body ?? {}) }),
  restoreSnapshot: (name: string) =>
    request<void>(`/cassettes/snapshots/${encodeURIComponent(name)}/restore`, { method: 'POST' }),
}

// ---- Compaction ----

export const compactionApi = {
  getStatus: () => request<CompactionStatus>('/compaction/status'),
  trigger: (body?: TriggerRequest) =>
    request<CompactionRun>('/compaction/trigger', { method: 'POST', body: JSON.stringify(body ?? {}) }),
  getHistory: (limit?: number) =>
    request<CompactionRun[]>(`/compaction/history${limit != null ? `?limit=${limit}` : ''}`),
}

// ---- Health ----

export const healthApi = {
  get: () => request<HealthStatus>('/health'),
}

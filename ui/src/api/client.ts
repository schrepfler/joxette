const API_BASE = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8080'

// ---- Types ----

export interface TopicConfig {
  topic: string
  mode: string
  paused: boolean
  active: boolean
  consumerGroup?: string
  retentionDays?: number
  brokerId?: string | null
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
  brokerId?: string
}

export interface UpdateTopicRequest {
  mode: string
  brokerId?: string | null
}

export interface TopicMatcherConfig {
  topic: string
  messageType: string
  idSource: string
  idExpression: string
}

export interface AddMatcherRequest {
  messageType: string
  idSource: string
  idExpression: string
}

export interface EntityTypeConfig {
  entityType: string
  buckets: number
  retentionDays?: number
}

export interface MatcherConfig {
  messageType: string
  idSource: string
  idExpression: string
}

export interface EntitySourceConfig {
  entityType: string
  topic: string
  mode: string
  matchers: MatcherConfig[]
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
  mode?: string
  matchers?: MatcherConfig[]
}

export interface Header {
  key: string
  value: string
}

export interface PeekMessage {
  partition: number
  offset: number
  timestamp: string
  key: string | null
  value: string
  valueEncoding: 'utf8' | 'base64'
  headers: Header[]
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
  messageType: string | null
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

export interface CompactionConfig {
  schedule: string
  entity: {
    lookbackDays: number
    minFilesPerBucket: number
    targetFileSizeMb: number
  }
  general: {
    enabled: boolean
    lookbackDays: number
    minFilesPerPartition: number
    targetFileSizeMb: number
  }
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

// ---- Transform types ----

export interface TransformPreset {
  name: string
  description?: string
  steps: import('../transforms/types').TransformStep[]
  fragments?: import('../transforms/types').FragmentDefinition[]
  createdAt?: string
  updatedAt?: string
}

export interface CreatePresetRequest {
  name: string
  description?: string
  steps: import('../transforms/types').TransformStep[]
}

export interface UpdatePresetRequest {
  description?: string
  steps: import('../transforms/types').TransformStep[]
}

/** Preview request body for POST /cassettes/{mode}/preview-transforms */
export interface PreviewTransformsRequest {
  mode: 'topic' | 'entity'
  topic?: string
  entityType?: string
  entityId?: string
  steps: import('../transforms/types').TransformStep[]
  limit?: number
}

export interface PreviewTransformsResponse {
  original: CassetteRecord | EntityRecord
  transformed: CassetteRecord | EntityRecord
}

// ---- Replay-to-topic types ----

export type ReplaySpeed = 0.5 | 1 | 2 | 5

export interface ReplayToTopicRequest {
  targetTopic: string
  from?: string        // ISO-8601 Instant; topic replay only
  to?: string          // ISO-8601 Instant; topic replay only
  transforms?: { restamp?: boolean }
}

export interface ReplayProgress {
  status: 'in_progress' | 'completed' | 'failed'
  targetTopic: string
  sentCount: number
  errorCount: number
  currentTimestamp?: string
  errorMessage?: string
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
  updateRetention: (topic: string, retentionDays: number) =>
    request<void>(`/topics/${encodeURIComponent(topic)}/retention`, { method: 'PUT', body: JSON.stringify({ retention_days: retentionDays }) }),
  delete: (topic: string) => request<void>(`/topics/${encodeURIComponent(topic)}`, { method: 'DELETE' }),
  pause: (topic: string) => request<TopicConfig>(`/topics/${encodeURIComponent(topic)}/pause`, { method: 'POST' }),
  resume: (topic: string) => request<TopicConfig>(`/topics/${encodeURIComponent(topic)}/resume`, { method: 'POST' }),
  listMatchers: (topic: string) =>
    request<TopicMatcherConfig[]>(`/topics/${encodeURIComponent(topic)}/matchers`),
  addMatcher: (topic: string, body: AddMatcherRequest) =>
    request<TopicMatcherConfig>(`/topics/${encodeURIComponent(topic)}/matchers`, { method: 'POST', body: JSON.stringify(body) }),
  deleteMatcher: (topic: string, messageType: string) =>
    request<void>(`/topics/${encodeURIComponent(topic)}/matchers/${encodeURIComponent(messageType)}`, { method: 'DELETE' }),
}

// ---- Entities ----

export const entitiesApi = {
  list: () => request<EntityTypeConfig[]>('/entities'),
  create: (body: CreateEntityRequest) => request<EntityTypeConfig>('/entities', { method: 'POST', body: JSON.stringify(body) }),
  get: (type: string) => request<EntityTypeConfig>(`/entities/${encodeURIComponent(type)}`),
  update: (type: string, body: UpdateEntityRequest) =>
    request<EntityTypeConfig>(`/entities/${encodeURIComponent(type)}`, { method: 'PUT', body: JSON.stringify(body) }),
  updateRetention: (type: string, retentionDays: number) =>
    request<void>(`/entities/${encodeURIComponent(type)}/retention`, { method: 'PUT', body: JSON.stringify({ retention_days: retentionDays }) }),
  delete: (type: string) => request<void>(`/entities/${encodeURIComponent(type)}`, { method: 'DELETE' }),
  getSources: (type: string) => request<EntitySourceConfig[]>(`/entities/${encodeURIComponent(type)}/sources`),
  addSource: (type: string, body: AddSourceRequest) =>
    request<EntitySourceConfig>(`/entities/${encodeURIComponent(type)}/sources`, { method: 'POST', body: JSON.stringify(body) }),
  deleteSource: (type: string, topic: string) =>
    request<void>(`/entities/${encodeURIComponent(type)}/sources/${encodeURIComponent(topic)}`, { method: 'DELETE' }),
  addMatcher: (type: string, topic: string, body: AddMatcherRequest) =>
    request<MatcherConfig>(`/entities/${encodeURIComponent(type)}/sources/${encodeURIComponent(topic)}/matchers`, { method: 'POST', body: JSON.stringify(body) }),
  deleteMatcher: (type: string, topic: string, messageType: string) =>
    request<void>(`/entities/${encodeURIComponent(type)}/sources/${encodeURIComponent(topic)}/matchers/${encodeURIComponent(messageType)}`, { method: 'DELETE' }),
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
  truncateTopic: (topic: string, before: string) =>
    request<{ deleted: number }>(`/cassettes/topics/${encodeURIComponent(topic)}/truncate`, { method: 'POST', body: JSON.stringify({ before }) }),
  truncateEntityType: (type: string, before: string) =>
    request<{ deleted: number }>(`/cassettes/entities/${encodeURIComponent(type)}/truncate`, { method: 'POST', body: JSON.stringify({ before }) }),
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
  /** Export catalog snapshot directly to the configured object-storage bucket. */
  exportSnapshotToObjectStore: (body?: { name?: string }) =>
    request<SnapshotInfo>('/cassettes/snapshots/export-to-object-store', {
      method: 'POST',
      body: JSON.stringify(body ?? {}),
    }),
  /** Rebuild the known_entities registry by scanning all entity cassette tables. */
  rebuildKnownEntities: () =>
    request<{ rebuilt: number }>('/cassettes/entities/rebuild-known-entities', { method: 'POST' }),
  /** Dry-run: apply a transform pipeline to the first N records and return before/after pairs. */
  previewTransforms: (req: PreviewTransformsRequest) => {
    const { mode, topic, entityType, entityId, steps, limit = 5 } = req
    if (mode === 'topic' && topic) {
      return request<PreviewTransformsResponse[]>(
        `/cassettes/topics/${encodeURIComponent(topic)}/replay`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
          body: JSON.stringify({ transform: steps, limit }),
        },
      ).then(paged => {
        // The POST /replay endpoint returns a PagedResponse; adapt to PreviewTransformsResponse[]
        const p = paged as unknown as { data: CassetteRecord[] }
        return (p.data ?? []).map(r => ({ original: r, transformed: r })) as PreviewTransformsResponse[]
      })
    }
    if (mode === 'entity' && entityType && entityId) {
      return request<PreviewTransformsResponse[]>(
        `/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}/replay`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
          body: JSON.stringify({ transform: steps, limit }),
        },
      ).then(paged => {
        const p = paged as unknown as { data: EntityRecord[] }
        return (p.data ?? []).map(r => ({ original: r, transformed: r })) as PreviewTransformsResponse[]
      })
    }
    return Promise.reject(new Error('Invalid mode or missing topic/entity params'))
  },
}

// ---- Streaming ----

export type StreamMode = 'json' | 'sse' | 'ndjson'

export type TopicStreamParams = {
  from?: string
  to?: string
  partition?: number
  offset_from?: number
  offset_to?: number
}

export type EntityStreamParams = {
  from?: string
  to?: string
}

async function streamLines(
  url: string,
  accept: string,
  onLine: (line: string) => void,
  onDone: () => void,
  onError: (err: Error) => void,
  signal: AbortSignal,
  postBody?: string,
): Promise<void> {
  let res: Response
  try {
    res = await fetch(url, postBody != null
      ? { method: 'POST', headers: { Accept: accept, 'Content-Type': 'application/json' }, body: postBody, signal }
      : { headers: { Accept: accept }, signal }
    )
  } catch (e) {
    if ((e as Error).name === 'AbortError') return
    onError(e as Error)
    return
  }
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    onError(new Error(`HTTP ${res.status}: ${text}`))
    return
  }
  const reader = res.body!.getReader()
  const decoder = new TextDecoder()
  let buf = ''
  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      buf += decoder.decode(value, { stream: true })
      const lines = buf.split('\n')
      buf = lines.pop() ?? ''
      for (const line of lines) onLine(line)
    }
    if (buf.trim()) onLine(buf)
    onDone()
  } catch (e) {
    if ((e as Error).name === 'AbortError') return
    onError(e as Error)
  }
}

function extractData(line: string, isSse: boolean): string | null {
  if (isSse) return line.startsWith('data:') ? line.slice(5).trim() : null
  const t = line.trim()
  return t || null
}

export function streamTopicRecords(
  topic: string,
  mode: 'sse' | 'ndjson',
  params: TopicStreamParams,
  callbacks: { onRecord: (r: CassetteRecord) => void; onDone: () => void; onError: (e: Error) => void },
): AbortController {
  const url = `${API_BASE}/cassettes/topics/${encodeURIComponent(topic)}${buildQuery(params)}`
  const accept = mode === 'sse' ? 'text/event-stream' : 'application/x-ndjson'
  const isSse = mode === 'sse'
  const ctrl = new AbortController()
  void streamLines(url, accept, (line) => {
    const data = extractData(line, isSse)
    if (!data) return
    try { callbacks.onRecord(JSON.parse(data) as CassetteRecord) } catch { /* skip malformed line */ }
  }, callbacks.onDone, callbacks.onError, ctrl.signal)
  return ctrl
}

export function streamEntityRecords(
  entityType: string,
  entityId: string,
  mode: 'sse' | 'ndjson',
  params: EntityStreamParams,
  callbacks: { onRecord: (r: EntityRecord) => void; onDone: () => void; onError: (e: Error) => void },
): AbortController {
  const url = `${API_BASE}/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}${buildQuery(params)}`
  const accept = mode === 'sse' ? 'text/event-stream' : 'application/x-ndjson'
  const isSse = mode === 'sse'
  const ctrl = new AbortController()
  void streamLines(url, accept, (line) => {
    const data = extractData(line, isSse)
    if (!data) return
    try { callbacks.onRecord(JSON.parse(data) as EntityRecord) } catch { /* skip malformed line */ }
  }, callbacks.onDone, callbacks.onError, ctrl.signal)
  return ctrl
}

/** Stream topic records via POST with a full transform pipeline (SSE). */
export function streamTopicRecordsWithTransform(
  topic: string,
  steps: import('../transforms/types').TransformStep[],
  params: TopicStreamParams,
  callbacks: { onRecord: (r: CassetteRecord) => void; onDone: () => void; onError: (e: Error) => void },
): AbortController {
  const url = `${API_BASE}/cassettes/topics/${encodeURIComponent(topic)}/replay`
  const body = JSON.stringify({ ...params, transform: steps })
  const ctrl = new AbortController()
  void streamLines(url, 'text/event-stream', (line) => {
    const data = extractData(line, true)
    if (!data) return
    // Skip the transform preamble event (event: transform)
    try {
      const parsed = JSON.parse(data)
      if (parsed && !Array.isArray(parsed) && typeof parsed.topic === 'string') {
        callbacks.onRecord(parsed as CassetteRecord)
      }
    } catch { /* skip malformed */ }
  }, callbacks.onDone, callbacks.onError, ctrl.signal, body)
  return ctrl
}

/** Stream entity records via POST with a full transform pipeline (SSE). */
export function streamEntityRecordsWithTransform(
  entityType: string,
  entityId: string,
  steps: import('../transforms/types').TransformStep[],
  params: EntityStreamParams,
  callbacks: { onRecord: (r: EntityRecord) => void; onDone: () => void; onError: (e: Error) => void },
): AbortController {
  const url = `${API_BASE}/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}/replay`
  const body = JSON.stringify({ ...params, transform: steps })
  const ctrl = new AbortController()
  void streamLines(url, 'text/event-stream', (line) => {
    const data = extractData(line, true)
    if (!data) return
    try {
      const parsed = JSON.parse(data)
      if (parsed && typeof parsed.entityId === 'string') {
        callbacks.onRecord(parsed as EntityRecord)
      }
    } catch { /* skip malformed */ }
  }, callbacks.onDone, callbacks.onError, ctrl.signal, body)
  return ctrl
}

export function streamTopicReplay(
  topic: string,
  speed: ReplaySpeed,
  body: ReplayToTopicRequest,
  callbacks: { onProgress: (p: ReplayProgress) => void; onDone: () => void; onError: (e: Error) => void },
): AbortController {
  const url = `${API_BASE}/cassettes/topics/${encodeURIComponent(topic)}/replay-to-topic?speed=${speed}`
  const ctrl = new AbortController()
  void streamLines(url, 'text/event-stream', (line) => {
    const data = extractData(line, true)
    if (!data) return
    try { callbacks.onProgress(JSON.parse(data) as ReplayProgress) } catch { /* skip malformed */ }
  }, callbacks.onDone, callbacks.onError, ctrl.signal, JSON.stringify(body))
  return ctrl
}

export function streamEntityReplay(
  entityType: string,
  entityId: string,
  speed: ReplaySpeed,
  body: ReplayToTopicRequest,
  callbacks: { onProgress: (p: ReplayProgress) => void; onDone: () => void; onError: (e: Error) => void },
): AbortController {
  const url = `${API_BASE}/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}/replay-to-topic?speed=${speed}`
  const ctrl = new AbortController()
  void streamLines(url, 'text/event-stream', (line) => {
    const data = extractData(line, true)
    if (!data) return
    try { callbacks.onProgress(JSON.parse(data) as ReplayProgress) } catch { /* skip malformed */ }
  }, callbacks.onDone, callbacks.onError, ctrl.signal, JSON.stringify(body))
  return ctrl
}

// ---- Transform Presets ----

export const transformsApi = {
  list: () => request<TransformPreset[]>('/transforms'),
  get: (name: string) => request<TransformPreset>(`/transforms/${encodeURIComponent(name)}`),
  create: (body: CreatePresetRequest) =>
    request<TransformPreset>('/transforms', { method: 'POST', body: JSON.stringify(body) }),
  update: (name: string, body: UpdatePresetRequest) =>
    request<TransformPreset>(`/transforms/${encodeURIComponent(name)}`, { method: 'PUT', body: JSON.stringify(body) }),
  delete: (name: string) =>
    request<void>(`/transforms/${encodeURIComponent(name)}`, { method: 'DELETE' }),
}

// ---- Compaction ----

export const compactionApi = {
  getStatus: () => request<CompactionStatus>('/compaction/status'),
  getConfig: () => request<CompactionConfig>('/compaction/config'),
  trigger: (body?: TriggerRequest) =>
    request<CompactionRun>('/compaction/trigger', { method: 'POST', body: JSON.stringify(body ?? {}) }),
  getHistory: (limit?: number) =>
    request<CompactionRun[]>(`/compaction/history${limit != null ? `?limit=${limit}` : ''}`),
}

// ---- Health ----

export const healthApi = {
  get: () => request<HealthStatus>('/health'),
}

// ---- Runtime config ----

export interface DeploymentConfig {
  kafkaBootstrapServers: string
  catalogPath: string
  objectStoragePath: string | null
  s3Endpoint: string
  s3Region: string
  inlineThresholdMb: number
  inlineThresholdRecords: number
  compactionSchedule: string
  retentionSchedule: string
  recordingBatchSize: number
  recordingBatchTimeoutMs: number
  compactionEntityMinFilesPerBucket: number
  compactionEntityTargetFileSizeMb: number
  compactionEntityLookbackDays: number
  compactionGeneralEnabled: boolean
  compactionGeneralMinFilesPerPartition: number
  compactionGeneralTargetFileSizeMb: number
}

export interface DomainSummary {
  topicCount: number
  entityTypeCount: number
  sourceMappingCount: number
}

export interface RuntimeConfig {
  deployment: DeploymentConfig
  domain: DomainSummary
}

export const configApi = {
  getRuntime: () => request<RuntimeConfig>('/config/runtime'),
}

// ---- Brokers ----

export interface BrokerConfig {
  brokerId: string
  bootstrapServers: string
  securityProtocol: string
  saslMechanism: string | null
  saslUsername: string | null
  saslPassword: string | null
  sslTruststorePath: string | null
  sslTruststorePassword: string | null
  sslKeystorePath: string | null
  sslKeystorePassword: string | null
}

export interface CreateBrokerRequest {
  brokerId: string
  bootstrapServers: string
  securityProtocol?: string
  saslMechanism?: string
  saslUsername?: string
  saslPassword?: string
  sslTruststorePath?: string
  sslTruststorePassword?: string
  sslKeystorePath?: string
  sslKeystorePassword?: string
}

export interface UpdateBrokerRequest {
  bootstrapServers: string
  securityProtocol?: string
  saslMechanism?: string
  saslUsername?: string
  saslPassword?: string
  sslTruststorePath?: string
  sslTruststorePassword?: string
  sslKeystorePath?: string
  sslKeystorePassword?: string
}

export interface BrokerTopicInfo {
  topicName: string
  partitionCount: number
  isRecorded: boolean
  recordingMode: string | null
}

export const brokersApi = {
  list: () => request<BrokerConfig[]>('/brokers'),
  create: (body: CreateBrokerRequest) =>
    request<BrokerConfig>('/brokers', { method: 'POST', body: JSON.stringify(body) }),
  get: (brokerId: string) =>
    request<BrokerConfig>(`/brokers/${encodeURIComponent(brokerId)}`),
  update: (brokerId: string, body: UpdateBrokerRequest) =>
    request<BrokerConfig>(`/brokers/${encodeURIComponent(brokerId)}`, {
      method: 'PUT',
      body: JSON.stringify(body),
    }),
  delete: (brokerId: string) =>
    request<void>(`/brokers/${encodeURIComponent(brokerId)}`, { method: 'DELETE' }),
  peekMessages: (brokerId: string, topic: string, limit?: number) =>
    request<PeekMessage[]>(
      `/brokers/${encodeURIComponent(brokerId)}/topics/${encodeURIComponent(topic)}/peek${buildQuery({ limit })}`,
    ),
  listTopics: (brokerId: string, params?: { includeInternal?: boolean; filter?: string }) =>
    request<BrokerTopicInfo[]>(
      `/brokers/${encodeURIComponent(brokerId)}/topics${buildQuery(params ?? {})}`,
    ),
}

// ---- Matchers ----

export interface MatcherPreviewRequest {
  key: string | null
  value: string | null
  headers: Header[]
  idSource: string
  idExpression: string
}

export interface MatcherPreviewResponse {
  matched: boolean
  entityId: string | null
  error: string | null
}

export const matchersApi = {
  preview: (body: MatcherPreviewRequest) =>
    request<MatcherPreviewResponse>('/matchers/preview', {
      method: 'POST',
      body: JSON.stringify(body),
    }),
}

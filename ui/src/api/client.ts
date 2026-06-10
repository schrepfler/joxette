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
  createKafkaTopicIfAbsent?: boolean
  numPartitions?: number
  replicationFactor?: number
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
  messageType: string | null
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

export interface SolTagSpan {
  from: number
  to: number
}

export interface SolMatchResponse {
  records: EntityRecord[]
  matched: boolean
  unexpectedNulls: string[]
  tags: Record<string, SolTagSpan>
  sequenceLength: number
}

/** One row of the SOL sequence model: a tag (or unnamed gap) and its hit count. */
export interface SolModelRow {
  label: string | null
  gap: boolean
  count: number
}

/** One example sequence from the batch SOL matcher. Spans index into `events`. */
export interface SolSequenceExample {
  entityId: string
  events: string[]
  tags: Record<string, SolTagSpan>
  matched: boolean
  truncated: boolean
}

/** Aggregate result of POST /cassettes/entities/{type}/sol-examples. */
export interface SolBatchResult {
  totalSequences: number
  matchedSequences: number
  model: SolModelRow[]
  examples: SolSequenceExample[]
}

export interface EntityInfo {
  entityType: string
  entityId: string
  firstSeen: string
  lastSeen: string
  messageCount: number
  sourceTopics: string[]
  lastMessageType: string | null
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

// ---- Entity output modes ----

export interface StateResult {
  state: Record<string, unknown> | null
  asOf: string | null
  eventCount: number
}

export interface DiffRecord {
  event: EntityRecord
  changedFields: string[] | null
  before: Record<string, unknown> | null
}

export interface PortraitResult {
  entityId: string
  entityType: string
  eventCount: number
  firstSeen: string | null
  lastSeen: string | null
  topicBreakdown: Record<string, number>
  recentEvents: EntityRecord[]
  currentState: Record<string, unknown> | null
}

// ---- Named Derived Streams ----

export interface StreamSourceOptions {
  messageTypes?: string[]
  from?: string
  to?: string
  lastN?: number
  dedup?: 'offset' | 'value' | 'none'
}

export interface StreamDefinition {
  id: string
  name: string
  entityType: string
  entityId?: string | null
  source?: StreamSourceOptions | null
  sol?: string | null
  solOutput?: 'events' | 'annotated' | 'summary'
  output?: 'events' | 'state' | 'diff' | 'snapshot'
  stateFold?: 'merge_patch' | 'last_value' | 'last_per_topic' | null
  responseFormat?: 'events' | 'timeline' | 'portrait'
  createdAt: string
  updatedAt: string
}

export interface CreateStreamRequest {
  id: string
  name: string
  entityType: string
  entityId?: string | null
  source?: StreamSourceOptions | null
  sol?: string | null
  solOutput?: string
  output?: string
  stateFold?: string | null
  responseFormat?: string
}

export type UpdateStreamRequest = Omit<CreateStreamRequest, 'id'>

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

export type PartitionStrategy = 'DEFAULT' | 'PRESERVE' | 'MODULO'

export interface ReplayToTopicRequest {
  targetTopic?: string
  from?: string        // ISO-8601 Instant; topic replay only
  to?: string          // ISO-8601 Instant; topic replay only
  transforms?: { restamp?: boolean }
  /** Per-source-topic routing overrides (entity replay). */
  topicMappings?: Record<string, string>
  /** Partition routing strategy. Defaults to DEFAULT on the server. */
  partitionStrategy?: PartitionStrategy
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

/** Sort direction for paged and streaming cassette replay. Default 'asc' on the backend. */
export type Order = 'asc' | 'desc'

export type CassetteTopicParams = QueryParams & {
  from?: string
  to?: string
  partition?: number
  offset_from?: number
  offset_to?: number
  limit?: number
  cursor?: string
  order?: Order
}

export type EntitySortBy = 'id' | 'lastActive' | 'mostMessages'

export type EntityListParams = QueryParams & {
  limit?: number
  cursor?: string
  sortBy?: EntitySortBy
}

export type EntitySearchParams = QueryParams & {
  q?: string
  limit?: number
  cursor?: string
  sortBy?: EntitySortBy
}

export type EntityRecordsParams = QueryParams & {
  from?: string
  to?: string
  limit?: number
  cursor?: string
  order?: Order
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

  getTopicFields: (topic: string, limit = 500) =>
    request<{ fields: string[] }>(`/cassettes/topics/${encodeURIComponent(topic)}/fields?limit=${limit}`)
      .then(r => r.fields),

  getEntityFields: (entityType: string, limit = 500) =>
    request<{ fields: string[] }>(`/cassettes/entities/${encodeURIComponent(entityType)}/fields?limit=${limit}`)
      .then(r => r.fields),

  getTopicMessageTypes: (topic: string, limit = 500) =>
    request<{ messageTypes: string[] }>(`/cassettes/topics/${encodeURIComponent(topic)}/message-types?limit=${limit}`)
      .then(r => r.messageTypes),

  getEntityMessageTypes: (entityType: string, limit = 500) =>
    request<{ messageTypes: string[] }>(`/cassettes/entities/${encodeURIComponent(entityType)}/message-types?limit=${limit}`)
      .then(r => r.messageTypes),

  /** Find sequences matching a SequenceQuery across a topic or entity cassette. */
  matchSequences: (
    mode: 'topic' | 'entity',
    params: { topic?: string; entityType?: string },
    query: import('../transforms/types').SequenceQuery,
  ): Promise<import('../transforms/types').SequenceMatchResponse> => {
    if (mode === 'topic' && params.topic) {
      return request(`/cassettes/topics/${encodeURIComponent(params.topic)}/match-sequences`, {
        method: 'POST',
        body: JSON.stringify(query),
      })
    }
    if (mode === 'entity' && params.entityType) {
      return request(`/cassettes/entities/${encodeURIComponent(params.entityType)}/match-sequences`, {
        method: 'POST',
        body: JSON.stringify(query),
      })
    }
    return Promise.reject(new Error('Invalid mode or missing topic/entityType params'))
  },

  /** Build a sunburst prefix-tree hierarchy for all sequences of an entity type. */
  buildSunburst: (
    entityType: string,
    req: { from?: string; to?: string; maxSteps?: number; minAngleDeg?: number; maxEntities?: number; solQuery?: string },
  ) =>
    request<import('../components/SunburstChart').SunburstData>(
      `/cassettes/entities/${encodeURIComponent(entityType)}/sunburst`,
      { method: 'POST', body: JSON.stringify(req) },
    ),

  /** Run a SOL query against a single entity's event sequence. */
  solMatchEntity: (
    entityType: string,
    entityId: string,
    query: string,
    from?: string,
    to?: string,
  ) =>
    request<SolMatchResponse>(
      `/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}/sol-match`,
      { method: 'POST', body: JSON.stringify({ query, from, to }) },
    ),

  /** Run a SOL query against a topic cassette (flat sequence). */
  solMatchTopic: (
    topic: string,
    query: string,
    from?: string,
    to?: string,
    typeField?: string,
  ) =>
    request<SolMatchResponse>(
      `/cassettes/topics/${encodeURIComponent(topic)}/sol-match`,
      { method: 'POST', body: JSON.stringify({ query, from, to, typeField: typeField || undefined }) },
    ),

  /** Run a SOL query across many entity sequences — examples-pane data. */
  solExamples: (
    entityType: string,
    query: string,
    opts?: { from?: string; to?: string; maxSequences?: number; exampleLimit?: number },
  ) =>
    request<SolBatchResult>(
      `/cassettes/entities/${encodeURIComponent(entityType)}/sol-examples`,
      { method: 'POST', body: JSON.stringify({ query, ...opts }) },
    ),

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

// ---- Entity output-mode helpers ----

export const entityOutputApi = {
  getState: (entityType: string, entityId: string, params?: { from?: string; to?: string; stateFold?: string }) =>
    request<StateResult>(
      `/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}/replay`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ output: 'state', stateFold: params?.stateFold ?? 'merge_patch', from: params?.from, to: params?.to }),
      },
    ),

  getDiff: (entityType: string, entityId: string, params?: { from?: string; to?: string }) =>
    request<PagedResponse<DiffRecord>>(
      `/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}/replay`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ output: 'diff', ...params }),
      },
    ).then(r => r.data),

  getPortrait: (entityType: string, entityId: string, params?: { from?: string; to?: string }) =>
    request<PortraitResult>(
      `/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}/replay`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ responseFormat: 'portrait', ...params }),
      },
    ),
}

// ---- Named Derived Streams API ----

export const streamsApi = {
  list: (entityType?: string) =>
    request<StreamDefinition[]>(`/streams${entityType ? `?entity_type=${encodeURIComponent(entityType)}` : ''}`),
  get: (id: string) =>
    request<StreamDefinition>(`/streams/${encodeURIComponent(id)}`),
  create: (req: CreateStreamRequest) =>
    request<StreamDefinition>('/streams', { method: 'POST', body: JSON.stringify(req) }),
  update: (id: string, req: UpdateStreamRequest) =>
    request<StreamDefinition>(`/streams/${encodeURIComponent(id)}`, { method: 'PUT', body: JSON.stringify(req) }),
  delete: (id: string) =>
    request<void>(`/streams/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  /** Pull query: evaluates the stream against the given entityId and returns paged records/state/etc. */
  pull: (id: string, entityId?: string) =>
    request<PagedResponse<EntityRecord>>(
      `/streams/${encodeURIComponent(id)}${entityId ? `?entity_id=${encodeURIComponent(entityId)}` : ''}`
    ),
}

// ---- Field suggestions ----

export type FieldContext = { mode: 'topic' | 'entity'; topic?: string; entityType?: string }

export function fetchFieldSuggestions(ctx: FieldContext): Promise<string[]> {
  if (ctx.mode === 'topic' && ctx.topic) return cassettesApi.getTopicFields(ctx.topic)
  if (ctx.mode === 'entity' && ctx.entityType) return cassettesApi.getEntityFields(ctx.entityType)
  return Promise.reject(new Error('Invalid field context'))
}

// ---- Streaming ----

export type StreamMode = 'json' | 'sse' | 'ndjson'

export type TopicStreamParams = {
  from?: string
  to?: string
  partition?: number
  offset_from?: number
  offset_to?: number
  order?: Order
}

export type EntityStreamParams = {
  from?: string
  to?: string
  order?: Order
}

export async function streamLines(
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
      for (const raw of lines) {
        // Normalize CRLF.
        const line = raw.endsWith('\r') ? raw.slice(0, -1) : raw
        // Drop SSE comment lines (e.g. ":heartbeat") silently — these are
        // keepalives from the follow-mode backend and must never reach the
        // record parser.
        if (line.startsWith(':')) continue
        onLine(line)
      }
    }
    const tail = buf.endsWith('\r') ? buf.slice(0, -1) : buf
    if (tail && !tail.startsWith(':')) onLine(tail)
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

/** Backend-emitted status transitions for follow-mode streams. */
export type StreamStatusEvent = 'follow' | 'overflow'

export interface RecordStreamCallbacks<R> {
  onRecord: (r: R) => void
  /** Optional: receive follow-mode status transitions (follow preamble, overflow terminal). */
  onStatus?: (event: StreamStatusEvent) => void
  onDone: () => void
  onError: (e: Error) => void
}

/**
 * Build an SSE event parser. The parser consumes lines emitted by
 * {@link streamLines} (one line at a time, including blank lines as
 * SSE-block terminators) and dispatches on blank-line boundaries:
 *  - blocks whose `event:` is `message` (the SSE default) → onRecord
 *  - blocks with an explicit `event:` name → onEvent(name, data)
 *
 * Heartbeat `:`-prefixed comment lines are already dropped by streamLines.
 */
function createSseParser(
  onRecord: (json: string) => void,
  onEvent: (name: string, json: string) => void,
): (line: string) => void {
  let currentEvent: string | null = null
  const dataBuf: string[] = []
  return (line: string) => {
    // SSE block terminator (blank line, possibly trailing \r).
    if (line === '' || line === '\r') {
      if (dataBuf.length > 0) {
        const data = dataBuf.join('\n')
        if (currentEvent == null || currentEvent === 'message') onRecord(data)
        else onEvent(currentEvent, data)
      }
      currentEvent = null
      dataBuf.length = 0
      return
    }
    if (line.startsWith('event:')) {
      currentEvent = line.slice(6).trim()
    } else if (line.startsWith('data:')) {
      // Strip single leading space after `data:` per the SSE spec.
      const v = line.slice(5)
      dataBuf.push(v.startsWith(' ') ? v.slice(1) : v)
    }
    // Ignore id:, retry:, and unknown fields.
  }
}

type EventEnvelope = { event?: string }

function readEventName(evt: unknown): string | null {
  if (evt == null || typeof evt !== 'object') return null
  const e = (evt as EventEnvelope).event
  return typeof e === 'string' ? e : null
}

/** Build an NDJSON line handler that distinguishes records from status events. */
function createNdjsonHandler<R>(
  onRecord: (r: R) => void,
  onStatus: ((e: StreamStatusEvent) => void) | undefined,
): (line: string) => void {
  return (line: string) => {
    const data = extractData(line, false)
    if (!data) return
    let parsed: unknown
    try { parsed = JSON.parse(data) } catch { return }
    const eventName = readEventName(parsed)
    if (eventName != null) {
      // Any envelope with a top-level `event` is a backend status line.
      // Surface follow/overflow; silently drop heartbeats and unknowns.
      if (eventName === 'follow' || eventName === 'overflow') onStatus?.(eventName)
      return
    }
    onRecord(parsed as R)
  }
}

export function streamTopicRecords(
  topic: string,
  mode: 'sse' | 'ndjson',
  params: TopicStreamParams & { follow?: boolean },
  callbacks: RecordStreamCallbacks<CassetteRecord>,
): AbortController {
  const { follow, ...rest } = params
  const query = buildQuery(follow ? { ...rest, follow: 'true' } : rest)
  const url = `${API_BASE}/cassettes/topics/${encodeURIComponent(topic)}${query}`
  const accept = mode === 'sse' ? 'text/event-stream' : 'application/x-ndjson'
  const ctrl = new AbortController()
  const onLine = mode === 'sse'
    ? createSseParser(
        (data) => { try { callbacks.onRecord(JSON.parse(data) as CassetteRecord) } catch { /* skip */ } },
        (name) => {
          if (name === 'follow' || name === 'overflow') callbacks.onStatus?.(name)
        },
      )
    : createNdjsonHandler<CassetteRecord>(callbacks.onRecord, callbacks.onStatus)
  void streamLines(url, accept, onLine, callbacks.onDone, callbacks.onError, ctrl.signal)
  return ctrl
}

export function streamEntityRecords(
  entityType: string,
  entityId: string,
  mode: 'sse' | 'ndjson',
  params: EntityStreamParams & { follow?: boolean },
  callbacks: RecordStreamCallbacks<EntityRecord>,
): AbortController {
  const { follow, ...rest } = params
  const query = buildQuery(follow ? { ...rest, follow: 'true' } : rest)
  const url = `${API_BASE}/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}${query}`
  const accept = mode === 'sse' ? 'text/event-stream' : 'application/x-ndjson'
  const ctrl = new AbortController()
  const onLine = mode === 'sse'
    ? createSseParser(
        (data) => { try { callbacks.onRecord(JSON.parse(data) as EntityRecord) } catch { /* skip */ } },
        (name) => {
          if (name === 'follow' || name === 'overflow') callbacks.onStatus?.(name)
        },
      )
    : createNdjsonHandler<EntityRecord>(callbacks.onRecord, callbacks.onStatus)
  void streamLines(url, accept, onLine, callbacks.onDone, callbacks.onError, ctrl.signal)
  return ctrl
}

// Exported for unit tests only.
export const __testables = { createSseParser, createNdjsonHandler }

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
  startDelayMs?: number,
): AbortController {
  const qs = new URLSearchParams({ speed: String(speed) })
  if (startDelayMs != null) qs.set('start_delay_ms', String(startDelayMs))
  const url = `${API_BASE}/cassettes/topics/${encodeURIComponent(topic)}/replay-to-topic?${qs}`
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
  startDelayMs?: number,
): AbortController {
  const qs = new URLSearchParams({ speed: String(speed) })
  if (startDelayMs != null) qs.set('start_delay_ms', String(startDelayMs))
  const url = `${API_BASE}/cassettes/entities/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}/replay-to-topic?${qs}`
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

// ---- Retention ----

export interface RetentionRun {
  id: number
  startedAt: string
  completedAt: string | null
  status: 'running' | 'completed' | 'failed'
  triggeredBy: string
  entityRowsDeleted: number
  generalRowsDeleted: number
  knownEntitiesDeleted: number
  errorMessage: string | null
}

export interface RetentionStatus {
  lastRun: RetentionRun | null
  nextScheduledRun: string | null
  running: boolean
}

export const retentionApi = {
  getStatus: () => request<RetentionStatus>('/compaction/retention-status'),
  getHistory: (limit?: number) =>
    request<RetentionRun[]>(`/compaction/retention-history${limit != null ? `?limit=${limit}` : ''}`),
  trigger: () =>
    request<RetentionRun>('/compaction/trigger-retention', { method: 'POST' }),
}

// ---- Health ----

export const healthApi = {
  get: () => request<HealthStatus>('/health'),
  getMetricsText: (): Promise<string> =>
    fetch(`${API_BASE}/metrics`, { headers: { Accept: 'text/plain' } }).then(r => r.text()),
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

// ---- Cluster / instances ----

export interface RecorderStatus {
  topic: string
  running: boolean
  startedAt: string
  lastBatchAt: string | null
  consumerLag: number
  lastError: string | null
  protocol: string
  assignedPartitions: number[]
  messagesConsumed: number
  messagesWritten: number
}

export interface InstanceRecord {
  instanceId: string
  recordingEnabled: boolean
  compactionEnabled: boolean
  catalogBackend: string
  startedAt: string
  lastHeartbeat: string
  kafkaAssignments: Record<string, number[]>
  status: string
}

export interface MemberView {
  address: string
  status: string
  reachable: boolean
  roles: string[]
}

export interface ActiveReplay {
  id: string
  sourceTopic: string
  targetTopic: string
  startedAt: string
  sentCount: number
  status: 'running' | 'completed' | 'failed'
}

export interface ClusterStateView {
  self: {
    instanceId: string
    recordingEnabled: boolean
    compactionEnabled: boolean
    catalogBackend: string
    startedAt: string | null
    lastHeartbeat: string | null
    heartbeatStatus: string
    pekkoAddress: string | null
    pekkoStatus: string | null
    pekkoReachable: boolean
    recorders: Record<string, RecorderStatus>
  }
  instances: InstanceRecord[]
  topology: MemberView[]
  activeReplays: ActiveReplay[]
}

export const instancesApi = {
  clusterState: () => request<ClusterStateView>('/instances/cluster-state'),
  liveMetricsUrl: () => `${API_BASE}/instances/live-metrics`,
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
  listTopics: (brokerId: string, params?: { includeInternal?: string; filter?: string }) =>
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

// ---- Catalog SQL console ----

export interface ColumnMeta {
  name: string
  typeName: string
}

export interface CatalogQueryResponse {
  columns: ColumnMeta[]
  rows: unknown[][]
  rowCount: number
  affectedRows: number
  truncated: boolean
  durationMs: number
  isQuery: boolean
}

export const catalogApi = {
  query: (sql: string, maxRows = 10_000) =>
    request<CatalogQueryResponse>('/catalog/query', {
      method: 'POST',
      body: JSON.stringify({ sql, maxRows }),
    }),
  tables: () =>
    catalogApi.query('SHOW ALL TABLES', 10_000),
}

import { useState, useRef, useEffect } from 'react'
import {
  streamTopicReplay,
  streamEntityReplay,
  type ReplaySpeed,
  type ReplayToTopicRequest,
  type ReplayProgress,
} from '../api/client'
import { TransformPipelineBuilder, emptyPipeline, serializeSteps } from './transforms/TransformPipelineBuilder'
import type { TransformPipeline } from '../transforms/types'

interface ReplayToTopicPanelProps {
  mode: 'topic' | 'entity'
  topic?: string
  entityType?: string
  entityId?: string
  /** Debounced from-filter value (topic mode only) — passed through to the request */
  from?: string
  /** Debounced to-filter value (topic mode only) — passed through to the request */
  to?: string
  /** stats.rowCount (topic) or stats.messageCount (entity) — progress bar denominator */
  totalCount?: number
}

const SPEEDS: ReplaySpeed[] = [0.5, 1, 2, 5]

type ReplayState = 'idle' | 'running' | 'done' | 'failed'

export function ReplayToTopicPanel({ mode, topic, entityType, entityId, from, to, totalCount }: ReplayToTopicPanelProps) {
  const [targetTopic, setTargetTopic] = useState('')
  const [speed, setSpeed] = useState<ReplaySpeed>(1)
  const [pipeline, setPipeline] = useState<TransformPipeline>(emptyPipeline)
  const [replayState, setReplayState] = useState<ReplayState>('idle')
  const [progress, setProgress] = useState<ReplayProgress | null>(null)
  const [errorMsg, setErrorMsg] = useState<string | null>(null)
  const [startDelayMs, setStartDelayMs] = useState('')
  const abortRef = useRef<AbortController | null>(null)

  useEffect(() => () => { abortRef.current?.abort() }, [])

  function startReplay() {
    if (!targetTopic.trim()) return
    abortRef.current?.abort()
    setReplayState('running')
    setProgress(null)
    setErrorMsg(null)

    // Merge pipeline into transforms field.
    // wall_time step → restamp: true for backward compat with replay-to-topic endpoint.
    const steps = serializeSteps(pipeline.steps)
    const hasWallTime = steps.some(s => s.type === 'wall_time')
    const transforms = steps.length > 0
      ? { restamp: hasWallTime }
      : undefined

    const body: ReplayToTopicRequest = {
      targetTopic: targetTopic.trim(),
      from: from || undefined,
      to: to || undefined,
      transforms,
    }

    const cbs = {
      onProgress: (p: ReplayProgress) => {
        setProgress(p)
        if (p.status === 'completed') setReplayState('done')
        if (p.status === 'failed') {
          setErrorMsg(p.errorMessage ?? 'Replay failed')
          setReplayState('failed')
        }
      },
      onDone: () => setReplayState(s => s === 'running' ? 'done' : s),
      onError: (e: Error) => { setErrorMsg(e.message); setReplayState('failed') },
    }

    const delayMs = startDelayMs.trim() ? Number(startDelayMs) : undefined
    abortRef.current = mode === 'topic'
      ? streamTopicReplay(topic!, speed, body, cbs, delayMs)
      : streamEntityReplay(entityType!, entityId!, speed, body, cbs, delayMs)
  }

  function stopReplay() {
    abortRef.current?.abort()
    abortRef.current = null
    setReplayState('idle')
  }

  function reset() {
    setReplayState('idle')
    setProgress(null)
    setErrorMsg(null)
  }

  const sent = progress?.sentCount ?? 0
  const total = totalCount ?? 0
  const pct = total > 0 ? Math.min(100, Math.round((sent / total) * 100)) : null

  const statusColor =
    replayState === 'done'   ? '#276749' :
    replayState === 'failed' ? '#e53e3e' :
    '#718096'

  const statusBg =
    replayState === 'done'   ? '#f0fff4' :
    replayState === 'failed' ? '#fff5f5' :
    undefined

  const isRunning = replayState === 'running'
  const stepCount = pipeline.steps.length

  return (
    <div style={{ background: statusBg ?? '#fff', border: `1px solid ${replayState === 'done' ? '#9ae6b4' : replayState === 'failed' ? '#feb2b2' : '#e2e8f0'}`, borderRadius: 8, padding: '1rem 1.25rem', marginBottom: '1.5rem', transition: 'background 0.3s, border-color 0.3s' }}>
      <h3 style={{ margin: '0 0 0.75rem', fontSize: 15 }}>Replay to Topic</h3>

      {/* Transform pipeline builder */}
      <TransformPipelineBuilder
        pipeline={pipeline}
        onChange={setPipeline}
        mode={mode}
        topic={topic}
        entityType={entityType}
        entityId={entityId}
        disabled={isRunning}
      />

      {/* Controls row */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', alignItems: 'flex-end', marginBottom: replayState !== 'idle' || progress !== null ? '0.75rem' : 0 }}>

        {/* Target topic input */}
        <div>
          <label style={labelStyle}>Target Topic</label>
          <input
            type="text"
            placeholder="my-target-topic"
            style={{ ...inputStyle, width: 240 }}
            value={targetTopic}
            onChange={e => setTargetTopic(e.target.value)}
            disabled={isRunning}
          />
        </div>

        {/* Start delay */}
        <div>
          <label style={labelStyle}>Start delay (ms)</label>
          <input
            type="number"
            min="0"
            placeholder="0 = immediate"
            style={{ ...inputStyle, width: 150 }}
            value={startDelayMs}
            onChange={e => setStartDelayMs(e.target.value)}
            disabled={isRunning}
          />
        </div>

        {/* Speed toggle group */}
        <div>
          <label style={labelStyle}>Speed</label>
          <div style={{ display: 'flex', border: '1px solid #cbd5e0', borderRadius: 4, overflow: 'hidden' }}>
            {SPEEDS.map((s, i) => (
              <button
                key={s}
                onClick={() => setSpeed(s)}
                disabled={isRunning}
                style={{
                  padding: '0.3rem 0.65rem',
                  background: speed === s ? '#3182ce' : '#fff',
                  color: speed === s ? '#fff' : '#4a5568',
                  border: 'none',
                  borderRight: i < SPEEDS.length - 1 ? '1px solid #cbd5e0' : 'none',
                  cursor: isRunning ? 'not-allowed' : 'pointer',
                  fontSize: 13,
                  fontWeight: speed === s ? 600 : 400,
                }}
              >
                {s}x
              </button>
            ))}
          </div>
        </div>

        {/* Pipeline indicator */}
        {stepCount > 0 && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, paddingBottom: 3 }}>
            <span style={{
              padding: '2px 8px', borderRadius: 10, fontSize: 12,
              background: '#ebf8ff', color: '#2b6cb0', fontWeight: 600, border: '1px solid #bee3f8',
            }}>
              {stepCount} transform{stepCount !== 1 ? 's' : ''} active
            </span>
          </div>
        )}

        {/* Start / Stop button */}
        <div style={{ display: 'flex', gap: 8 }}>
          {!isRunning ? (
            <button
              style={{
                ...primaryBtnStyle,
                background: targetTopic.trim() ? '#3182ce' : '#a0aec0',
                cursor: targetTopic.trim() ? 'pointer' : 'not-allowed',
              }}
              onClick={startReplay}
              disabled={!targetTopic.trim()}
            >
              Start Replay
            </button>
          ) : (
            <button style={{ ...primaryBtnStyle, background: '#e53e3e' }} onClick={stopReplay}>
              Stop
            </button>
          )}
          {(replayState === 'done' || replayState === 'failed') && (
            <button style={secondaryBtnStyle} onClick={reset}>New Replay</button>
          )}
        </div>
      </div>

      {/* Progress section */}
      {(replayState !== 'idle' || progress !== null) && (
        <div>
          {pct !== null && (
            <div style={{ marginBottom: '0.4rem' }}>
              <div style={{ height: 8, background: '#e2e8f0', borderRadius: 4, overflow: 'hidden' }}>
                <div
                  style={{
                    height: '100%',
                    width: `${pct}%`,
                    background: replayState === 'failed' ? '#e53e3e' : replayState === 'done' ? '#38a169' : '#3182ce',
                    borderRadius: 4,
                    transition: 'width 0.3s ease',
                  }}
                />
              </div>
              <div style={{ fontSize: 12, color: '#718096', marginTop: 3 }}>
                {sent.toLocaleString()} / {total.toLocaleString()} ({pct}%)
              </div>
            </div>
          )}
          <div style={{ fontSize: 13, color: statusColor, fontWeight: replayState !== 'running' ? 600 : 400 }}>
            {replayState === 'running' && (
              `\u25cf Replaying to \u201c${progress?.targetTopic ?? targetTopic.trim()}\u201d\u2026 ${sent.toLocaleString()} sent`
            )}
            {replayState === 'done' && (
              `\u2713 Completed \u2014 ${sent.toLocaleString()} messages sent to \u201c${progress?.targetTopic ?? targetTopic.trim()}\u201d${(progress?.errorCount ?? 0) > 0 ? ` (${progress!.errorCount} errors)` : ''}`
            )}
            {replayState === 'failed' && (
              `\u2717 Failed: ${errorMsg ?? 'Unknown error'}`
            )}
          </div>
        </div>
      )}
    </div>
  )
}

const labelStyle: React.CSSProperties = { display: 'block', marginBottom: 4, fontSize: 13, fontWeight: 600, color: '#4a5568' }
const inputStyle: React.CSSProperties = { padding: '0.4rem 0.6rem', border: '1px solid #cbd5e0', borderRadius: 4, fontSize: 14 }
const primaryBtnStyle: React.CSSProperties = { padding: '0.45rem 1rem', background: '#3182ce', color: '#fff', border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 14 }
const secondaryBtnStyle: React.CSSProperties = { padding: '0.35rem 0.8rem', background: '#fff', color: '#4a5568', border: '1px solid #cbd5e0', borderRadius: 4, cursor: 'pointer', fontSize: 13 }

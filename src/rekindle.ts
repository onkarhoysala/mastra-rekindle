import { resolveConfig, type RekindleOptions } from './config.js'
import { OwnershipLostError, RekindleNotStartedError } from './errors.js'
import { errorFields } from './logger.js'
import { setDefaultProvider, type OwnershipProvider } from './registry.js'
import { migrationSql } from './sql.js'
import { createOwnerStore, type OwnerStore } from './store.js'
import { createSweeper, type SweepResult } from './sweep.js'

export interface SignalHandlerOptions {
  /** Default SIGTERM and SIGINT. */
  signals?: NodeJS.Signals[]
  /**
   * After draining, stop rekindle and re-raise the signal so the process exits as it would
   * have without rekindle. Skipped when something else also listens for the signal, so your
   * own shutdown code stays in charge. Default true.
   */
  exit?: boolean
}

export interface Rekindle extends OwnershipProvider {
  readonly instanceId: string
  /** True once drain() or stop() has been called: no new runs are claimed. */
  readonly isDraining: boolean
  /** Create the schema and tables if absent. Idempotent. Or run migrationSql() in your own migrations. */
  migrate(): Promise<void>
  /**
   * Take back any runs a previous boot left under this instanceId, heartbeat now and then on an
   * interval, and sweep after the boot delay and then on an interval. Calling it again while
   * running does nothing; calling it after stop() throws.
   */
  start(): Promise<void>
  /**
   * Stop claiming new runs but keep heartbeating, so this instance's in-flight runs aren't taken
   * over while it drains. One-way. Does nothing unless start() has finished.
   */
  drain(): void
  /** Stop everything. In-flight runs can be taken over once the heartbeat goes stale. Final. */
  stop(): Promise<void>
  /** On SIGTERM/SIGINT: drain(), then by default stop and let the signal exit the process. */
  installSignalHandlers(options?: SignalHandlerOptions): void
  /** One pass over the opted-in workflows. A call while a sweep is running joins it. */
  sweepOnce(): Promise<SweepResult>
}

/**
 * - `idle`: created, start() not called yet. Sweeps can be run by hand.
 * - `starting`: start() is in progress.
 * - `running`: heartbeating and sweeping on a timer.
 * - `draining`: heartbeating, not claiming.
 * - `stopped`: nothing runs, for good.
 */
type LifecycleState = 'idle' | 'starting' | 'running' | 'draining' | 'stopped'

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
const jitter = (max: number) => (max > 0 ? Math.floor(Math.random() * max) : 0)

export function createRekindle(options: RekindleOptions): Rekindle {
  return assembleRekindle(options).rekindle
}

/**
 * createRekindle, plus what tests drive directly: the store behind it, and a heartbeat that also
 * opens the guards' first-heartbeat wait. Not part of the package API.
 */
export function assembleRekindle(options: RekindleOptions): {
  rekindle: Rekindle
  store: OwnerStore
  heartbeat: () => Promise<void>
} {
  const config = resolveConfig(options)
  const { instanceId, logger: log } = config
  const store = createOwnerStore(config)

  // Opens on the first heartbeat. A guard that runs earlier (eg the app starts rekindle without
  // awaiting it and a run lands first) waits for it instead of failing.
  let hasHeartbeated = false
  let openFirstHeartbeat: () => void = () => {}
  const firstHeartbeat = new Promise<void>((resolve) => {
    openFirstHeartbeat = resolve
  })

  async function heartbeat(): Promise<void> {
    await store.heartbeat()
    hasHeartbeated = true
    openFirstHeartbeat()
  }

  /** True once this instance has heartbeated, waiting until `deadline` (epoch ms) at most. */
  async function waitForFirstHeartbeat(deadline: number): Promise<boolean> {
    if (hasHeartbeated) return true
    let timer: NodeJS.Timeout | undefined
    const timedOut = new Promise<false>((resolve) => {
      timer = setTimeout(() => resolve(false), Math.max(0, deadline - Date.now()))
    })
    try {
      return await Promise.race([firstHeartbeat.then(() => true as const), timedOut])
    } finally {
      clearTimeout(timer)
    }
  }

  let state: LifecycleState = 'idle'
  let heartbeatTimer: NodeJS.Timeout | undefined
  let sweepTimer: NodeJS.Timeout | undefined
  let inFlightSweep: Promise<SweepResult> | undefined

  const isClaiming = () => state === 'idle' || state === 'starting' || state === 'running'
  const isHeartbeating = () => state === 'running' || state === 'draining'
  const sweep = createSweeper(config, store, () => !isClaiming())

  async function guard(workflowName: string, runId: string): Promise<void> {
    // Retry across the stale window before giving up: a restart can land just before the dead
    // owner's heartbeat crosses the threshold. A dead owner becomes takeable inside the window;
    // a live competitor stays fresh the whole time, and then this step must stop.
    const deadline = Date.now() + config.guardWindowMs
    if (!(await waitForFirstHeartbeat(deadline))) throw new RekindleNotStartedError(workflowName, 'no-heartbeat')
    for (;;) {
      if (await store.hold(workflowName, runId)) return
      const refusal = await store.whyNotHeld(workflowName, runId)
      switch (refusal) {
        case 'done':
          throw new OwnershipLostError(workflowName, runId, 'done')
        case 'no-heartbeat':
          throw new RekindleNotStartedError(workflowName, 'no-heartbeat')
        case 'self-stale':
          // Stale while running, eg the event loop stalled past staleMs. Heartbeat and try again at
          // once: if another instance took the run over meanwhile, the retry sees it as the owner.
          if (!isHeartbeating()) throw new OwnershipLostError(workflowName, runId, 'not-alive')
          await heartbeat()
          break
        case 'owned-elsewhere':
          if (Date.now() >= deadline) throw new OwnershipLostError(workflowName, runId, 'owned-elsewhere')
          await sleep(config.guardRetryMs)
          break
      }
    }
  }

  function sweepOnce(): Promise<SweepResult> {
    if (!isClaiming()) return Promise.resolve({ restarted: 0 })
    inFlightSweep ??= sweep().finally(() => {
      inFlightSweep = undefined
    })
    return inFlightSweep
  }

  function scheduleSweep(delayMs: number): void {
    sweepTimer = setTimeout(() => {
      if (state !== 'running') return
      void sweepOnce()
        .catch((err) => log.error('sweep failed', errorFields(err)))
        .finally(() => {
          if (state === 'running') scheduleSweep(config.sweepIntervalMs + jitter(config.sweepJitterMs))
        })
    }, delayMs)
    sweepTimer.unref()
  }

  const rekindle: Rekindle = {
    instanceId,
    get isDraining() {
      return state === 'draining' || state === 'stopped'
    },

    async migrate() {
      await config.pool.query(migrationSql(config.schema))
    },

    async start() {
      if (state === 'stopped') {
        // stop() may have left runs executing in this process; letting go of them here would let
        // another instance restart them while they run.
        throw new Error('rekindle: start() after stop() is not supported; create a new Rekindle instead')
      }
      if (state !== 'idle') {
        log.warn('start() called while already running; ignoring', { instanceId })
        return
      }
      state = 'starting'
      try {
        // A previous process with this same instanceId is gone; its runs must not look live-owned.
        await store.releaseAll()
        await heartbeat()
      } catch (err) {
        if (state === 'starting') state = 'idle'
        throw err
      }
      // stop() while this was awaiting wins: don't arm the timers.
      if (state !== 'starting') return
      state = 'running'
      heartbeatTimer = setInterval(() => {
        // A failed beat is not fatal; the next tick retries. If beats keep failing this
        // instance looks dead, and its runs get taken over.
        void heartbeat().catch((err) => log.error('heartbeat write failed', errorFields(err)))
      }, config.heartbeatMs)
      heartbeatTimer.unref()
      scheduleSweep(config.bootDelayMs + jitter(config.bootJitterMs))
      log.info('started', {
        instanceId,
        schema: config.schema,
        heartbeatMs: config.heartbeatMs,
        staleMs: config.staleMs,
        sweepIntervalMs: config.sweepIntervalMs,
      })
    },

    drain() {
      if (state !== 'running') return
      state = 'draining'
      clearTimeout(sweepTimer)
      log.info('draining: no new claims, heartbeat continues', { instanceId })
    },

    async stop() {
      state = 'stopped'
      clearTimeout(sweepTimer)
      clearInterval(heartbeatTimer)
      // A failed sweep is reported to whoever started it; here it only needs to have finished.
      await inFlightSweep?.catch(() => {})
    },

    installSignalHandlers({ signals = ['SIGTERM', 'SIGINT'], exit = true } = {}) {
      for (const signal of signals) {
        // Prepended, so this runs before any other listener and sees them all: a `once` handler
        // of the app's would otherwise already be gone by the time this counts listeners.
        process.prependOnceListener(signal, () => {
          const appHandlesIt = process.listenerCount(signal) > 0
          rekindle.drain()
          // With the app's own shutdown in charge, keep heartbeating so in-flight runs aren't
          // taken over while it finishes; the process exits when the app says so.
          if (!exit || appHandlesIt) return
          void rekindle.stop().finally(() => process.kill(process.pid, signal))
        })
      }
    },

    sweepOnce,
    guard,
    clear: (workflowName, runId) => store.markDone(workflowName, runId),
  }

  if (config.setAsDefault) setDefaultProvider(rekindle)
  return { rekindle, store, heartbeat }
}

import { randomUUID } from 'node:crypto'
import type { Pool } from 'pg'
import { OwnershipLostError, RekindleNotStartedError } from './errors.js'
import { consoleLogger, errorFields, type RekindleLogger } from './logger.js'
import { registeredWorkflows, setDefaultProvider, type OwnershipProvider } from './registry.js'
import { assertSchemaName, migrationSql, quoteSchema } from './sql.js'

/** A run as Mastra's listWorkflowRuns returns it. Only these fields are read. */
export interface ListedRun {
  runId: string
  updatedAt?: Date | string
  createdAt?: Date | string
}

/**
 * The slice of Mastra rekindle touches. Typed structurally so any @mastra/core in the supported
 * range fits.
 */
export interface MastraLike {
  getWorkflow(name: string): {
    listWorkflowRuns(args?: {
      status?: string
      perPage?: number | false
      page?: number
      summary?: boolean
    }): Promise<{ runs: ListedRun[]; total?: number }>
    createRun(args: { runId: string }): Promise<{ restart(): Promise<unknown> }>
  }
  getLogger?(): RekindleLogger
  getStorage?(): unknown
}

export interface RekindleOptions {
  /** Your Mastra instance. rekindle uses its PostgresStore's pool and schema, and its logger. */
  mastra?: MastraLike
  /** Override the pool. Default: the pool of Mastra's PostgresStore. */
  pool?: Pool
  /** Override the schema for the two tables. Default: the schema of Mastra's PostgresStore. */
  schema?: string
  /** This process's identity. Default: a random id per boot. Must be unique among live processes. */
  instanceId?: string
  /** How often this instance proves it is alive. Default 5s. */
  heartbeatMs?: number
  /** A heartbeat older than this means the instance is dead. Must exceed heartbeatMs. Default 20s. */
  staleMs?: number
  /** Gap between guard retries while waiting for a dead owner to go stale. Default 3s. */
  guardRetryMs?: number
  /** How long a guard keeps retrying before giving up. Default staleMs + 5s. */
  guardWindowMs?: number
  /** Gap between sweeps. Default 10 min. */
  sweepIntervalMs?: number
  /** Random extra on each sweep gap, so instances don't sweep in lockstep. Default 60s. */
  sweepJitterMs?: number
  /** Wait before the first sweep. Keep it above staleMs so instances killed by a deploy have gone stale. Default 30s. */
  bootDelayMs?: number
  /** Random extra on the boot delay. Default 15s. */
  bootJitterMs?: number
  /** Restarts allowed per sweep across all workflows, so a big backlog drips out. Default 25. */
  maxRestartsPerSweep?: number
  /** How many times one run may be restarted before rekindle leaves it alone. Default 3. */
  maxAttempts?: number
  /** Runs fetched per page when listing a workflow's active runs. Default 100. */
  sweepPageSize?: number
  /** Dead instance rows and finished owner rows older than this are deleted. Default 1h. */
  pruneAfterMs?: number
  /** Workflows to sweep. Default: everything registered with rekindleWorkflow(). */
  workflows?: string[]
  /** Default: Mastra's logger, else console. */
  logger?: RekindleLogger
  /** Make this the instance rekindleWorkflow() guards use. Default true. */
  setAsDefault?: boolean
}

export interface SweepResult {
  restarted: number
}

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
  readonly isDraining: boolean
  /** Create the schema and tables if absent. Idempotent. Or run migrationSql() in your own migrations. */
  migrate(): Promise<void>
  /**
   * Take back any runs a previous boot left under this instanceId, heartbeat now and then on an
   * interval, and sweep after the boot delay and then on an interval. Calling it again while
   * running does nothing.
   */
  start(mastra?: MastraLike): Promise<void>
  /** Stop claiming new runs but keep heartbeating, so this instance's in-flight runs aren't taken over while it drains. */
  drain(): void
  /** Stop everything. In-flight runs can be taken over once the heartbeat goes stale. */
  stop(): Promise<void>
  /** On SIGTERM/SIGINT: drain(), then by default stop and let the signal exit the process. */
  installSignalHandlers(options?: SignalHandlerOptions): void
  /** One pass over the registered workflows. */
  sweepOnce(mastra?: MastraLike): Promise<SweepResult>
  heartbeatOnce(): Promise<void>
  /**
   * Atomically take a run whose owner is dead, or that has no owner and that Mastra hasn't
   * touched within the stale window. True if this instance won.
   */
  claimAbandoned(workflowName: string, runId: string, lastActivityAt: Date): Promise<boolean>
  /** Give up a claim (eg after a failed restart) so the next sweep tries again. */
  release(workflowName: string, runId: string): Promise<void>
  pruneDeadInstances(): Promise<number>
}

const ACTIVE_STATUSES = ['running', 'waiting'] as const

// The timings are interpolated into SQL intervals, so anything but a finite number is refused.
function nonNegative(name: string, value: number): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    throw new Error(`rekindle: ${name} must be a finite number >= 0, got ${value}`)
  }
  return value
}

function positive(name: string, value: number): number {
  if (nonNegative(name, value) === 0) throw new Error(`rekindle: ${name} must be greater than 0`)
  return value
}

function positiveInt(name: string, value: number): number {
  if (!Number.isInteger(positive(name, value))) throw new Error(`rekindle: ${name} must be a whole number, got ${value}`)
  return value
}

export function createRekindle(options: RekindleOptions): Rekindle {
  // PostgresStore exposes `pool` publicly. `schema` is TS-private but set on every version in the
  // supported range ('public' unless schemaName was given).
  const store = options.mastra?.getStorage?.() as { pool?: Pool; schema?: string } | undefined
  const maybePool = options.pool ?? store?.pool
  if (!maybePool) {
    throw new Error('rekindle needs Postgres: give Mastra a PostgresStore from @mastra/pg, or pass `pool`')
  }
  const pool: Pool = maybePool
  const schemaName = options.schema ?? store?.schema
  if (!schemaName) throw new Error("rekindle needs a schema: pass `schema` when not using Mastra's PostgresStore")
  const schema = assertSchemaName(schemaName)
  const q = quoteSchema(schema)
  let log = options.logger ?? options.mastra?.getLogger?.() ?? consoleLogger
  const instanceId = options.instanceId ?? `rekindle-${randomUUID()}`
  const heartbeatMs = positive('heartbeatMs', options.heartbeatMs ?? 5_000)
  const staleMs = positive('staleMs', options.staleMs ?? 20_000)
  const guardRetryMs = positive('guardRetryMs', options.guardRetryMs ?? 3_000)
  const guardWindowMs = positive('guardWindowMs', options.guardWindowMs ?? staleMs + 5_000)
  const sweepIntervalMs = positive('sweepIntervalMs', options.sweepIntervalMs ?? 10 * 60_000)
  const sweepJitterMs = nonNegative('sweepJitterMs', options.sweepJitterMs ?? 60_000)
  const bootDelayMs = nonNegative('bootDelayMs', options.bootDelayMs ?? 30_000)
  const bootJitterMs = nonNegative('bootJitterMs', options.bootJitterMs ?? 15_000)
  const maxRestartsPerSweep = positiveInt('maxRestartsPerSweep', options.maxRestartsPerSweep ?? 25)
  const maxAttempts = positiveInt('maxAttempts', options.maxAttempts ?? 3)
  const sweepPageSize = positiveInt('sweepPageSize', options.sweepPageSize ?? 100)
  const pruneAfterMs = positive('pruneAfterMs', options.pruneAfterMs ?? 3_600_000)
  if (staleMs <= heartbeatMs) {
    throw new Error(`rekindle: staleMs (${staleMs}) must be greater than heartbeatMs (${heartbeatMs})`)
  }

  const instances = `${q}.rekindle_instances`
  const owners = `${q}.rekindle_run_owners`
  const stale = `make_interval(secs => ${staleMs / 1000})`
  const pruneAfter = `make_interval(secs => ${pruneAfterMs / 1000})`

  // Staleness is always judged inside the statement that acts on it, with Postgres now(), so
  // there is no read-then-write race and client clocks don't matter.
  const deadOwners = `SELECT instance_id FROM ${instances} WHERE heartbeat_at < now() - ${stale}`
  // An instance that itself looks dead must not take anything: it would be taken from at once.
  const selfAlive = (param: string) =>
    `EXISTS (SELECT 1 FROM ${instances} WHERE instance_id = ${param} AND heartbeat_at >= now() - ${stale})`

  let heartbeatTimer: NodeJS.Timeout | null = null
  let sweepTimer: NodeJS.Timeout | null = null
  let started = false
  let draining = false
  let sweeping: Promise<SweepResult> | null = null

  const jitter = (max: number) => (max > 0 ? Math.floor(Math.random() * max) : 0)

  async function heartbeatOnce(): Promise<void> {
    await pool.query(
      `INSERT INTO ${instances} (instance_id, heartbeat_at) VALUES ($1, now())
       ON CONFLICT (instance_id) DO UPDATE SET heartbeat_at = now()`,
      [instanceId]
    )
  }

  /** Take or keep the run unless it's done or a different, live instance owns it. */
  async function holdOwnership(workflowName: string, runId: string): Promise<boolean> {
    const res = await pool.query(
      `INSERT INTO ${owners} AS o (workflow_name, run_id, owner_instance_id, claimed_at)
       SELECT $1, $2, $3, now() WHERE ${selfAlive('$3')}
       ON CONFLICT (workflow_name, run_id) DO UPDATE
         SET owner_instance_id = $3, claimed_at = now()
       WHERE o.done_at IS NULL
         AND (o.owner_instance_id IS NULL
              OR o.owner_instance_id = $3
              OR o.owner_instance_id IN (${deadOwners}))`,
      [workflowName, runId, instanceId]
    )
    return (res.rowCount ?? 0) > 0
  }

  /** Why holdOwnership failed, apart from a live competitor. */
  async function guardBlocker(workflowName: string, runId: string): Promise<'done' | 'no-heartbeat' | 'self-stale' | null> {
    const { rows } = await pool.query(
      `SELECT
         (SELECT done_at IS NOT NULL FROM ${owners} WHERE workflow_name = $1 AND run_id = $2) AS done,
         (SELECT heartbeat_at >= now() - ${stale} FROM ${instances} WHERE instance_id = $3) AS alive`,
      [workflowName, runId, instanceId]
    )
    const { done, alive } = rows[0] as { done: boolean | null; alive: boolean | null }
    if (done) return 'done'
    if (alive === null) return 'no-heartbeat'
    if (!alive) return 'self-stale'
    return null
  }

  async function guard(workflowName: string, runId: string): Promise<void> {
    // Retry across the stale window before giving up: a resume can land just before the dead
    // owner's heartbeat crosses the threshold. A dead owner becomes takeable inside the window;
    // a live competitor stays fresh the whole time, and then this step must stop.
    const deadline = Date.now() + guardWindowMs
    for (;;) {
      if (await holdOwnership(workflowName, runId)) return
      const blocker = await guardBlocker(workflowName, runId)
      if (blocker === 'done') throw new OwnershipLostError(runId, workflowName, 'done')
      if (blocker === 'no-heartbeat') throw new RekindleNotStartedError(workflowName, 'no-heartbeat')
      if (blocker === 'self-stale') {
        // Running but stale, eg the event loop stalled past staleMs. Heartbeat and try again at
        // once: if another instance took the run over meanwhile, the retry sees it as the owner.
        if (!heartbeatTimer) throw new OwnershipLostError(runId, workflowName, 'not-alive')
        await heartbeatOnce()
        continue
      }
      if (Date.now() >= deadline) throw new OwnershipLostError(runId, workflowName, 'owned-elsewhere')
      await new Promise((r) => setTimeout(r, guardRetryMs))
    }
  }

  async function claimAbandoned(workflowName: string, runId: string, lastActivityAt: Date): Promise<boolean> {
    // A run with an owner row: take it if the owner is dead (or released it), it isn't done and
    // it hasn't used up its attempts.
    const taken = await pool.query(
      `UPDATE ${owners}
         SET owner_instance_id = $3, claimed_at = now(), restarted_at = now(), attempts = attempts + 1
       WHERE workflow_name = $1 AND run_id = $2
         AND done_at IS NULL
         AND attempts < $4
         AND (owner_instance_id IS NULL OR owner_instance_id IN (${deadOwners}))
         AND ${selfAlive('$3')}`,
      [workflowName, runId, instanceId, maxAttempts]
    )
    if ((taken.rowCount ?? 0) > 0) return true

    // A run with no owner row has never been guarded, or a step is running before its first
    // guard. Only treat it as abandoned once Mastra itself hasn't touched it for the stale window.
    const inserted = await pool.query(
      `INSERT INTO ${owners} (workflow_name, run_id, owner_instance_id, claimed_at, restarted_at, attempts)
       SELECT $1, $2, $3, now(), now(), 1
       WHERE $4 > 0
         AND $5::timestamptz < now() - ${stale}
         AND ${selfAlive('$3')}
       ON CONFLICT (workflow_name, run_id) DO NOTHING`,
      [workflowName, runId, instanceId, maxAttempts, lastActivityAt]
    )
    return (inserted.rowCount ?? 0) > 0
  }

  async function release(workflowName: string, runId: string): Promise<void> {
    await pool.query(
      `UPDATE ${owners} SET owner_instance_id = NULL
       WHERE workflow_name = $1 AND run_id = $2 AND owner_instance_id = $3`,
      [workflowName, runId, instanceId]
    )
  }

  async function clear(workflowName: string, runId: string): Promise<void> {
    // Marked done rather than deleted: Mastra persists the run's final status after the last
    // step returns, and until then a sweep must not mistake the run for an ownerless one.
    await pool.query(
      `INSERT INTO ${owners} AS o (workflow_name, run_id, owner_instance_id, done_at)
       VALUES ($1, $2, NULL, now())
       ON CONFLICT (workflow_name, run_id) DO UPDATE SET owner_instance_id = NULL, done_at = now()`,
      [workflowName, runId]
    )
  }

  async function pruneDeadInstances(): Promise<number> {
    const res = await pool.query(`DELETE FROM ${instances} WHERE heartbeat_at < now() - ${pruneAfter}`)
    await pool.query(`DELETE FROM ${owners} WHERE done_at < now() - ${pruneAfter}`)
    return res.rowCount ?? 0
  }

  /** Owner rows of runs no longer active, age-floored so a run that just started is never pruned. */
  async function pruneInactiveOwners(workflowName: string, activeRunIds: string[]): Promise<void> {
    await pool.query(
      `DELETE FROM ${owners}
       WHERE workflow_name = $1
         AND claimed_at < now() - ${pruneAfter}
         AND NOT (run_id = ANY($2::text[]))`,
      [workflowName, activeRunIds]
    )
  }

  /** Runs already reported as out of attempts, so each is logged once per process, not every sweep. */
  const reportedExhausted = new Set<string>()

  async function warnIfExhausted(workflowName: string, runId: string): Promise<void> {
    const key = `${workflowName}\u0000${runId}`
    if (reportedExhausted.has(key)) return
    const { rows } = await pool.query(
      `SELECT attempts FROM ${owners}
       WHERE workflow_name = $1 AND run_id = $2
         AND done_at IS NULL
         AND attempts >= $3
         AND (owner_instance_id IS NULL OR owner_instance_id IN (${deadOwners}))`,
      [workflowName, runId, maxAttempts]
    )
    if (rows.length === 0) return
    reportedExhausted.add(key)
    log.warn('run used up its restart attempts; leaving it alone', {
      workflow: workflowName,
      runId,
      attempts: rows[0].attempts,
      maxAttempts,
    })
  }

  async function tryRestart(
    workflow: ReturnType<MastraLike['getWorkflow']>,
    name: string,
    run: ListedRun
  ): Promise<boolean> {
    const lastActivityAt = new Date(run.updatedAt ?? run.createdAt ?? 0)
    let won: boolean
    try {
      won = await claimAbandoned(name, run.runId, lastActivityAt)
    } catch (err) {
      log.error('claim failed', { workflow: name, runId: run.runId, ...errorFields(err) })
      return false
    }
    if (!won) {
      await warnIfExhausted(name, run.runId).catch(() => {})
      return false
    }

    log.info('resuming abandoned run', { workflow: name, runId: run.runId, instanceId })
    try {
      const handle = await workflow.createRun({ runId: run.runId })
      // restart() resolves only when the run finishes, so don't await it. If it fails, release
      // the claim so a later sweep retries (up to maxAttempts) instead of leaving the run owned
      // by a live instance that isn't running it.
      void handle.restart().catch((err) => {
        log.error('resumed run failed; releasing it', { workflow: name, runId: run.runId, ...errorFields(err) })
        void release(name, run.runId).catch(() => {})
      })
      return true
    } catch (err) {
      log.error('restart kickoff failed; releasing it', { workflow: name, runId: run.runId, ...errorFields(err) })
      await release(name, run.runId).catch(() => {})
      return false
    }
  }

  async function sweepWorkflow(mastra: MastraLike, name: string, budget: number): Promise<number> {
    let workflow: ReturnType<MastraLike['getWorkflow']>
    try {
      workflow = mastra.getWorkflow(name)
    } catch (err) {
      log.error('workflow not registered with Mastra', { workflow: name, ...errorFields(err) })
      return 0
    }

    const activeRunIds: string[] = []
    let sawEverything = true
    let restarted = 0

    listing: for (const status of ACTIVE_STATUSES) {
      for (let page = 0; ; page++) {
        if (draining || restarted >= budget) {
          sawEverything = false
          break listing
        }
        let runs: ListedRun[]
        try {
          ;({ runs } = await workflow.listWorkflowRuns({ status, perPage: sweepPageSize, page, summary: true }))
        } catch (err) {
          log.error('listWorkflowRuns failed', { workflow: name, status, ...errorFields(err) })
          sawEverything = false
          break listing
        }
        for (const run of runs) {
          if (!run.runId) continue
          activeRunIds.push(run.runId)
          if (restarted < budget && !draining && (await tryRestart(workflow, name, run))) restarted += 1
        }
        if (runs.length < sweepPageSize) break
      }
    }

    // Prune only after seeing every active run; otherwise it could delete owners of active runs
    // that simply weren't listed.
    if (sawEverything) await pruneInactiveOwners(name, activeRunIds).catch(() => {})
    if (restarted >= budget) log.info('restart cap reached; the rest waits for the next sweep', { workflow: name, restarted })
    return restarted
  }

  async function runSweep(mastra: MastraLike): Promise<SweepResult> {
    let budget = maxRestartsPerSweep
    let restarted = 0
    for (const name of options.workflows ?? registeredWorkflows()) {
      if (budget <= 0 || draining) break
      const n = await sweepWorkflow(mastra, name, budget)
      budget -= n
      restarted += n
    }
    await pruneDeadInstances().catch((err) => log.error('prune failed', errorFields(err)))
    return { restarted }
  }

  function resolveMastra(given?: MastraLike): MastraLike {
    const mastra = given ?? options.mastra
    if (!mastra) throw new Error('rekindle needs Mastra: pass it here or as `mastra` in createRekindle()')
    return mastra
  }

  /** Single-flight: a call while a sweep is running joins it. */
  function sweepOnce(given?: MastraLike): Promise<SweepResult> {
    if (draining) return Promise.resolve({ restarted: 0 })
    const mastra = resolveMastra(given)
    if (!sweeping) {
      sweeping = runSweep(mastra).finally(() => {
        sweeping = null
      })
    }
    return sweeping
  }

  function scheduleSweep(mastra: MastraLike, delay: number): void {
    if (draining) return
    sweepTimer = setTimeout(() => {
      if (draining) return
      void sweepOnce(mastra)
        .catch((err) => log.error('sweep failed', errorFields(err)))
        .finally(() => scheduleSweep(mastra, sweepIntervalMs + jitter(sweepJitterMs)))
    }, delay)
    sweepTimer.unref?.()
  }

  const rekindle: Rekindle = {
    instanceId,
    get isDraining() {
      return draining
    },

    async migrate() {
      await pool.query(migrationSql(schema))
    },

    async start(given) {
      if (started) {
        log.warn('start() called while already running; ignoring', { instanceId })
        return
      }
      const mastra = resolveMastra(given)
      if (!options.logger && mastra.getLogger) log = mastra.getLogger()
      started = true
      try {
        // A previous process with this same instanceId is gone; its runs must not look live-owned.
        await pool.query(
          `UPDATE ${owners} SET owner_instance_id = NULL WHERE owner_instance_id = $1 AND done_at IS NULL`,
          [instanceId]
        )
        await heartbeatOnce()
      } catch (err) {
        started = false
        throw err
      }
      draining = false
      heartbeatTimer = setInterval(() => {
        // A failed beat is not fatal; the next tick retries. If beats keep failing this
        // instance looks dead, and its runs get taken over.
        void heartbeatOnce().catch((err) => log.error('heartbeat write failed', errorFields(err)))
      }, heartbeatMs)
      heartbeatTimer.unref?.()
      scheduleSweep(mastra, bootDelayMs + jitter(bootJitterMs))
      log.info('started', { instanceId, schema, heartbeatMs, staleMs, sweepIntervalMs })
    },

    drain() {
      if (draining) return
      draining = true
      if (sweepTimer) clearTimeout(sweepTimer)
      sweepTimer = null
      log.info('draining: no new claims, heartbeat continues', { instanceId })
    },

    async stop() {
      rekindle.drain()
      if (heartbeatTimer) clearInterval(heartbeatTimer)
      heartbeatTimer = null
      started = false
      await sweeping?.catch(() => {})
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
    heartbeatOnce,
    guard,
    clear,
    claimAbandoned,
    release,
    pruneDeadInstances,
  }

  if (options.setAsDefault ?? true) setDefaultProvider(rekindle)
  return rekindle
}

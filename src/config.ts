import { randomUUID } from 'node:crypto'
import type { Pool } from 'pg'
import { RekindleConfigError } from './errors.js'
import { consoleLogger, type RekindleLogger } from './logger.js'
import type { MastraLike } from './mastra.js'
import { assertSchemaName } from './sql.js'

export interface RekindleOptions {
  /** Your Mastra instance. rekindle restarts runs through it, and uses its PostgresStore's pool and schema, and its logger. */
  mastra: MastraLike
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
  /** How many times one run may be restarted in total before rekindle leaves it alone. Default 3. */
  maxAttempts?: number
  /** Runs fetched per page when listing a workflow's active runs. Default 100. */
  sweepPageSize?: number
  /** Dead instance rows and finished owner rows older than this are deleted. Default 1h. */
  pruneAfterMs?: number
  /** Workflows to sweep. Default: everything registered with rekindleWorkflow(). */
  workflows?: string[]
  /** Default: Mastra's logger, else console. */
  logger?: RekindleLogger
  /** Make this the Rekindle object rekindleWorkflow() guards go through. Default true. */
  setAsDefault?: boolean
}

export const DEFAULTS = {
  heartbeatMs: 5_000,
  staleMs: 20_000,
  guardRetryMs: 3_000,
  /** Added to staleMs when guardWindowMs isn't given. */
  guardWindowPastStaleMs: 5_000,
  sweepIntervalMs: 10 * 60_000,
  sweepJitterMs: 60_000,
  bootDelayMs: 30_000,
  bootJitterMs: 15_000,
  maxRestartsPerSweep: 25,
  maxAttempts: 3,
  sweepPageSize: 100,
  pruneAfterMs: 60 * 60_000,
} as const

/** Options with defaults applied and every value checked. */
export interface RekindleConfig {
  readonly mastra: MastraLike
  readonly pool: Pool
  readonly schema: string
  readonly instanceId: string
  readonly logger: RekindleLogger
  readonly heartbeatMs: number
  readonly staleMs: number
  readonly guardRetryMs: number
  readonly guardWindowMs: number
  readonly sweepIntervalMs: number
  readonly sweepJitterMs: number
  readonly bootDelayMs: number
  readonly bootJitterMs: number
  readonly maxRestartsPerSweep: number
  readonly maxAttempts: number
  readonly sweepPageSize: number
  readonly pruneAfterMs: number
  /** Undefined means whatever is registered when a sweep runs; workflows register at import time, possibly after this. */
  readonly workflows: readonly string[] | undefined
  readonly setAsDefault: boolean
}

export function resolveConfig(options: RekindleOptions): RekindleConfig {
  const { mastra } = options
  if (!mastra || typeof mastra.getWorkflow !== 'function') {
    throw new RekindleConfigError('needs your Mastra instance as `mastra`')
  }

  // PostgresStore exposes `pool` publicly. `schema` is TS-private but set on every version in the
  // supported range ('public' unless schemaName was given).
  const storage = mastra.getStorage?.() as { pool?: Pool; schema?: string } | undefined
  const pool = options.pool ?? storage?.pool
  if (!pool) throw new RekindleConfigError('needs Postgres: give Mastra a PostgresStore from @mastra/pg, or pass `pool`')
  const schema = options.schema ?? storage?.schema
  if (!schema) throw new RekindleConfigError("needs a schema: pass `schema` when not using Mastra's PostgresStore")

  const heartbeatMs = positive('heartbeatMs', options.heartbeatMs ?? DEFAULTS.heartbeatMs)
  const staleMs = positive('staleMs', options.staleMs ?? DEFAULTS.staleMs)
  if (staleMs <= heartbeatMs) {
    throw new RekindleConfigError(`staleMs (${staleMs}) must be greater than heartbeatMs (${heartbeatMs})`)
  }

  return {
    mastra,
    pool,
    schema: assertSchemaName(schema),
    instanceId: options.instanceId ?? `rekindle-${randomUUID()}`,
    logger: options.logger ?? mastra.getLogger?.() ?? consoleLogger,
    heartbeatMs,
    staleMs,
    guardRetryMs: positive('guardRetryMs', options.guardRetryMs ?? DEFAULTS.guardRetryMs),
    guardWindowMs: positive('guardWindowMs', options.guardWindowMs ?? staleMs + DEFAULTS.guardWindowPastStaleMs),
    sweepIntervalMs: positive('sweepIntervalMs', options.sweepIntervalMs ?? DEFAULTS.sweepIntervalMs),
    sweepJitterMs: nonNegative('sweepJitterMs', options.sweepJitterMs ?? DEFAULTS.sweepJitterMs),
    bootDelayMs: nonNegative('bootDelayMs', options.bootDelayMs ?? DEFAULTS.bootDelayMs),
    bootJitterMs: nonNegative('bootJitterMs', options.bootJitterMs ?? DEFAULTS.bootJitterMs),
    maxRestartsPerSweep: positiveInt('maxRestartsPerSweep', options.maxRestartsPerSweep ?? DEFAULTS.maxRestartsPerSweep),
    maxAttempts: positiveInt('maxAttempts', options.maxAttempts ?? DEFAULTS.maxAttempts),
    sweepPageSize: positiveInt('sweepPageSize', options.sweepPageSize ?? DEFAULTS.sweepPageSize),
    pruneAfterMs: positive('pruneAfterMs', options.pruneAfterMs ?? DEFAULTS.pruneAfterMs),
    workflows: workflowList(options.workflows),
    setAsDefault: options.setAsDefault ?? true,
  }
}

// The timings are interpolated into SQL intervals, so anything but a finite number is refused.
function nonNegative(name: string, value: number): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    throw new RekindleConfigError(`${name} must be a finite number >= 0, got ${value}`)
  }
  return value
}

function positive(name: string, value: number): number {
  if (nonNegative(name, value) === 0) throw new RekindleConfigError(`${name} must be greater than 0`)
  return value
}

function positiveInt(name: string, value: number): number {
  if (!Number.isInteger(positive(name, value))) throw new RekindleConfigError(`${name} must be a whole number, got ${value}`)
  return value
}

function workflowList(workflows: string[] | undefined): readonly string[] | undefined {
  if (workflows === undefined) return undefined
  if (!Array.isArray(workflows) || workflows.some((name) => typeof name !== 'string' || name === '')) {
    throw new RekindleConfigError('workflows must be an array of workflow names')
  }
  return [...workflows]
}

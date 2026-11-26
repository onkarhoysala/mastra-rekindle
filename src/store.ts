import type { Pool } from 'pg'
import { tableNames } from './sql.js'

/**
 * Every statement rekindle runs, bound to one instance. Staleness is always judged inside the
 * statement that acts on it, with Postgres now(), so there is no read-then-write race and client
 * clocks don't matter.
 */
export interface OwnerStore {
  /** Write this instance's heartbeat. */
  heartbeat(): Promise<void>
  /** Take or keep the run unless it's done or a different, live instance owns it. True if held. */
  hold(workflowName: string, runId: string): Promise<boolean>
  /** Why hold() just failed. */
  whyNotHeld(workflowName: string, runId: string): Promise<HoldRefusal>
  /**
   * Take a run whose owner is dead, or that has no owner and that Mastra hasn't touched within
   * the stale window, counting it as a restart attempt. True if this instance won.
   */
  claim(workflowName: string, runId: string, lastActivityAt: Date): Promise<boolean>
  /** Give up a claim (eg after a failed restart) so the next sweep tries again. */
  release(workflowName: string, runId: string): Promise<void>
  /** Give up every unfinished run held under this instanceId, eg by a previous boot. */
  releaseAll(): Promise<void>
  /** Mark the run done, so no sweep ever restarts it. */
  markDone(workflowName: string, runId: string): Promise<void>
  /** The attempt count of an unfinished, unowned run that used up its attempts, else null. */
  exhaustedAttempts(workflowName: string, runId: string): Promise<number | null>
  /** Delete dead instance rows and done owner rows older than pruneAfterMs. Returns the instances deleted. */
  pruneExpired(): Promise<number>
  /** Delete a workflow's owner rows not in `activeRunIds`, if claimed longer than pruneAfterMs ago. */
  pruneOwnersExcept(workflowName: string, activeRunIds: readonly string[]): Promise<void>
}

/**
 * - `done`: the run was marked done.
 * - `no-heartbeat`: this instance has never heartbeated.
 * - `self-stale`: this instance's own heartbeat is stale.
 * - `owned-elsewhere`: a different, live instance owns the run.
 */
export type HoldRefusal = 'done' | 'no-heartbeat' | 'self-stale' | 'owned-elsewhere'

export interface OwnerStoreSettings {
  pool: Pool
  schema: string
  instanceId: string
  staleMs: number
  pruneAfterMs: number
  maxAttempts: number
}

export function createOwnerStore({ pool, schema, instanceId, staleMs, pruneAfterMs, maxAttempts }: OwnerStoreSettings): OwnerStore {
  const { instances, owners } = tableNames(schema)
  const staleIntervalSql = `make_interval(secs => ${staleMs / 1000})`
  const pruneIntervalSql = `make_interval(secs => ${pruneAfterMs / 1000})`
  const deadInstanceIdsSql = `SELECT instance_id FROM ${instances} WHERE heartbeat_at < now() - ${staleIntervalSql}`
  // An instance that itself looks dead must not take anything: it would be taken from at once.
  const isAliveSql = (instanceParam: string) =>
    `EXISTS (SELECT 1 FROM ${instances} WHERE instance_id = ${instanceParam} AND heartbeat_at >= now() - ${staleIntervalSql})`

  const changedRows = (result: { rowCount: number | null }) => (result.rowCount ?? 0) > 0

  return {
    async heartbeat() {
      await pool.query(
        `INSERT INTO ${instances} (instance_id, heartbeat_at) VALUES ($1, now())
         ON CONFLICT (instance_id) DO UPDATE SET heartbeat_at = now()`,
        [instanceId]
      )
    },

    async hold(workflowName, runId) {
      const result = await pool.query(
        `INSERT INTO ${owners} AS o (workflow_name, run_id, owner_instance_id, claimed_at)
         SELECT $1, $2, $3, now() WHERE ${isAliveSql('$3')}
         ON CONFLICT (workflow_name, run_id) DO UPDATE
           SET owner_instance_id = $3, claimed_at = now()
         WHERE o.done_at IS NULL
           AND (o.owner_instance_id IS NULL
                OR o.owner_instance_id = $3
                OR o.owner_instance_id IN (${deadInstanceIdsSql}))`,
        [workflowName, runId, instanceId]
      )
      return changedRows(result)
    },

    async whyNotHeld(workflowName, runId) {
      const { rows } = await pool.query<{ done: boolean | null; alive: boolean | null }>(
        `SELECT
           (SELECT done_at IS NOT NULL FROM ${owners} WHERE workflow_name = $1 AND run_id = $2) AS done,
           (SELECT heartbeat_at >= now() - ${staleIntervalSql} FROM ${instances} WHERE instance_id = $3) AS alive`,
        [workflowName, runId, instanceId]
      )
      const { done, alive } = rows[0]
      if (done) return 'done'
      if (alive === null) return 'no-heartbeat'
      if (!alive) return 'self-stale'
      return 'owned-elsewhere'
    },

    async claim(workflowName, runId, lastActivityAt) {
      // A run with an owner row: take it if the owner is dead (or released it), it isn't done and
      // it hasn't used up its attempts.
      const taken = await pool.query(
        `UPDATE ${owners}
           SET owner_instance_id = $3, claimed_at = now(), restarted_at = now(), attempts = attempts + 1
         WHERE workflow_name = $1 AND run_id = $2
           AND done_at IS NULL
           AND attempts < $4
           AND (owner_instance_id IS NULL OR owner_instance_id IN (${deadInstanceIdsSql}))
           AND ${isAliveSql('$3')}`,
        [workflowName, runId, instanceId, maxAttempts]
      )
      if (changedRows(taken)) return true

      // A run with no owner row has never been guarded, or a step is running before its first
      // guard. Only treat it as abandoned once Mastra itself hasn't touched it for the stale window.
      const inserted = await pool.query(
        `INSERT INTO ${owners} (workflow_name, run_id, owner_instance_id, claimed_at, restarted_at, attempts)
         SELECT $1, $2, $3, now(), now(), 1
         WHERE $4::timestamptz < now() - ${staleIntervalSql}
           AND ${isAliveSql('$3')}
         ON CONFLICT (workflow_name, run_id) DO NOTHING`,
        [workflowName, runId, instanceId, lastActivityAt]
      )
      return changedRows(inserted)
    },

    async release(workflowName, runId) {
      await pool.query(
        `UPDATE ${owners} SET owner_instance_id = NULL
         WHERE workflow_name = $1 AND run_id = $2 AND owner_instance_id = $3`,
        [workflowName, runId, instanceId]
      )
    },

    async releaseAll() {
      await pool.query(`UPDATE ${owners} SET owner_instance_id = NULL WHERE owner_instance_id = $1 AND done_at IS NULL`, [
        instanceId,
      ])
    },

    async markDone(workflowName, runId) {
      // Marked done rather than deleted: Mastra persists the run's final status after the last
      // step returns, and until then a sweep must not mistake the run for an ownerless one.
      await pool.query(
        `INSERT INTO ${owners} AS o (workflow_name, run_id, owner_instance_id, done_at)
         VALUES ($1, $2, NULL, now())
         ON CONFLICT (workflow_name, run_id) DO UPDATE SET owner_instance_id = NULL, done_at = now()`,
        [workflowName, runId]
      )
    },

    async exhaustedAttempts(workflowName, runId) {
      const { rows } = await pool.query<{ attempts: number }>(
        `SELECT attempts FROM ${owners}
         WHERE workflow_name = $1 AND run_id = $2
           AND done_at IS NULL
           AND attempts >= $3
           AND (owner_instance_id IS NULL OR owner_instance_id IN (${deadInstanceIdsSql}))`,
        [workflowName, runId, maxAttempts]
      )
      return rows[0]?.attempts ?? null
    },

    async pruneExpired() {
      const deadInstances = await pool.query(`DELETE FROM ${instances} WHERE heartbeat_at < now() - ${pruneIntervalSql}`)
      await pool.query(`DELETE FROM ${owners} WHERE done_at < now() - ${pruneIntervalSql}`)
      return deadInstances.rowCount ?? 0
    },

    async pruneOwnersExcept(workflowName, activeRunIds) {
      // Age-floored, so a run that just started is never pruned.
      await pool.query(
        `DELETE FROM ${owners}
         WHERE workflow_name = $1
           AND claimed_at < now() - ${pruneIntervalSql}
           AND NOT (run_id = ANY($2::text[]))`,
        [workflowName, activeRunIds]
      )
    },
  }
}

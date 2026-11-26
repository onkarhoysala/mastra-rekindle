import type { RekindleConfig } from './config.js'
import { errorFields } from './logger.js'
import type { ListedRun, MastraWorkflow } from './mastra.js'
import { registeredWorkflows } from './registry.js'
import type { OwnerStore } from './store.js'

export interface SweepResult {
  restarted: number
}

const ACTIVE_STATUSES = ['running', 'waiting'] as const

/** Every active (running or waiting) run of a workflow, a page at a time. */
async function* listActiveRuns(workflow: MastraWorkflow, pageSize: number): AsyncGenerator<ListedRun> {
  for (const status of ACTIVE_STATUSES) {
    for (let page = 0; ; page++) {
      const { runs } = await workflow.listWorkflowRuns({ status, perPage: pageSize, page, summary: true })
      for (const run of runs) if (run.runId) yield run
      if (runs.length < pageSize) break
    }
  }
}

/** One pass over the opted-in workflows: claim each abandoned run and restart it, up to the per-sweep cap. */
export function createSweeper(config: RekindleConfig, store: OwnerStore, shouldStop: () => boolean) {
  const { mastra, logger: log, instanceId } = config

  /** Runs already reported as out of attempts, so each is logged once per process, not every sweep. */
  const reportedExhausted = new Set<string>()

  async function warnIfExhausted(workflowName: string, runId: string): Promise<void> {
    const key = `${workflowName}\u0000${runId}`
    if (reportedExhausted.has(key)) return
    const attempts = await store.exhaustedAttempts(workflowName, runId)
    if (attempts === null) return
    reportedExhausted.add(key)
    log.warn('run used up its restart attempts; leaving it alone', {
      workflow: workflowName,
      runId,
      attempts,
      maxAttempts: config.maxAttempts,
    })
  }

  async function releaseAfterFailure(workflowName: string, runId: string): Promise<void> {
    await store
      .release(workflowName, runId)
      .catch((err) => log.error('release failed', { workflow: workflowName, runId, ...errorFields(err) }))
  }

  /** Claim the run and, if this instance won, kick off its restart. True if it was restarted. */
  async function claimAndRestart(workflow: MastraWorkflow, workflowName: string, run: ListedRun): Promise<boolean> {
    const runId = run.runId
    // A run with neither timestamp counts as idle since the epoch, ie long enough to be claimable.
    const lastActivityAt = new Date(run.updatedAt ?? run.createdAt ?? 0)
    let won: boolean
    try {
      won = await store.claim(workflowName, runId, lastActivityAt)
    } catch (err) {
      log.error('claim failed', { workflow: workflowName, runId, ...errorFields(err) })
      return false
    }
    if (!won) {
      await warnIfExhausted(workflowName, runId).catch((err) =>
        log.error('attempts check failed', { workflow: workflowName, runId, ...errorFields(err) })
      )
      return false
    }

    log.info('resuming abandoned run', { workflow: workflowName, runId, instanceId })
    try {
      const handle = await workflow.createRun({ runId })
      // restart() resolves only when the run finishes, so don't await it. If it fails, release
      // the claim so a later sweep retries (up to maxAttempts) instead of leaving the run owned
      // by a live instance that isn't running it.
      void handle.restart().catch(async (err) => {
        log.error('restarted run failed; releasing it', { workflow: workflowName, runId, ...errorFields(err) })
        await releaseAfterFailure(workflowName, runId)
      })
      return true
    } catch (err) {
      log.error('restart kickoff failed; releasing it', { workflow: workflowName, runId, ...errorFields(err) })
      await releaseAfterFailure(workflowName, runId)
      return false
    }
  }

  async function sweepWorkflow(workflowName: string, budget: number): Promise<number> {
    let workflow: MastraWorkflow
    try {
      workflow = mastra.getWorkflow(workflowName)
    } catch (err) {
      log.error('workflow not registered with Mastra', { workflow: workflowName, ...errorFields(err) })
      return 0
    }

    const activeRunIds: string[] = []
    let restarted = 0
    let listedAllActiveRuns = true
    try {
      for await (const run of listActiveRuns(workflow, config.sweepPageSize)) {
        if (shouldStop() || restarted >= budget) {
          listedAllActiveRuns = false
          break
        }
        activeRunIds.push(run.runId)
        if (await claimAndRestart(workflow, workflowName, run)) restarted += 1
      }
    } catch (err) {
      listedAllActiveRuns = false
      log.error('listWorkflowRuns failed', { workflow: workflowName, ...errorFields(err) })
    }

    // Prune only after seeing every active run; otherwise it could delete owners of active runs
    // that simply weren't listed.
    if (listedAllActiveRuns) {
      await store
        .pruneOwnersExcept(workflowName, activeRunIds)
        .catch((err) => log.error('owner prune failed', { workflow: workflowName, ...errorFields(err) }))
    }
    if (restarted >= budget) log.info('restart cap reached; the rest waits for the next sweep', { workflow: workflowName, restarted })
    return restarted
  }

  return async function sweep(): Promise<SweepResult> {
    let restarted = 0
    for (const workflowName of config.workflows ?? registeredWorkflows()) {
      const budget = config.maxRestartsPerSweep - restarted
      if (budget <= 0 || shouldStop()) break
      restarted += await sweepWorkflow(workflowName, budget)
    }
    await store.pruneExpired().catch((err) => log.error('prune failed', errorFields(err)))
    return { restarted }
  }
}

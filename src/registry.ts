import { RekindleNotStartedError } from './errors.js'

/**
 * Opt-in registry. Workflows declare themselves resumable at import time, before Mastra (or the
 * Rekindle object) exists, which is what keeps opting in to one line:
 *
 *   const ownership = rekindleWorkflow('reportRun')
 *   // first line of every step:
 *   await ownership.guard(runId)   // runId from the step's execute args
 *
 * The sweep only ever restarts registered workflows, so a one-shot workflow whose steps are not
 * safe to re-run is never restarted blindly.
 */

/** What a workflow's steps call. Implemented by the Rekindle object createRekindle() returns. */
export interface OwnershipProvider {
  guard(workflowName: string, runId: string): Promise<void>
  clear(workflowName: string, runId: string): Promise<void>
}

const registered = new Set<string>()
let defaultProvider: OwnershipProvider | null = null

export interface WorkflowOwnership {
  /** First line of every step. Takes or keeps the run; throws OwnershipLostError if another live instance holds it. */
  guard(runId: string): Promise<void>
  /** On the final step: mark the run done so no sweep ever restarts it. */
  clear(runId: string): Promise<void>
}

export function rekindleWorkflow(workflowName: string): WorkflowOwnership {
  registered.add(workflowName)
  const provider = () => {
    if (!defaultProvider) throw new RekindleNotStartedError(workflowName)
    return defaultProvider
  }
  return {
    guard: async (runId) => provider().guard(workflowName, runId),
    clear: async (runId) => provider().clear(workflowName, runId),
  }
}

export function registeredWorkflows(): string[] {
  return [...registered]
}

/** The Rekindle object `rekindleWorkflow()` guards go through. createRekindle sets it unless told not to. */
export function setDefaultProvider(provider: OwnershipProvider | null): void {
  defaultProvider = provider
}

/** Tests only. */
export function resetRegistry(): void {
  registered.clear()
  defaultProvider = null
}

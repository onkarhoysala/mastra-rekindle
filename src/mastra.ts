import type { RekindleLogger } from './logger.js'

/** A run as Mastra's listWorkflowRuns returns it. Only these fields are read. */
export interface ListedRun {
  runId: string
  updatedAt?: Date | string
  createdAt?: Date | string
}

/** The slice of a Mastra workflow rekindle touches. */
export interface MastraWorkflow {
  listWorkflowRuns(args?: {
    status?: string
    perPage?: number | false
    page?: number
    summary?: boolean
  }): Promise<{ runs: ListedRun[]; total?: number }>
  createRun(args: { runId: string }): Promise<{ restart(): Promise<unknown> }>
}

/**
 * The slice of Mastra rekindle touches. Typed structurally so any @mastra/core in the supported
 * range fits.
 */
export interface MastraLike {
  getWorkflow(name: string): MastraWorkflow
  getLogger?(): RekindleLogger
  getStorage?(): unknown
}

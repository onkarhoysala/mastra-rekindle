/**
 * Why a guard refused a step:
 *  - `owned-elsewhere`: another, live instance owns the run.
 *  - `done`: the run was already marked done with clear().
 *  - `not-alive`: this instance is stopped and its heartbeat is stale, so it may have been taken over.
 */
export type OwnershipLostReason = 'owned-elsewhere' | 'done' | 'not-alive'

const MESSAGES: Record<OwnershipLostReason, string> = {
  'owned-elsewhere': 'is held by another live instance',
  done: 'was already marked done',
  'not-alive': "can't be held: this instance is stopped and its heartbeat is stale",
}

/** Thrown by a step's guard when this instance must not run the step. Don't catch it; let the step fail. */
export class OwnershipLostError extends Error {
  constructor(
    readonly runId: string,
    readonly workflowName: string,
    readonly reason: OwnershipLostReason = 'owned-elsewhere'
  ) {
    super(`${workflowName} run ${runId} ${MESSAGES[reason]}`)
    this.name = 'OwnershipLostError'
  }
}

/** Thrown by a guard that runs before createRekindle(), or when start() hasn't heartbeated within the guard window. */
export class RekindleNotStartedError extends Error {
  constructor(
    readonly workflowName: string,
    reason: 'no-instance' | 'no-heartbeat' = 'no-instance'
  ) {
    super(
      reason === 'no-instance'
        ? `rekindle: guard for workflow "${workflowName}" ran before createRekindle(); create the instance when your server boots`
        : `rekindle: guard for workflow "${workflowName}" waited the guard window and this instance never heartbeated; call rekindle.start() when your server boots`
    )
    this.name = 'RekindleNotStartedError'
  }
}

/**
 * Why a guard refused a step:
 *  - `owned-elsewhere`: another, live instance owns the run.
 *  - `done`: the run was already marked done with clear().
 *  - `not-alive`: this instance isn't running and its heartbeat is stale, so it may have been taken over.
 */
export type OwnershipLostReason = 'owned-elsewhere' | 'done' | 'not-alive'

const OWNERSHIP_LOST_MESSAGES: Record<OwnershipLostReason, string> = {
  'owned-elsewhere': 'is held by another live instance',
  done: 'was already marked done',
  'not-alive': "can't be held: this instance isn't running and its heartbeat is stale",
}

/** Thrown by a step's guard when this instance must not run the step. Don't catch it; let the step fail. */
export class OwnershipLostError extends Error {
  constructor(
    readonly workflowName: string,
    readonly runId: string,
    readonly reason: OwnershipLostReason = 'owned-elsewhere'
  ) {
    super(`${workflowName} run ${runId} ${OWNERSHIP_LOST_MESSAGES[reason]}`)
    this.name = 'OwnershipLostError'
  }
}

/**
 * Why a guard couldn't check ownership at all:
 *  - `no-instance`: it ran before createRekindle().
 *  - `no-heartbeat`: this instance hadn't heartbeated by the end of the guard window, ie start() wasn't called.
 */
export type RekindleNotStartedReason = 'no-instance' | 'no-heartbeat'

const NOT_STARTED_MESSAGES: Record<RekindleNotStartedReason, string> = {
  'no-instance': 'ran before createRekindle(); create the instance when your server boots',
  'no-heartbeat': 'waited the guard window and this instance never heartbeated; call rekindle.start() when your server boots',
}

export class RekindleNotStartedError extends Error {
  constructor(
    readonly workflowName: string,
    readonly reason: RekindleNotStartedReason = 'no-instance'
  ) {
    super(`rekindle: guard for workflow "${workflowName}" ${NOT_STARTED_MESSAGES[reason]}`)
    this.name = 'RekindleNotStartedError'
  }
}

/** Thrown by createRekindle() and migrationSql() when an option is missing or invalid. */
export class RekindleConfigError extends Error {
  constructor(message: string) {
    super(`rekindle: ${message}`)
    this.name = 'RekindleConfigError'
  }
}

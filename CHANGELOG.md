# Changelog

## 0.2.0

Code cleanup: config, SQL, sweep and lifecycle are now separate modules. The claim and guard SQL is unchanged.

Breaking:

- `mastra` is now required in `createRekindle()`, and `start()` and `sweepOnce()` no longer take one. Pass Mastra once, at creation.
- `OwnershipLostError` takes `(workflowName, runId, reason)`, the same order as everything else. It was `(runId, workflowName, reason)`.
- `heartbeatOnce()`, `claimAbandoned()`, `release()` and `pruneDeadInstances()` are no longer on the `Rekindle` object. They were only there for tests.
- `start()` after `stop()` throws. It used to start again and let go of runs this process might still be running.
- Config errors are `RekindleConfigError` instead of plain `Error`.

Other changes:

- `stop()` while `start()` is still in progress wins; `start()` no longer arms the timers afterwards.
- `drain()` does nothing until `start()` has finished. Before, calling it early made `sweepOnce()` a no-op until `start()`.
- A sweep stops listing a workflow's runs as soon as it hits the restart cap, and skips the owner prune for that workflow (it already did when the cap was hit before the last page).
- `RekindleNotStartedError` keeps its `reason`.
- Failed releases, attempt checks and owner prunes are logged instead of ignored. Logged errors include the stack.
- Log wording: "resumed run failed" is now "restarted run failed", the `not-alive` message says "isn't running" instead of "is stopped", and the "listWorkflowRuns failed" log no longer has a `status` field.
- `MastraWorkflow` and `RekindleNotStartedReason` types are exported.

If you only use `createRekindle({ mastra, ... })`, `migrate()`, `start()`, `drain()`, `stop()` and `rekindleWorkflow(...).guard/clear`, nothing changes for you.

## 0.1.3

README links the blog post.

## 0.1.2

The guard waits for this instance's first heartbeat instead of failing when a run lands before `start()` finishes. README notes on `runId`, `drain()` and `clear()`.

## 0.1.1

Removed `ecsTaskInstanceId()`. Pass your ECS task ARN as `instanceId` yourself.

## 0.1.0

First release.

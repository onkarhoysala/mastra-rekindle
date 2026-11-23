# mastra-rekindle

Rekindle restarts Mastra workflow runs that were left `running` (or `waiting` in a sleep) after a crash or a deploy. With multiple instances, only one of them restarts a given run.

It calls Mastra's `run.restart()`, which resumes a run from its last active step. Each instance writes a heartbeat to Postgres and claims a run before restarting it. A periodic sweep looks for runs to claim, and a guard at the start of each step checks ownership.

## Why this was needed

> As of 17 Nov 2026, Mastra has this built in. `@mastra/core` 1.84.0 shipped native workflow recovery, with run leases, a recovery loop and leader election.

We needed it back in May/June 2026, though, when we were running 1.4x. Our agent platform at Flash runs long workflows (scout runs ~18 min, research batches ~30 min), and every deploy or crash left them stuck in `running`. So I built this, first commit in our internal repo on 9 Jul 2026. It's been running in production since then across 8 of our agents.

If you're on Mastra 1.84 or later, use the native recovery. We're moving to it too. rekindle is for codebases still on 1.46 to 1.83, like one of ours. Tested on 1.46 and 1.78, maintained as needed.

## How it works

- Every server process writes a heartbeat to `rekindle_instances` every 5s.
- Every opted-in run has a row in `rekindle_run_owners` saying which instance owns it. The guard writes it.
- Every ~10 min (after a 30s boot delay, with jitter) each instance pages through the `running` and `waiting` runs of the opted-in workflows and tries to claim them.
- A claim is one SQL statement. It succeeds only if the run's owner has a heartbeat older than 20s (or released it), the run isn't marked done, it hasn't been restarted 3 times already, and the claiming instance's own heartbeat is fresh. The instance that gets the row calls `restart()`. The others get 0 rows and skip the run.

  ```sql
  UPDATE rekindle_run_owners
     SET owner_instance_id = $me, claimed_at = now(), restarted_at = now(), attempts = attempts + 1
   WHERE workflow_name = $1 AND run_id = $2
     AND done_at IS NULL
     AND attempts < $maxAttempts
     AND (owner_instance_id IS NULL OR owner_instance_id IN (
           SELECT instance_id FROM rekindle_instances WHERE heartbeat_at < now() - interval '20 seconds'))
     AND EXISTS (SELECT 1 FROM rekindle_instances
                  WHERE instance_id = $me AND heartbeat_at >= now() - interval '20 seconds')
  ```

  The stale checks are part of the same statement, and all times are Postgres `now()`.
- A run with no owner row at all (eg its first step hasn't reached the guard yet) is only claimed once Mastra itself hasn't updated it for 20s.
- The guard at the start of each step takes or keeps ownership of the run. It aborts the step if a different, live instance owns the run, eg when this process stalled and another instance took the run over.
- `clear()` in the last step marks the run done, so no sweep restarts it while Mastra is still saving its final status.
- If a restart fails, the claim is released and the next sweep tries again, up to 3 restarts per run. After that rekindle logs a warning (once per process) and leaves the run alone.
- A sweep does at most 25 restarts. Anything left over is picked up by later sweeps.
- On `start()`, an instance lets go of any runs a previous process with the same `instanceId` left behind.
- On SIGTERM an instance stops claiming new runs. If your app has its own SIGTERM handler, rekindle keeps heartbeating while your shutdown runs, so its in-flight runs aren't taken over. If not, it stops and lets the signal exit the process.

## Using it

Workflows have to opt in.

1. Register the workflow: `const ownership = rekindleWorkflow('myWorkflow')`, with the same key you use in `new Mastra({ workflows })`.
2. Call `await ownership.guard(runId)` as the first line of every step, using the `runId` Mastra passes to `execute`. Your input schemas don't change. A step without a guard (an agent passed straight to `createStep`, `.map`, a nested workflow) leaves the run unprotected while it runs, so wrap that work in a step that guards.
3. Make every step safe to re-run. `restart()` reruns the interrupted step from its start and skips completed steps, so a step has to re-read its inputs and write without duplicating.
4. Call `await ownership.clear(runId)` in the last step.

```ts
// workflows/my-workflow.ts
import { createStep, createWorkflow } from '@mastra/core/workflows'
import { rekindleWorkflow } from 'mastra-rekindle'

const ownership = rekindleWorkflow('myWorkflow')

const fetchStep = createStep({
  id: 'fetch',
  inputSchema, outputSchema,
  execute: async ({ inputData, runId }) => {
    await ownership.guard(runId)  // runId is what Mastra passes every step
    // ... re-run-safe work ...
    return inputData
  },
})
// ... more steps, each starting with ownership.guard(runId); the last one calls ownership.clear(runId)
```

```ts
// server boot
import { Mastra } from '@mastra/core/mastra'
import { PostgresStore } from '@mastra/pg'
import { createRekindle } from 'mastra-rekindle'

export const mastra = new Mastra({
  workflows: { myWorkflow },
  storage: new PostgresStore({ id: 'main', connectionString: process.env.DATABASE_URL }),
  recovery: { workflows: 'off' },  // see below
})

const rekindle = createRekindle({ mastra })  // uses Mastra's Postgres pool, schema and logger
await rekindle.migrate()          // or run migrationSql('<schema>') in your own migrations
await rekindle.start()            // before serving requests: guards fail until the first heartbeat
rekindle.installSignalHandlers()  // SIGTERM/SIGINT -> drain, then exit unless you handle the signal yourself
```

`recovery: { workflows: 'off' }`: newer versions in the supported range have a `recovery` option that restarts every active run on boot, and it defaults to on (eg 1.78). Turn it off, otherwise every booting instance restarts every active run alongside rekindle. Older versions (eg 1.46) don't have the option and don't restart on boot in production, so leave the line out there; it's a type error on those versions. `mastra dev` on those versions does restart active runs when it reloads, which is fine locally.

`installSignalHandlers()` drains on SIGTERM/SIGINT. If nothing else listens for the signal, it then stops rekindle and re-raises the signal, so the process exits as usual. If your app has its own handler, rekindle only drains and keeps heartbeating, and the exit is up to your shutdown code. Pass `{ exit: false }` to only drain either way.

`instanceId` has to be unique among live processes. The default (random per boot) is fine. A readable id makes `rekindle_instances` easier to match to your servers: `os.hostname()`, a pod name, or on ECS the task ARN:

```ts
async function ecsTaskArn(): Promise<string | undefined> {
  const base = process.env.ECS_CONTAINER_METADATA_URI_V4
  if (!base) return undefined
  const res = await fetch(`${base}/task`, { signal: AbortSignal.timeout(2000) }).catch(() => null)
  return res?.ok ? ((await res.json()) as { TaskARN?: string }).TaskARN : undefined
}

const rekindle = createRekindle({ mastra, instanceId: await ecsTaskArn() })
```

A stable id like a pod name also works across restarts, since `start()` lets go of what the previous process with that id held.

## Options

| Option | Default | |
|---|---|---|
| `mastra` | | Your Mastra instance. Its storage has to be a `PostgresStore`. |
| `pool` | Mastra's | Override the `pg` Pool. |
| `schema` | Mastra's | Override the schema for the two tables. |
| `instanceId` | random per boot | This process's id, unique among live processes. |
| `heartbeatMs` | 5s | Heartbeat interval. |
| `staleMs` | 20s | Heartbeat older than this and the instance counts as dead. Has to be more than `heartbeatMs`. |
| `guardRetryMs` / `guardWindowMs` | 3s / staleMs + 5s | How long the guard waits for an owner that just died to go stale. |
| `sweepIntervalMs` / `sweepJitterMs` | 10 min / 60s | Sweep interval. |
| `bootDelayMs` / `bootJitterMs` | 30s / 15s | Wait before the first sweep. Keep it above the stale window. |
| `maxRestartsPerSweep` | 25 | Restart cap per sweep, across all workflows. |
| `maxAttempts` | 3 | Restarts per run before rekindle leaves it alone. |
| `sweepPageSize` | 100 | Runs fetched per page while sweeping. |
| `pruneAfterMs` | 1h | Dead instance rows and finished owner rows older than this get deleted. |
| `workflows` | everything registered | Which workflows to sweep. |
| `logger` | Mastra's logger | Anything with `info` / `warn` / `error`. |
| `setAsDefault` | true | Make this the instance `rekindleWorkflow()` guards go through. |

## Demo

The demo runs two instances against one Postgres. Instance A starts a three-step run and gets `kill -9`'d in the middle of the slow step. Instance B claims the run once A's heartbeat is stale and resumes it at the slow step, without re-running the first step.

```bash
npm install
npm run db:start   # throwaway Postgres on :54329 from a local install, or point DATABASE_URL anywhere
DATABASE_URL=postgres://postgres@localhost:54329/postgres npx tsx examples/basic/demo.ts
```

```
  5.9s  --- kill -9 A (mid step "slow")
  5.9s  --- starting instance B
 11.5s  [rekindle] resuming abandoned run { workflow: 'demoRun', instanceId: 'B-603149' }
 11.6s  [B] step slow start
 21.6s  [B] step slow end
 22.6s  [B] step publish end

step log
  gather   start  on A
  gather   end    on A
  slow     start  on A
  slow     start  on B
  slow     end    on B
  publish  start  on B
  publish  end    on B

PASS: resumed on B at the interrupted step, gather not re-run
```

## Tests

The tests need a real Postgres. One runs a real Mastra workflow, stops its instance mid-step and checks another instance resumes it from that step. The rest cover five instances claiming the same run (one gets it), takeover from a dead owner, leaving a live owner alone, the guard aborting in a process that was taken over, the guard waiting for the stale window, done runs, the attempt cap and its warning, option validation, `waiting` runs, paging, a reused `instanceId`, release after a failed restart, the per-sweep cap, opt-in, drain and signal handling.

```bash
npm run db:start
TEST_DATABASE_URL=postgres://postgres@localhost:54329/postgres npm test
```

## Limits

- Resume is per step, not per item. A step interrupted halfway reruns from its start.
- The guard only checks at the start of a step. If a live instance can't heartbeat for 20s (pool exhausted, a long database failover), another instance takes its runs over and the step it was running runs twice. rekindle shares Mastra's pool by default; under heavy load give it a small pool of its own with `pool`.
- A hung step on a live instance isn't detected. That would need per-step deadlines.
- Suspend and resume across instances doesn't work: if a run suspends on A while A stays up, a resume that lands on B fails its guard.
- When you first roll this out, runs that started before the deploy and haven't been updated for 20s can be restarted.
- Postgres only.

## License

MIT
